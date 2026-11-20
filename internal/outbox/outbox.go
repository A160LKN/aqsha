// Package outbox — надёжная отправка событий в Kafka по шаблону transactional outbox.
//
// Событие записывается в таблицу outbox в той же транзакции, что и изменение данных.
// Отдельный цикл (Relay) читает неотправленные строки, публикует их в Kafka и помечает
// отправленными. Если Kafka недоступна или сервис упал между коммитом и отправкой,
// событие не теряется — оно уйдёт при следующем проходе. Цена — возможный дубль,
// поэтому получатели обязаны быть идемпотентными.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/A160LKN/aqsha/internal/platform"
)

var unsent = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "aqsha_outbox_unsent",
	Help: "Сколько событий ждёт отправки в Kafka. Растёт — значит, Kafka недоступна или отправка не успевает.",
}, []string{"service"})

// Add записывает событие в outbox внутри уже открытой транзакции.
// Идентификатор запроса и клиент из контекста уйдут в заголовки записи Kafka.
func Add(ctx context.Context, tx pgx.Tx, topic, key string, payload []byte) error {
	_, err := tx.Exec(ctx, `INSERT INTO outbox (topic, key, payload, trace_id, client_id) VALUES ($1, $2, $3, $4, $5)`,
		topic, key, payload, platform.Trace(ctx), platform.ClientID(ctx))
	return err
}

// Relay публикует события из outbox в Kafka.
type Relay struct {
	Service  string
	Pool     *pgxpool.Pool
	Kafka    *kgo.Client
	Log      *slog.Logger
	Interval time.Duration
	Batch    int
}

// Run отправляет события, пока не отменён контекст.
func (r *Relay) Run(ctx context.Context) {
	interval := r.Interval
	if interval == 0 {
		interval = 200 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		n, err := r.Flush(ctx)
		if err != nil && ctx.Err() == nil {
			r.Log.Warn("outbox: отправка не удалась, повторю", "err", err)
		}
		if n > 0 {
			continue // возможно, есть ещё — не ждём тик
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Flush отправляет одну пачку событий и возвращает, сколько отправлено.
// Строки блокируются FOR UPDATE SKIP LOCKED: несколько реплик сервиса не отправят одно событие дважды.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	batch := r.Batch
	if batch == 0 {
		batch = 100
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `SELECT id, topic, key, payload, trace_id, client_id FROM outbox
		WHERE sent_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return 0, err
	}
	var ids []int64
	var recs []*kgo.Record
	for rows.Next() {
		var id int64
		rec := &kgo.Record{}
		var key, trace, client string
		if err := rows.Scan(&id, &rec.Topic, &key, &rec.Value, &trace, &client); err != nil {
			rows.Close()
			return 0, err
		}
		rec.Key = []byte(key)
		if trace != "" {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: platform.TraceHeader, Value: []byte(trace)})
		}
		if client != "" {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: platform.ClientHeader, Value: []byte(client)})
		}
		ids = append(ids, id)
		recs = append(recs, rec)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(recs) > 0 {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := r.Kafka.ProduceSync(pctx, recs...).FirstErr()
		cancel()
		if err != nil {
			r.updateGauge(ctx)
			return 0, err
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	r.updateGauge(ctx)
	return len(recs), nil
}

func (r *Relay) updateGauge(ctx context.Context) {
	var n float64
	if err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n); err == nil {
		unsent.WithLabelValues(r.Service).Set(n)
	}
}
