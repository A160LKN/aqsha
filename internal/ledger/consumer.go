package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/A160LKN/aqsha/gen/aqsha/events/v1"
	"github.com/A160LKN/aqsha/internal/money"
	"github.com/A160LKN/aqsha/internal/outbox"
	"github.com/A160LKN/aqsha/internal/platform"
)

// Причины отказа в переводе.
const (
	ReasonInsufficientFunds = "insufficient_funds"
	ReasonAccountNotFound   = "account_not_found"
)

// ErrPoison — событие, которое нельзя обработать ни сейчас, ни после повтора: битое или с неверными данными.
var ErrPoison = errors.New("событие нельзя обработать")

// IsPermanent — ошибка, которую повтор не исправит. Такое событие уходит в DLQ: иначе обработчик
// крутил бы его вечно, и вся партиция (все переводы с этих счетов) встала бы за ним.
//
// Кроме ErrPoison это ошибки PostgreSQL классов 22 (неверные данные) и 23 (нарушено ограничение
// целостности): то же событие нарушит их и в сотый раз. Обрыв соединения, таймаут, взаимная
// блокировка (40P01) и конфликт сериализации (40001) — временные: событие повторяется, пока база
// не вернётся, и не теряется.
func IsPermanent(err error) bool {
	if errors.Is(err, ErrPoison) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23")
	}
	return false
}

// Consumer проводит переводы из топика aqsha.transfers.requested.
//
// Доставка в Kafka — «хотя бы один раз»: одно событие может прийти дважды
// (повтор продюсера, падение до коммита смещения, ребалансировка). Поэтому
// обработка идемпотентна: event_id записывается в processed_events в той же
// транзакции, что и движение денег, и второй раз событие ничего не меняет.
type Consumer struct {
	Pool  *pgxpool.Pool
	Kafka *kgo.Client
	Log   *slog.Logger
}

// Handle обрабатывает одно событие. Ошибка ErrPoison — отправить в DLQ, любая другая — повторить позже.
func (c *Consumer) Handle(ctx context.Context, rec *kgo.Record) error {
	ctx = platform.WithTrace(ctx, platform.HeaderValue(rec, platform.TraceHeader))
	var ev eventsv1.TransferRequested
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		return fmt.Errorf("%w: не разбирается: %v", ErrPoison, err)
	}
	if ev.GetEventId() == "" || ev.GetTransferId() == "" || ev.GetFromAccount() == "" || ev.GetToAccount() == "" ||
		ev.GetFromAccount() == ev.GetToAccount() || money.Validate(ev.GetAmountTiyn()) != nil {
		return fmt.Errorf("%w: неверные поля в событии %q", ErrPoison, ev.GetEventId())
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, ev.GetEventId())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		processed.WithLabelValues("duplicate").Inc()
		c.Log.InfoContext(ctx, "повтор события — пропускаю", "event_id", ev.GetEventId(),
			"transfer_id", ev.GetTransferId(), "trace_id", platform.Trace(ctx))
		return nil
	}

	outcome, reason, err := apply(ctx, tx, &ev, platform.HeaderValue(rec, platform.ClientHeader))
	if err != nil {
		return err
	}
	res, err := proto.Marshal(&eventsv1.TransferResult{
		EventId: platform.NewUUID(), TransferId: ev.GetTransferId(), Outcome: outcome, FailureReason: reason,
	})
	if err != nil {
		return err
	}
	if err := outbox.Add(ctx, tx, platform.TopicTransferResults, ev.GetTransferId(), res); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	result := "completed"
	if outcome == eventsv1.TransferResult_OUTCOME_FAILED {
		result = "failed_" + reason
	}
	processed.WithLabelValues(result).Inc()
	c.Log.InfoContext(ctx, "перевод обработан", "transfer_id", ev.GetTransferId(), "result", result,
		"trace_id", platform.Trace(ctx))
	return nil
}

// apply двигает деньги. Оба счёта блокируются в порядке id — два встречных перевода
// (A→B и B→A) не возьмут блокировки крест-накрест и не уйдут в deadlock.
//
// client — клиент API, создавший перевод (заголовок Kafka). Списать можно только со своего счёта:
// чужой счёт для клиента не существует, поэтому и причина отказа та же — account_not_found.
// Зачислить можно на любой существующий счёт.
func apply(ctx context.Context, tx pgx.Tx, ev *eventsv1.TransferRequested, client string) (eventsv1.TransferResult_Outcome, string, error) {
	rows, err := tx.Query(ctx, `SELECT id, balance, client_id FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE`,
		[]string{ev.GetFromAccount(), ev.GetToAccount()})
	if err != nil {
		return 0, "", err
	}
	type account struct {
		balance int64
		client  string
	}
	accounts := map[string]account{}
	for rows.Next() {
		var id string
		var a account
		if err := rows.Scan(&id, &a.balance, &a.client); err != nil {
			rows.Close()
			return 0, "", err
		}
		accounts[id] = a
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, "", err
	}
	src, okFrom := accounts[ev.GetFromAccount()]
	_, okTo := accounts[ev.GetToAccount()]
	from := src.balance
	switch {
	case !okFrom || !okTo || client == "" || src.client != client:
		return eventsv1.TransferResult_OUTCOME_FAILED, ReasonAccountNotFound, nil
	case from < ev.GetAmountTiyn():
		return eventsv1.TransferResult_OUTCOME_FAILED, ReasonInsufficientFunds, nil
	}
	for _, m := range []struct {
		acc   string
		delta int64
	}{{ev.GetFromAccount(), -ev.GetAmountTiyn()}, {ev.GetToAccount(), ev.GetAmountTiyn()}} {
		if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + $2 WHERE id = $1`, m.acc, m.delta); err != nil {
			return 0, "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO entries (account_id, delta, transfer_id) VALUES ($1, $2, $3)`,
			m.acc, m.delta, ev.GetTransferId()); err != nil {
			return 0, "", err
		}
	}
	return eventsv1.TransferResult_OUTCOME_COMPLETED, "", nil
}

// Run читает топик и обрабатывает события, пока не отменён контекст.
// Смещение фиксируется только после успешной обработки: упавший сервис
// прочитает необработанные события заново.
func (c *Consumer) Run(ctx context.Context) {
	for {
		fetches := c.Kafka.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.Log.Warn("ошибка чтения Kafka", "topic", t, "partition", p, "err", err)
		})
		var done []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) {
			if ctx.Err() != nil {
				return
			}
			c.handleWithRetry(ctx, rec)
			done = append(done, rec)
		})
		if len(done) > 0 && ctx.Err() == nil {
			if err := c.Kafka.CommitRecords(ctx, done...); err != nil {
				c.Log.Warn("не удалось зафиксировать смещение — события придут повторно", "err", err)
			}
		}
		c.Kafka.AllowRebalance()
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, rec *kgo.Record) {
	backoff := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := c.Handle(ctx, rec)
		if err == nil {
			return
		}
		if IsPermanent(err) {
			c.toDLQ(ctx, rec, err)
			return
		}
		// Долгий сбой — уже не «моргнула сеть»: после 10 попыток пишем ошибкой, чтобы её заметили
		level := slog.LevelWarn
		if attempt >= 10 {
			level = slog.LevelError
		}
		c.Log.Log(ctx, level, "ошибка обработки, повторю", "err", err, "attempt", attempt, "offset", rec.Offset,
			"trace_id", platform.HeaderValue(rec, platform.TraceHeader))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (c *Consumer) toDLQ(ctx context.Context, rec *kgo.Record, cause error) {
	processed.WithLabelValues("dlq").Inc()
	dlq := &kgo.Record{Topic: platform.TopicTransferDLQ, Key: rec.Key, Value: rec.Value,
		Headers: append(rec.Headers, kgo.RecordHeader{Key: "error", Value: []byte(cause.Error())})}
	for {
		if err := c.Kafka.ProduceSync(ctx, dlq).FirstErr(); err == nil {
			break
		}
		// Остановка сервиса не ждёт Kafka: смещение не зафиксировано, событие придёт снова после перезапуска
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	c.Log.Error("событие отправлено в DLQ", "err", cause, "partition", rec.Partition, "offset", rec.Offset,
		"trace_id", platform.HeaderValue(rec, platform.TraceHeader))
}
