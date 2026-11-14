package payments

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/A160LKN/aqsha/gen/aqsha/events/v1"
	"github.com/A160LKN/aqsha/internal/platform"
	"github.com/A160LKN/aqsha/internal/transfer"
)

// Results читает итоги переводов из aqsha.transfers.results и обновляет статус.
// Обновление идёт только из pending: повтор события или опоздавший дубль
// не перезапишут итоговый статус.
type Results struct {
	Pool  *pgxpool.Pool
	Kafka *kgo.Client
	Log   *slog.Logger
}

// Handle применяет один итог.
func (r *Results) Handle(ctx context.Context, rec *kgo.Record) error {
	ctx = platform.WithTrace(ctx, platform.HeaderValue(rec, platform.TraceHeader))
	var ev eventsv1.TransferResult
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		r.Log.ErrorContext(ctx, "битый итог перевода — пропускаю", "err", err, "offset", rec.Offset)
		return nil
	}
	next := transfer.Failed
	if ev.GetOutcome() == eventsv1.TransferResult_OUTCOME_COMPLETED {
		next = transfer.Completed
	} else if ev.GetOutcome() != eventsv1.TransferResult_OUTCOME_FAILED {
		r.Log.ErrorContext(ctx, "неизвестный итог перевода — пропускаю", "transfer_id", ev.GetTransferId())
		return nil
	}
	if !transfer.CanTransition(transfer.Pending, next) {
		return fmt.Errorf("недопустимый переход в %s", next)
	}
	tag, err := r.Pool.Exec(ctx, `UPDATE transfers SET status = $2, failure_reason = $3, updated_at = now()
		WHERE id = $1 AND status = $4`, ev.GetTransferId(), string(next), ev.GetFailureReason(), string(transfer.Pending))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		r.Log.InfoContext(ctx, "итог уже применён — пропускаю", "transfer_id", ev.GetTransferId(), "trace_id", platform.Trace(ctx))
		return nil
	}
	r.Log.InfoContext(ctx, "перевод завершён", "transfer_id", ev.GetTransferId(), "status", string(next),
		"reason", ev.GetFailureReason(), "trace_id", platform.Trace(ctx))
	return nil
}

// Run читает итоги, пока не отменён контекст. Смещение фиксируется после обработки.
func (r *Results) Run(ctx context.Context) {
	for {
		fetches := r.Kafka.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		var done []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) {
			for ctx.Err() == nil {
				err := r.Handle(ctx, rec)
				if err == nil {
					done = append(done, rec)
					return
				}
				r.Log.Warn("ошибка применения итога, повторю", "err", err)
				time.Sleep(500 * time.Millisecond)
			}
		})
		if len(done) > 0 && ctx.Err() == nil {
			if err := r.Kafka.CommitRecords(ctx, done...); err != nil {
				r.Log.Warn("не удалось зафиксировать смещение", "err", err)
			}
		}
		r.Kafka.AllowRebalance()
	}
}
