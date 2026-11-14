// Package payments — сервис платежей: принимает переводы по gRPC и следит за их итогом.
package payments

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/A160LKN/aqsha/gen/aqsha/events/v1"
	paymentsv1 "github.com/A160LKN/aqsha/gen/aqsha/payments/v1"
	"github.com/A160LKN/aqsha/internal/money"
	"github.com/A160LKN/aqsha/internal/outbox"
	"github.com/A160LKN/aqsha/internal/platform"
	"github.com/A160LKN/aqsha/internal/transfer"
)

//go:embed migrations/*.sql
var migrationsDir embed.FS

// Migrations возвращает файлы миграций.
func Migrations() fs.FS {
	sub, err := fs.Sub(migrationsDir, "migrations")
	if err != nil {
		panic(err)
	}
	return sub
}

// ReplayedHeader — заголовок ответа: перевод уже был создан раньше с этим ключом.
const ReplayedHeader = "idempotent-replayed"

var created = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "aqsha_transfers_created_total",
	Help: "Запросы на перевод: новые и повторы по ключу идемпотентности.",
}, []string{"kind"})

// Service реализует gRPC-API сервиса платежей.
type Service struct {
	paymentsv1.UnimplementedPaymentServiceServer
	Pool *pgxpool.Pool
	Log  *slog.Logger
}

// CreateTransfer регистрирует перевод и в той же транзакции кладёт событие в outbox.
func (s *Service) CreateTransfer(ctx context.Context, req *paymentsv1.CreateTransferRequest) (*paymentsv1.CreateTransferResponse, error) {
	client := ClientID(ctx)
	if err := validateCreate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	hash := requestHash(req)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	t := &paymentsv1.Transfer{Id: platform.NewUUID(), FromAccount: req.GetFromAccount(), ToAccount: req.GetToAccount(),
		AmountTiyn: req.GetAmountTiyn(), Status: paymentsv1.TransferStatus_TRANSFER_STATUS_PENDING}
	// ON CONFLICT: второй одновременный запрос с тем же ключом дождётся коммита первого и ничего не вставит
	tag, err := tx.Exec(ctx, `INSERT INTO transfers (id, client_id, idempotency_key, request_hash, from_account, to_account, amount)
		VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (client_id, idempotency_key) DO NOTHING`,
		t.Id, client, req.GetIdempotencyKey(), hash, t.FromAccount, t.ToAccount, t.AmountTiyn)
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	if tag.RowsAffected() == 0 {
		existing, existingHash, err := s.byKey(ctx, tx, client, req.GetIdempotencyKey())
		if err != nil {
			return nil, s.internal(ctx, err)
		}
		if existingHash != hash {
			return nil, status.Error(codes.AlreadyExists, "ключ идемпотентности уже использован для другого перевода")
		}
		created.WithLabelValues("replayed").Inc()
		_ = grpc.SetHeader(ctx, metadata.Pairs(ReplayedHeader, "true"))
		return &paymentsv1.CreateTransferResponse{Transfer: existing}, nil
	}
	ev, err := proto.Marshal(&eventsv1.TransferRequested{EventId: platform.NewUUID(), TransferId: t.Id,
		FromAccount: t.FromAccount, ToAccount: t.ToAccount, AmountTiyn: t.AmountTiyn})
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	// Ключ — счёт списания: все переводы с одного счёта идут в одну партицию по порядку
	if err := outbox.Add(ctx, tx, platform.TopicTransferRequested, t.FromAccount, ev); err != nil {
		return nil, s.internal(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, s.internal(ctx, err)
	}
	created.WithLabelValues("new").Inc()
	s.Log.InfoContext(ctx, "перевод принят", "transfer_id", t.Id, "client", client, "trace_id", platform.Trace(ctx))
	return &paymentsv1.CreateTransferResponse{Transfer: t}, nil
}

// GetTransfer возвращает перевод. Чужой перевод выглядит как несуществующий.
func (s *Service) GetTransfer(ctx context.Context, req *paymentsv1.GetTransferRequest) (*paymentsv1.GetTransferResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id обязателен")
	}
	t, err := scanTransfer(s.Pool.QueryRow(ctx, `SELECT id, from_account, to_account, amount, status, failure_reason
		FROM transfers WHERE id = $1 AND client_id = $2`, req.GetId(), ClientID(ctx)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "перевод не найден")
	}
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	return &paymentsv1.GetTransferResponse{Transfer: t}, nil
}

func (s *Service) byKey(ctx context.Context, tx pgx.Tx, client, key string) (*paymentsv1.Transfer, string, error) {
	var hash string
	row := tx.QueryRow(ctx, `SELECT id, from_account, to_account, amount, status, failure_reason, request_hash
		FROM transfers WHERE client_id = $1 AND idempotency_key = $2`, client, key)
	t := &paymentsv1.Transfer{}
	var st string
	err := row.Scan(&t.Id, &t.FromAccount, &t.ToAccount, &t.AmountTiyn, &st, &t.FailureReason, &hash)
	t.Status = toProto(transfer.Status(st))
	return t, hash, err
}

func (s *Service) internal(ctx context.Context, err error) error {
	s.Log.ErrorContext(ctx, "ошибка базы", "err", err, "trace_id", platform.Trace(ctx))
	return status.Error(codes.Internal, "внутренняя ошибка")
}

func validateCreate(req *paymentsv1.CreateTransferRequest) error {
	key := req.GetIdempotencyKey()
	switch {
	case len(key) < 8 || len(key) > 64:
		return fmt.Errorf("idempotency_key: от 8 до 64 символов")
	case strings.TrimSpace(req.GetFromAccount()) == "" || strings.TrimSpace(req.GetToAccount()) == "":
		return fmt.Errorf("from_account и to_account обязательны")
	case req.GetFromAccount() == req.GetToAccount():
		return fmt.Errorf("нельзя перевести деньги на тот же счёт")
	}
	return money.Validate(req.GetAmountTiyn())
}

func requestHash(req *paymentsv1.CreateTransferRequest) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d", req.GetFromAccount(), req.GetToAccount(), req.GetAmountTiyn())))
	return hex.EncodeToString(sum[:])
}

func scanTransfer(row pgx.Row) (*paymentsv1.Transfer, error) {
	t := &paymentsv1.Transfer{}
	var st string
	if err := row.Scan(&t.Id, &t.FromAccount, &t.ToAccount, &t.AmountTiyn, &st, &t.FailureReason); err != nil {
		return nil, err
	}
	t.Status = toProto(transfer.Status(st))
	return t, nil
}

func toProto(s transfer.Status) paymentsv1.TransferStatus {
	switch s {
	case transfer.Pending:
		return paymentsv1.TransferStatus_TRANSFER_STATUS_PENDING
	case transfer.Completed:
		return paymentsv1.TransferStatus_TRANSFER_STATUS_COMPLETED
	case transfer.Failed:
		return paymentsv1.TransferStatus_TRANSFER_STATUS_FAILED
	}
	return paymentsv1.TransferStatus_TRANSFER_STATUS_UNSPECIFIED
}
