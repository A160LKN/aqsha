// Package ledger — сервис счетов: балансы, пополнения и проведение переводов из Kafka.
package ledger

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/A160LKN/aqsha/gen/aqsha/ledger/v1"
	"github.com/A160LKN/aqsha/internal/money"
	"github.com/A160LKN/aqsha/internal/platform"
)

// Migrations — схема базы сервиса.
//
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

var processed = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "aqsha_ledger_events_total",
	Help: "События переводов, обработанные сервисом счетов, по результату.",
}, []string{"result"})

// Service реализует gRPC-API сервиса счетов.
type Service struct {
	ledgerv1.UnimplementedLedgerServiceServer
	Pool *pgxpool.Pool
	Log  *slog.Logger
}

// OpenAccount открывает счёт с нулевым балансом. Владелец счёта — клиент из x-api-key.
func (s *Service) OpenAccount(ctx context.Context, req *ledgerv1.OpenAccountRequest) (*ledgerv1.OpenAccountResponse, error) {
	owner := strings.TrimSpace(req.GetOwner())
	if owner == "" || len([]rune(owner)) > 100 {
		return nil, status.Error(codes.InvalidArgument, "owner: от 1 до 100 символов")
	}
	acc := &ledgerv1.Account{Id: platform.NewID("acc"), Owner: owner, Currency: "KZT"}
	if _, err := s.Pool.Exec(ctx, `INSERT INTO accounts (id, owner, client_id) VALUES ($1, $2, $3)`,
		acc.Id, acc.Owner, platform.ClientID(ctx)); err != nil {
		return nil, s.internal(ctx, err)
	}
	return &ledgerv1.OpenAccountResponse{Account: acc}, nil
}

// GetAccount возвращает счёт с текущим балансом. Чужой счёт выглядит как несуществующий.
func (s *Service) GetAccount(ctx context.Context, req *ledgerv1.GetAccountRequest) (*ledgerv1.GetAccountResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id обязателен")
	}
	acc, err := getAccount(ctx, s.Pool, req.GetId(), platform.ClientID(ctx))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "счёт не найден")
	}
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	return &ledgerv1.GetAccountResponse{Account: acc}, nil
}

// Deposit зачисляет деньги на счёт. Повтор с тем же deposit_id возвращает счёт без второго зачисления.
func (s *Service) Deposit(ctx context.Context, req *ledgerv1.DepositRequest) (*ledgerv1.DepositResponse, error) {
	if err := money.Validate(req.GetAmountTiyn()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetAccountId() == "" || req.GetDepositId() == "" || len(req.GetDepositId()) > 64 {
		return nil, status.Error(codes.InvalidArgument, "account_id и deposit_id (до 64 символов) обязательны")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1 AND client_id = $2)`,
		req.GetAccountId(), platform.ClientID(ctx)).Scan(&exists); err != nil {
		return nil, s.internal(ctx, err)
	}
	if !exists {
		return nil, status.Error(codes.NotFound, "счёт не найден")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO deposits (deposit_id, account_id, amount) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		req.GetDepositId(), req.GetAccountId(), req.GetAmountTiyn())
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	if tag.RowsAffected() == 0 {
		var acc string
		var amount int64
		if err := tx.QueryRow(ctx, `SELECT account_id, amount FROM deposits WHERE deposit_id = $1`, req.GetDepositId()).Scan(&acc, &amount); err != nil {
			return nil, s.internal(ctx, err)
		}
		if acc != req.GetAccountId() || amount != req.GetAmountTiyn() {
			return nil, status.Error(codes.AlreadyExists, "deposit_id уже использован для другого пополнения")
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + $2 WHERE id = $1`, req.GetAccountId(), req.GetAmountTiyn()); err != nil {
			return nil, s.internal(ctx, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO entries (account_id, delta, deposit_id) VALUES ($1, $2, $3)`,
			req.GetAccountId(), req.GetAmountTiyn(), req.GetDepositId()); err != nil {
			return nil, s.internal(ctx, err)
		}
	}
	acc, err := getAccount(ctx, tx, req.GetAccountId(), platform.ClientID(ctx))
	if err != nil {
		return nil, s.internal(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, s.internal(ctx, err)
	}
	return &ledgerv1.DepositResponse{Account: acc}, nil
}

func (s *Service) internal(ctx context.Context, err error) error {
	s.Log.ErrorContext(ctx, "ошибка базы", "err", err, "trace_id", platform.Trace(ctx))
	return status.Error(codes.Internal, "внутренняя ошибка")
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func getAccount(ctx context.Context, q querier, id, client string) (*ledgerv1.Account, error) {
	acc := &ledgerv1.Account{}
	err := q.QueryRow(ctx, `SELECT id, owner, balance, currency FROM accounts WHERE id = $1 AND client_id = $2`, id, client).
		Scan(&acc.Id, &acc.Owner, &acc.BalanceTiyn, &acc.Currency)
	return acc, err
}
