//go:build e2e

// Package e2e проверяет развёрнутую систему снаружи — как клиент: по gRPC, с настоящими Kafka и PostgreSQL.
// Запуск: go test -tags e2e ./e2e (адреса — в переменных окружения, см. README).
package e2e

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/A160LKN/aqsha/gen/aqsha/ledger/v1"
	paymentsv1 "github.com/A160LKN/aqsha/gen/aqsha/payments/v1"
	"github.com/A160LKN/aqsha/internal/platform"
)

var (
	ledgerAddr   = platform.Env("LEDGER_ADDR", "localhost:9091")
	paymentsAddr = platform.Env("PAYMENTS_ADDR", "localhost:9090")
	apiKey       = platform.Env("API_KEY", "demo-key-change-me")
)

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type client struct {
	t        *testing.T
	ledger   ledgerv1.LedgerServiceClient
	payments paymentsv1.PaymentServiceClient
}

func newClient(t *testing.T) *client {
	return &client{t: t,
		ledger:   ledgerv1.NewLedgerServiceClient(dial(t, ledgerAddr)),
		payments: paymentsv1.NewPaymentServiceClient(dial(t, paymentsAddr))}
}

func (c *client) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	c.t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, "x-api-key", apiKey)
}

func (c *client) account(balance int64) string {
	c.t.Helper()
	acc, err := c.ledger.OpenAccount(c.ctx(), &ledgerv1.OpenAccountRequest{Owner: "e2e"})
	require.NoError(c.t, err)
	if balance > 0 {
		_, err = c.ledger.Deposit(c.ctx(), &ledgerv1.DepositRequest{AccountId: acc.GetAccount().GetId(),
			AmountTiyn: balance, DepositId: platform.NewUUID()})
		require.NoError(c.t, err)
	}
	return acc.GetAccount().GetId()
}

func (c *client) balance(id string) int64 {
	c.t.Helper()
	r, err := c.ledger.GetAccount(c.ctx(), &ledgerv1.GetAccountRequest{Id: id})
	require.NoError(c.t, err)
	return r.GetAccount().GetBalanceTiyn()
}

func (c *client) transfer(from, to string, amount int64) string {
	c.t.Helper()
	r, err := c.payments.CreateTransfer(c.ctx(), &paymentsv1.CreateTransferRequest{
		IdempotencyKey: platform.NewUUID(), FromAccount: from, ToAccount: to, AmountTiyn: amount})
	require.NoError(c.t, err)
	return r.GetTransfer().GetId()
}

func (c *client) wait(id string, timeout time.Duration) *paymentsv1.Transfer {
	c.t.Helper()
	var tr *paymentsv1.Transfer
	require.Eventually(c.t, func() bool {
		r, err := c.payments.GetTransfer(c.ctx(), &paymentsv1.GetTransferRequest{Id: id})
		if err != nil {
			return false
		}
		tr = r.GetTransfer()
		return tr.GetStatus() != paymentsv1.TransferStatus_TRANSFER_STATUS_PENDING
	}, timeout, 200*time.Millisecond, "перевод %s завершился", id)
	return tr
}

func TestHealth(t *testing.T) {
	for _, addr := range []string{ledgerAddr, paymentsAddr} {
		r, err := healthpb.NewHealthClient(dial(t, addr)).Check(context.Background(), &healthpb.HealthCheckRequest{})
		require.NoError(t, err, addr)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, r.GetStatus(), addr)
	}
}

func TestTransferEndToEnd(t *testing.T) {
	c := newClient(t)
	a, b := c.account(50_000_00), c.account(0)

	ok := c.wait(c.transfer(a, b, 20_000_00), 30*time.Second)
	assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_COMPLETED, ok.GetStatus())
	failed := c.wait(c.transfer(a, b, 40_000_00), 30*time.Second)
	assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_FAILED, failed.GetStatus())
	assert.Equal(t, "insufficient_funds", failed.GetFailureReason())

	assert.EqualValues(t, 30_000_00, c.balance(a))
	assert.EqualValues(t, 20_000_00, c.balance(b))
}

func TestIdempotencyAcrossTheWire(t *testing.T) {
	c := newClient(t)
	a, b := c.account(10_00), c.account(0)
	req := &paymentsv1.CreateTransferRequest{IdempotencyKey: platform.NewUUID(), FromAccount: a, ToAccount: b, AmountTiyn: 10_00}
	first, err := c.payments.CreateTransfer(c.ctx(), req)
	require.NoError(t, err)
	var header metadata.MD
	again, err := c.payments.CreateTransfer(c.ctx(), req, grpc.Header(&header))
	require.NoError(t, err)
	assert.Equal(t, first.GetTransfer().GetId(), again.GetTransfer().GetId())
	assert.Equal(t, []string{"true"}, header.Get("idempotent-replayed"))
	c.wait(first.GetTransfer().GetId(), 30*time.Second)
	assert.EqualValues(t, 10_00, c.balance(b), "деньги пришли один раз")
}

func TestAuthAndErrors(t *testing.T) {
	c := newClient(t)
	_, err := c.payments.CreateTransfer(context.Background(), &paymentsv1.CreateTransferRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = c.payments.CreateTransfer(c.ctx(), &paymentsv1.CreateTransferRequest{IdempotencyKey: "short"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = c.payments.GetTransfer(c.ctx(), &paymentsv1.GetTransferRequest{Id: platform.NewUUID()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// 40 переводов с одного счёта параллельно, денег — на 25. На настоящей Kafka и PostgreSQL.
func TestParallelTransfersFromOneAccount(t *testing.T) {
	c := newClient(t)
	a, b := c.account(25_00), c.account(0)
	var wg sync.WaitGroup
	ids := make(chan string, 40)
	for range 40 {
		wg.Add(1)
		go func() { defer wg.Done(); ids <- c.transfer(a, b, 1_00) }()
	}
	wg.Wait()
	close(ids)
	completed := 0
	for id := range ids {
		if c.wait(id, 60*time.Second).GetStatus() == paymentsv1.TransferStatus_TRANSFER_STATUS_COMPLETED {
			completed++
		}
	}
	assert.Equal(t, 25, completed)
	assert.Zero(t, c.balance(a))
	assert.EqualValues(t, 25_00, c.balance(b))
}

// Хаос-тест: сервис счетов останавливается посреди работы и поднимается снова.
// Команды остановки и запуска задаёт окружение: docker compose или kubectl.
func TestLedgerOutage(t *testing.T) {
	stop, start := os.Getenv("STOP_LEDGER_CMD"), os.Getenv("START_LEDGER_CMD")
	if stop == "" || start == "" {
		t.Skip("STOP_LEDGER_CMD и START_LEDGER_CMD не заданы")
	}
	c := newClient(t)
	a, b := c.account(10_000_00), c.account(0)
	run(t, stop)
	t.Cleanup(func() { run(t, start) })

	var ids []string
	for range 10 {
		ids = append(ids, c.transfer(a, b, 1_000_00))
	}
	time.Sleep(2 * time.Second)
	for _, id := range ids {
		r, err := c.payments.GetTransfer(c.ctx(), &paymentsv1.GetTransferRequest{Id: id})
		require.NoError(t, err)
		assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_PENDING, r.GetTransfer().GetStatus(),
			"сервис счетов лежит — переводы ждут, а не падают")
	}

	run(t, start)
	c.ledger = ledgerv1.NewLedgerServiceClient(dial(t, ledgerAddr))
	for _, id := range ids {
		assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_COMPLETED, c.wait(id, 120*time.Second).GetStatus())
	}
	require.Eventually(t, func() bool {
		r, err := c.ledger.GetAccount(c.ctx(), &ledgerv1.GetAccountRequest{Id: b})
		return err == nil && r.GetAccount().GetBalanceTiyn() == 10_000_00
	}, 60*time.Second, time.Second, "все переводы проведены ровно один раз")
}

func run(t *testing.T, cmd string) {
	t.Helper()
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	require.NoError(t, err, "%s: %s", cmd, out)
}
