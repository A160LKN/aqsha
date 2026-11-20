// Package system_test проверяет оба сервиса вместе: gRPC, outbox, Kafka и обработчики событий в одном процессе.
package system_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	ledgerv1 "github.com/A160LKN/aqsha/gen/aqsha/ledger/v1"
	paymentsv1 "github.com/A160LKN/aqsha/gen/aqsha/payments/v1"
	"github.com/A160LKN/aqsha/internal/ledger"
	"github.com/A160LKN/aqsha/internal/outbox"
	"github.com/A160LKN/aqsha/internal/payments"
	"github.com/A160LKN/aqsha/internal/platform"
	"github.com/A160LKN/aqsha/internal/testutil"
)

type system struct {
	t                   *testing.T
	brokers             []string
	ledger              ledgerv1.LedgerServiceClient
	payments            paymentsv1.PaymentServiceClient
	startLedgerConsumer func() (stop func())
}

func start(t *testing.T, withLedgerConsumer bool) *system {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := testutil.Log(t)
	brokers := testutil.Kafka(t, 3)

	lpool := testutil.DB(t, ledger.Migrations())
	ppool := testutil.DB(t, payments.Migrations())

	go (&outbox.Relay{Service: "ledger", Pool: lpool, Kafka: testutil.Client(t, brokers, ""), Log: log}).Run(ctx)
	go (&outbox.Relay{Service: "payments", Pool: ppool, Kafka: testutil.Client(t, brokers, ""), Log: log}).Run(ctx)
	go (&payments.Results{Pool: ppool, Kafka: testutil.Client(t, brokers, "payments", platform.TopicTransferResults), Log: log}).Run(ctx)

	s := &system{t: t, brokers: brokers}
	s.startLedgerConsumer = func() func() {
		cctx, ccancel := context.WithCancel(ctx)
		cl, err := platform.NewKafka(brokers, "ledger", platform.TopicTransferRequested)
		require.NoError(t, err)
		done := make(chan struct{})
		go func() {
			(&ledger.Consumer{Pool: lpool, Kafka: cl, Log: log}).Run(cctx)
			close(done)
		}()
		return func() { ccancel(); <-done; cl.Close() }
	}
	if withLedgerConsumer {
		stop := s.startLedgerConsumer()
		t.Cleanup(stop)
	}

	lconn := testutil.GRPC(t, func(g *grpc.Server) {
		ledgerv1.RegisterLedgerServiceServer(g, &ledger.Service{Pool: lpool, Log: log})
	}, platform.UnaryInterceptor(log), platform.AuthInterceptor(map[string]string{"system-key-1": "shop"}))
	pconn := testutil.GRPC(t, func(g *grpc.Server) {
		paymentsv1.RegisterPaymentServiceServer(g, &payments.Service{Pool: ppool, Log: log})
	}, platform.UnaryInterceptor(log), payments.AuthInterceptor(map[string]string{"system-key-1": "shop"}))
	s.ledger = ledgerv1.NewLedgerServiceClient(lconn)
	s.payments = paymentsv1.NewPaymentServiceClient(pconn)
	return s
}

func (s *system) ctx() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), payments.APIKeyHeader, "system-key-1")
}

func (s *system) account(balance int64) string {
	s.t.Helper()
	acc, err := s.ledger.OpenAccount(s.ctx(), &ledgerv1.OpenAccountRequest{Owner: "клиент"})
	require.NoError(s.t, err)
	if balance > 0 {
		_, err = s.ledger.Deposit(s.ctx(), &ledgerv1.DepositRequest{
			AccountId: acc.GetAccount().GetId(), AmountTiyn: balance, DepositId: platform.NewUUID()})
		require.NoError(s.t, err)
	}
	return acc.GetAccount().GetId()
}

func (s *system) balance(id string) int64 {
	s.t.Helper()
	r, err := s.ledger.GetAccount(s.ctx(), &ledgerv1.GetAccountRequest{Id: id})
	require.NoError(s.t, err)
	return r.GetAccount().GetBalanceTiyn()
}

func (s *system) transfer(key, from, to string, amount int64) string {
	s.t.Helper()
	r, err := s.payments.CreateTransfer(s.ctx(), &paymentsv1.CreateTransferRequest{
		IdempotencyKey: key, FromAccount: from, ToAccount: to, AmountTiyn: amount})
	require.NoError(s.t, err)
	return r.GetTransfer().GetId()
}

func (s *system) status(id string) *paymentsv1.Transfer {
	s.t.Helper()
	r, err := s.payments.GetTransfer(s.ctx(), &paymentsv1.GetTransferRequest{Id: id})
	require.NoError(s.t, err)
	return r.GetTransfer()
}

func (s *system) waitFinal(id string) *paymentsv1.Transfer {
	s.t.Helper()
	var tr *paymentsv1.Transfer
	testutil.Eventually(s.t, func() bool {
		tr = s.status(id)
		return tr.GetStatus() != paymentsv1.TransferStatus_TRANSFER_STATUS_PENDING
	}, "перевод "+id+" завершился")
	return tr
}

func TestTransferLifecycle(t *testing.T) {
	t.Parallel()
	s := start(t, true)
	aliya, bolat := s.account(100_000_00), s.account(0)

	ok := s.transfer("lifecycle-ok-1", aliya, bolat, 30_000_00)
	tooMuch := s.transfer("lifecycle-big-1", aliya, bolat, 80_000_00)
	ghost := s.transfer("lifecycle-ghost", aliya, "acc_000000000000", 1_00)

	assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_COMPLETED, s.waitFinal(ok).GetStatus())
	failed := s.waitFinal(tooMuch)
	assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_FAILED, failed.GetStatus())
	assert.Equal(t, ledger.ReasonInsufficientFunds, failed.GetFailureReason())
	assert.Equal(t, ledger.ReasonAccountNotFound, s.waitFinal(ghost).GetFailureReason())

	assert.EqualValues(t, 70_000_00, s.balance(aliya))
	assert.EqualValues(t, 30_000_00, s.balance(bolat))
}

// Сервис счетов лежит, пока клиенты переводят деньги. Переводы ждут в Kafka и
// проводятся, когда сервис поднимется, — ни один не потерян и не проведён дважды.
func TestLedgerDownThenRecovers(t *testing.T) {
	t.Parallel()
	s := start(t, false)
	a, b := s.account(10_000_00), s.account(0)

	var ids []string
	for i := range 10 {
		ids = append(ids, s.transfer("outage-key-"+string(rune('a'+i)), a, b, 1_000_00))
	}
	for _, id := range ids {
		assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_PENDING, s.status(id).GetStatus(), "пока сервис счетов лежит — ждём")
	}

	stop := s.startLedgerConsumer()
	defer stop()
	for _, id := range ids {
		assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_COMPLETED, s.waitFinal(id).GetStatus())
	}
	assert.Zero(t, s.balance(a))
	assert.EqualValues(t, 10_000_00, s.balance(b))
}

// Сервис счетов перезапускается посреди потока переводов.
func TestLedgerRestartMidStream(t *testing.T) {
	t.Parallel()
	s := start(t, false)
	a, b := s.account(50_00), s.account(0)

	stop := s.startLedgerConsumer()
	var ids []string
	for i := range 50 {
		ids = append(ids, s.transfer("restart-key-"+platform.NewID("k"), a, b, 1_00))
		if i == 20 {
			stop() // «упал» посреди потока
			stop = s.startLedgerConsumer()
		}
	}
	defer stop()
	for _, id := range ids {
		assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_COMPLETED, s.waitFinal(id).GetStatus())
	}
	assert.Zero(t, s.balance(a), "все 50 переводов проведены ровно один раз")
	assert.EqualValues(t, 50_00, s.balance(b))
}
