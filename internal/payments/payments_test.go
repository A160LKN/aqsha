package payments_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/A160LKN/aqsha/gen/aqsha/events/v1"
	paymentsv1 "github.com/A160LKN/aqsha/gen/aqsha/payments/v1"
	"github.com/A160LKN/aqsha/internal/payments"
	"github.com/A160LKN/aqsha/internal/platform"
	"github.com/A160LKN/aqsha/internal/testutil"
)

const (
	keyAliya = "key-aliya-0001"
	keyBolat = "key-bolat-0002"
)

type env struct {
	pool   *pgxpool.Pool
	client paymentsv1.PaymentServiceClient
}

func newEnv(t *testing.T) env {
	t.Helper()
	pool := testutil.DB(t, payments.Migrations())
	svc := &payments.Service{Pool: pool, Log: testutil.Log(t)}
	conn := testutil.GRPC(t, func(s *grpc.Server) { paymentsv1.RegisterPaymentServiceServer(s, svc) },
		platform.UnaryInterceptor(testutil.Log(t)),
		payments.AuthInterceptor(payments.ParseKeys(keyAliya+":aliya,"+keyBolat+":bolat")))
	return env{pool: pool, client: paymentsv1.NewPaymentServiceClient(conn)}
}

func as(key string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), payments.APIKeyHeader, key)
}

func req(key string) *paymentsv1.CreateTransferRequest {
	return &paymentsv1.CreateTransferRequest{IdempotencyKey: key, FromAccount: "acc_a", ToAccount: "acc_b", AmountTiyn: 5_000_00}
}

func (e env) outbox(t *testing.T) []*eventsv1.TransferRequested {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), `SELECT payload FROM outbox WHERE topic = $1 ORDER BY id`, platform.TopicTransferRequested)
	require.NoError(t, err)
	defer rows.Close()
	var out []*eventsv1.TransferRequested
	for rows.Next() {
		var b []byte
		require.NoError(t, rows.Scan(&b))
		ev := &eventsv1.TransferRequested{}
		require.NoError(t, proto.Unmarshal(b, ev))
		out = append(out, ev)
	}
	return out
}

func TestAuth(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	cases := map[string]context.Context{
		"без ключа":        context.Background(),
		"неизвестный ключ": as("key-unknown-9999"),
		"пустой ключ":      as(""),
		"ключ с пробелом":  as(keyAliya + " "),
	}
	for name, ctx := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := e.client.CreateTransfer(ctx, req("idem-key-auth"))
			assert.Equal(t, codes.Unauthenticated, status.Code(err))
		})
	}
	assert.Empty(t, e.outbox(t), "без авторизации ничего не записано")
}

func TestCreateTransfer(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := metadata.AppendToOutgoingContext(as(keyAliya), platform.TraceHeader, "trace-from-client")
	var header metadata.MD

	resp, err := e.client.CreateTransfer(ctx, req("idem-key-0001"), grpc.Header(&header))
	require.NoError(t, err)
	tr := resp.GetTransfer()
	assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_PENDING, tr.GetStatus(), "перевод асинхронный: сначала pending")
	assert.Equal(t, []string{"trace-from-client"}, header.Get(platform.TraceHeader), "идентификатор запроса возвращается клиенту")
	assert.Empty(t, header.Get(payments.ReplayedHeader))

	events := e.outbox(t)
	require.Len(t, events, 1, "событие записано в той же транзакции, что и перевод")
	assert.Equal(t, tr.GetId(), events[0].GetTransferId())
	assert.EqualValues(t, 5_000_00, events[0].GetAmountTiyn())

	var key, trace string
	require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT key, trace_id FROM outbox`).Scan(&key, &trace))
	assert.Equal(t, "acc_a", key, "ключ сообщения — счёт списания: порядок переводов с одного счёта сохраняется")
	assert.Equal(t, "trace-from-client", trace)

	got, err := e.client.GetTransfer(as(keyAliya), &paymentsv1.GetTransferRequest{Id: tr.GetId()})
	require.NoError(t, err)
	assert.True(t, proto.Equal(tr, got.GetTransfer()))
}

func TestCreateTransferValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	cases := map[string]func(r *paymentsv1.CreateTransferRequest){
		"ключ 7 символов":        func(r *paymentsv1.CreateTransferRequest) { r.IdempotencyKey = "1234567" },
		"ключ 65 символов":       func(r *paymentsv1.CreateTransferRequest) { r.IdempotencyKey = strings.Repeat("k", 65) },
		"нет отправителя":        func(r *paymentsv1.CreateTransferRequest) { r.FromAccount = "" },
		"получатель из пробелов": func(r *paymentsv1.CreateTransferRequest) { r.ToAccount = "   " },
		"тот же счёт":            func(r *paymentsv1.CreateTransferRequest) { r.ToAccount = r.FromAccount },
		"ноль":                   func(r *paymentsv1.CreateTransferRequest) { r.AmountTiyn = 0 },
		"минус":                  func(r *paymentsv1.CreateTransferRequest) { r.AmountTiyn = -1 },
		"больше потолка":         func(r *paymentsv1.CreateTransferRequest) { r.AmountTiyn = 1_000_000_000_00 + 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := req("idem-key-valid")
			mutate(r)
			_, err := e.client.CreateTransfer(as(keyAliya), r)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	// Границы, которые должны пройти
	for name, mutate := range map[string]func(r *paymentsv1.CreateTransferRequest){
		"ключ 8 символов": func(r *paymentsv1.CreateTransferRequest) { r.IdempotencyKey = "12345678" },
		"ключ 64 символа": func(r *paymentsv1.CreateTransferRequest) { r.IdempotencyKey = strings.Repeat("k", 64) },
		"один тиын":       func(r *paymentsv1.CreateTransferRequest) { r.IdempotencyKey = "one-tiyn-key"; r.AmountTiyn = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			r := req("")
			mutate(r)
			_, err := e.client.CreateTransfer(as(keyAliya), r)
			assert.NoError(t, err)
		})
	}
}

// Клиент не получил ответ (обрыв сети) и повторил запрос. Второго перевода быть не должно.
func TestIdempotentReplay(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first, err := e.client.CreateTransfer(as(keyAliya), req("idem-key-replay"))
	require.NoError(t, err)

	var header metadata.MD
	again, err := e.client.CreateTransfer(as(keyAliya), req("idem-key-replay"), grpc.Header(&header))
	require.NoError(t, err)
	assert.Equal(t, first.GetTransfer().GetId(), again.GetTransfer().GetId())
	assert.Equal(t, []string{"true"}, header.Get(payments.ReplayedHeader))
	assert.Len(t, e.outbox(t), 1, "событие для Kafka одно")

	changed := req("idem-key-replay")
	changed.AmountTiyn++
	_, err = e.client.CreateTransfer(as(keyAliya), changed)
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "тот же ключ с другой суммой — ошибка, а не молчаливый старый ответ")

	// Ключи разных клиентов независимы
	other, err := e.client.CreateTransfer(as(keyBolat), req("idem-key-replay"))
	require.NoError(t, err)
	assert.NotEqual(t, first.GetTransfer().GetId(), other.GetTransfer().GetId())
}

// 20 одинаковых запросов одновременно (клиент агрессивно повторяет) — ровно один перевод.
func TestConcurrentRetriesCreateOneTransfer(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.client.CreateTransfer(as(keyAliya), req("idem-key-burst"))
			if assert.NoError(t, err) {
				ids <- r.GetTransfer().GetId()
			}
		}()
	}
	wg.Wait()
	close(ids)
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	assert.Len(t, unique, 1)
	assert.Len(t, e.outbox(t), 1)
}

func TestForeignTransferIsNotFound(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r, err := e.client.CreateTransfer(as(keyAliya), req("idem-key-owner"))
	require.NoError(t, err)

	_, err = e.client.GetTransfer(as(keyBolat), &paymentsv1.GetTransferRequest{Id: r.GetTransfer().GetId()})
	assert.Equal(t, codes.NotFound, status.Code(err), "чужой перевод не отличить от несуществующего")

	_, err = e.client.GetTransfer(as(keyAliya), &paymentsv1.GetTransferRequest{Id: platform.NewUUID()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = e.client.GetTransfer(as(keyAliya), &paymentsv1.GetTransferRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func result(t *testing.T, transferID string, outcome eventsv1.TransferResult_Outcome, reason string) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(&eventsv1.TransferResult{EventId: platform.NewUUID(), TransferId: transferID, Outcome: outcome, FailureReason: reason})
	require.NoError(t, err)
	return &kgo.Record{Value: b}
}

func TestResultsUpdateStatusOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	res := &payments.Results{Pool: e.pool, Log: testutil.Log(t)}
	ctx := context.Background()

	ok, err := e.client.CreateTransfer(as(keyAliya), req("idem-key-res-1"))
	require.NoError(t, err)
	id := ok.GetTransfer().GetId()

	require.NoError(t, res.Handle(ctx, result(t, id, eventsv1.TransferResult_OUTCOME_COMPLETED, "")))
	// Опоздавший или повторный итог не перезаписывает итоговый статус
	require.NoError(t, res.Handle(ctx, result(t, id, eventsv1.TransferResult_OUTCOME_FAILED, "insufficient_funds")))

	got, err := e.client.GetTransfer(as(keyAliya), &paymentsv1.GetTransferRequest{Id: id})
	require.NoError(t, err)
	assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_COMPLETED, got.GetTransfer().GetStatus())
	assert.Empty(t, got.GetTransfer().GetFailureReason())

	failed, err := e.client.CreateTransfer(as(keyAliya), req("idem-key-res-2"))
	require.NoError(t, err)
	require.NoError(t, res.Handle(ctx, result(t, failed.GetTransfer().GetId(), eventsv1.TransferResult_OUTCOME_FAILED, "insufficient_funds")))
	got, err = e.client.GetTransfer(as(keyAliya), &paymentsv1.GetTransferRequest{Id: failed.GetTransfer().GetId()})
	require.NoError(t, err)
	assert.Equal(t, paymentsv1.TransferStatus_TRANSFER_STATUS_FAILED, got.GetTransfer().GetStatus())
	assert.Equal(t, "insufficient_funds", got.GetTransfer().GetFailureReason())
}

func TestBrokenResultsAreSkipped(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	res := &payments.Results{Pool: e.pool, Log: testutil.Log(t)}
	assert.NoError(t, res.Handle(context.Background(), &kgo.Record{Value: []byte{0xff, 0x01}}), "битое событие не блокирует очередь")
	assert.NoError(t, res.Handle(context.Background(), result(t, "nope", eventsv1.TransferResult_OUTCOME_UNSPECIFIED, "")))
	assert.NoError(t, res.Handle(context.Background(), result(t, platform.NewUUID(), eventsv1.TransferResult_OUTCOME_COMPLETED, "")),
		"итог неизвестного перевода — не ошибка")
}

func TestParseKeys(t *testing.T) {
	assert.Equal(t, map[string]string{"k1": "c1", "k2": "c2"}, payments.ParseKeys(" k1:c1, k2:c2 ,bad,:x,y:"))
	assert.Empty(t, payments.ParseKeys(""))
}
