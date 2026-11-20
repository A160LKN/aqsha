package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/A160LKN/aqsha/gen/aqsha/events/v1"
	"github.com/A160LKN/aqsha/internal/ledger"
	"github.com/A160LKN/aqsha/internal/platform"
	"github.com/A160LKN/aqsha/internal/testutil"
)

type fixture struct {
	pool *pgxpool.Pool
	c    *ledger.Consumer
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	pool := testutil.DB(t, ledger.Migrations())
	return fixture{pool: pool, c: &ledger.Consumer{Pool: pool, Log: testutil.Log(t)}}
}

const client = "shop"

func (f fixture) account(t *testing.T, balance int64) string {
	t.Helper()
	return f.accountOf(t, client, balance)
}

func (f fixture) accountOf(t *testing.T, client string, balance int64) string {
	t.Helper()
	id := platform.NewID("acc")
	_, err := f.pool.Exec(context.Background(), `INSERT INTO accounts (id, owner, balance, client_id) VALUES ($1, 'тест', $2, $3)`, id, balance, client)
	require.NoError(t, err)
	return id
}

func (f fixture) balance(t *testing.T, id string) int64 {
	t.Helper()
	var b int64
	require.NoError(t, f.pool.QueryRow(context.Background(), `SELECT balance FROM accounts WHERE id = $1`, id).Scan(&b))
	return b
}

// results — итоги переводов, которые сервис положил в outbox для отправки в Kafka.
func (f fixture) results(t *testing.T) []*eventsv1.TransferResult {
	t.Helper()
	rows, err := f.pool.Query(context.Background(), `SELECT payload FROM outbox WHERE topic = $1 ORDER BY id`, platform.TopicTransferResults)
	require.NoError(t, err)
	defer rows.Close()
	var out []*eventsv1.TransferResult
	for rows.Next() {
		var b []byte
		require.NoError(t, rows.Scan(&b))
		r := &eventsv1.TransferResult{}
		require.NoError(t, proto.Unmarshal(b, r))
		out = append(out, r)
	}
	return out
}

func event(t *testing.T, from, to string, amount int64) (*kgo.Record, *eventsv1.TransferRequested) {
	t.Helper()
	ev := &eventsv1.TransferRequested{EventId: platform.NewUUID(), TransferId: platform.NewUUID(),
		FromAccount: from, ToAccount: to, AmountTiyn: amount}
	b, err := proto.Marshal(ev)
	require.NoError(t, err)
	return &kgo.Record{Topic: platform.TopicTransferRequested, Key: []byte(from), Value: b,
		Headers: []kgo.RecordHeader{
			{Key: platform.TraceHeader, Value: []byte("trace-" + ev.EventId[:8])},
			{Key: platform.ClientHeader, Value: []byte(client)},
		}}, ev
}

func TestTransferMovesMoney(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a, b := f.account(t, 100_00), f.account(t, 0)
	rec, ev := event(t, a, b, 30_00)

	require.NoError(t, f.c.Handle(context.Background(), rec))

	assert.EqualValues(t, 70_00, f.balance(t, a))
	assert.EqualValues(t, 30_00, f.balance(t, b))
	res := f.results(t)
	require.Len(t, res, 1)
	assert.Equal(t, ev.TransferId, res[0].TransferId)
	assert.Equal(t, eventsv1.TransferResult_OUTCOME_COMPLETED, res[0].Outcome)

	// Сумма проводок по каждому счёту равна его балансу — деньги не появились и не исчезли
	var sumA, sumB int64
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT coalesce(sum(delta) FILTER (WHERE account_id = $1), 0), coalesce(sum(delta) FILTER (WHERE account_id = $2), 0) FROM entries`,
		a, b).Scan(&sumA, &sumB))
	assert.EqualValues(t, -30_00, sumA)
	assert.EqualValues(t, 30_00, sumB)

	// Идентификатор запроса из заголовка Kafka дошёл до исходящего события — цепочку видно в логах
	var trace string
	require.NoError(t, f.pool.QueryRow(context.Background(), `SELECT trace_id FROM outbox`).Scan(&trace))
	assert.Equal(t, "trace-"+ev.EventId[:8], trace)
}

// Повторная доставка — нормальная ситуация для Kafka. Второй раз деньги списаться не должны.
func TestDuplicateEventDoesNotChargeTwice(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a, b := f.account(t, 100_00), f.account(t, 0)
	rec, _ := event(t, a, b, 30_00)

	for range 3 {
		require.NoError(t, f.c.Handle(context.Background(), rec))
	}

	assert.EqualValues(t, 70_00, f.balance(t, a), "списано один раз, а не три")
	assert.EqualValues(t, 30_00, f.balance(t, b))
	assert.Len(t, f.results(t), 1, "итог отправляется один раз")
}

func TestInsufficientFunds(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a, b := f.account(t, 10_00), f.account(t, 0)
	rec, _ := event(t, a, b, 10_01)

	require.NoError(t, f.c.Handle(context.Background(), rec))

	assert.EqualValues(t, 10_00, f.balance(t, a), "баланс не тронут")
	assert.Zero(t, f.balance(t, b))
	res := f.results(t)
	require.Len(t, res, 1)
	assert.Equal(t, eventsv1.TransferResult_OUTCOME_FAILED, res[0].Outcome)
	assert.Equal(t, ledger.ReasonInsufficientFunds, res[0].FailureReason)
}

func TestExactBalanceCanBeSpent(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a, b := f.account(t, 10_00), f.account(t, 0)
	rec, _ := event(t, a, b, 10_00)
	require.NoError(t, f.c.Handle(context.Background(), rec))
	assert.Zero(t, f.balance(t, a), "граница: весь баланс до тиына можно перевести")
}

func TestUnknownAccount(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.account(t, 100_00)
	for name, pair := range map[string][2]string{
		"нет получателя":  {a, "acc_000000000000"},
		"нет отправителя": {"acc_000000000000", a},
	} {
		rec, _ := event(t, pair[0], pair[1], 1_00)
		require.NoError(t, f.c.Handle(context.Background(), rec), name)
	}
	assert.EqualValues(t, 100_00, f.balance(t, a))
	for _, r := range f.results(t) {
		assert.Equal(t, ledger.ReasonAccountNotFound, r.FailureReason)
	}
}

// Клиент API может списывать только со своих счетов: чужой счёт для него не существует.
// Событие без клиента (так оно не может прийти от сервиса платежей) тоже ничего не списывает.
func TestCannotDebitForeignAccount(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	foreign, mine := f.accountOf(t, "other-client", 100_00), f.account(t, 0)

	rec, _ := event(t, foreign, mine, 50_00)
	require.NoError(t, f.c.Handle(context.Background(), rec))

	noClient, _ := event(t, foreign, mine, 50_00)
	noClient.Headers = noClient.Headers[:1] // только trace
	require.NoError(t, f.c.Handle(context.Background(), noClient))

	assert.EqualValues(t, 100_00, f.balance(t, foreign), "с чужого счёта ничего не списано")
	assert.EqualValues(t, 0, f.balance(t, mine))
	res := f.results(t)
	require.Len(t, res, 2)
	for _, r := range res {
		assert.Equal(t, eventsv1.TransferResult_OUTCOME_FAILED, r.Outcome)
		assert.Equal(t, ledger.ReasonAccountNotFound, r.FailureReason)
	}
}

// Зачислять можно на счёт другого клиента — так платят за товары и услуги.
func TestCanCreditForeignAccount(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	mine, foreign := f.account(t, 100_00), f.accountOf(t, "other-client", 0)
	rec, _ := event(t, mine, foreign, 40_00)
	require.NoError(t, f.c.Handle(context.Background(), rec))
	assert.EqualValues(t, 60_00, f.balance(t, mine))
	assert.EqualValues(t, 40_00, f.balance(t, foreign))
}

// Битое или заведомо неверное событие повтором не исправить — его надо убрать в DLQ, а не крутить вечно.
func TestPoisonEvents(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a, b := f.account(t, 100_00), f.account(t, 0)
	cases := map[string]*eventsv1.TransferRequested{
		"ноль":         {EventId: "e1", TransferId: "t1", FromAccount: a, ToAccount: b, AmountTiyn: 0},
		"минус":        {EventId: "e2", TransferId: "t2", FromAccount: a, ToAccount: b, AmountTiyn: -100},
		"тот же счёт":  {EventId: "e3", TransferId: "t3", FromAccount: a, ToAccount: a, AmountTiyn: 100},
		"без event_id": {TransferId: "t4", FromAccount: a, ToAccount: b, AmountTiyn: 100},
		"без счёта":    {EventId: "e5", TransferId: "t5", ToAccount: b, AmountTiyn: 100},
	}
	for name, ev := range cases {
		t.Run(name, func(t *testing.T) {
			v, err := proto.Marshal(ev)
			require.NoError(t, err)
			err = f.c.Handle(context.Background(), &kgo.Record{Value: v})
			assert.ErrorIs(t, err, ledger.ErrPoison)
		})
	}
	t.Run("не protobuf", func(t *testing.T) {
		err := f.c.Handle(context.Background(), &kgo.Record{Value: []byte{0xff, 0xff, 0xff}})
		assert.ErrorIs(t, err, ledger.ErrPoison)
	})
	assert.EqualValues(t, 100_00, f.balance(t, a))
	assert.Empty(t, f.results(t))
}

// 50 переводов с одного счёта одновременно, денег хватает ровно на 20.
// Без блокировки строк счёт ушёл бы в минус или «потерял» списания.
func TestConcurrentDebitsNeverOverdraw(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a, b := f.account(t, 20_00), f.account(t, 0)

	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for range 50 {
		rec, _ := event(t, a, b, 1_00)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- f.c.Handle(context.Background(), rec)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.Zero(t, f.balance(t, a))
	assert.EqualValues(t, 20_00, f.balance(t, b))
	var completed, failed int
	for _, r := range f.results(t) {
		if r.Outcome == eventsv1.TransferResult_OUTCOME_COMPLETED {
			completed++
		} else {
			failed++
		}
	}
	assert.Equal(t, 20, completed)
	assert.Equal(t, 30, failed)
}

// Встречные переводы A→B и B→A одновременно. Если блокировать счета в порядке «отправитель, получатель»,
// две транзакции возьмут их крест-накрест и PostgreSQL прервёт одну с deadlock.
func TestOppositeTransfersDoNotDeadlock(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a, b := f.account(t, 1000_00), f.account(t, 1000_00)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := range 100 {
		from, to := a, b
		if i%2 == 1 {
			from, to = b, a
		}
		rec, _ := event(t, from, to, 1_00)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- f.c.Handle(context.Background(), rec)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "ни одна транзакция не прервана")
	}
	assert.EqualValues(t, 2000_00, f.balance(t, a)+f.balance(t, b), "общая сумма сохранилась")
	assert.EqualValues(t, 1000_00, f.balance(t, a), "50 туда и 50 обратно")
}

// Полный цикл через Kafka: обычное событие, его дубль и битое событие.
// Битое уходит в DLQ, дубль ничего не меняет, обработка не останавливается.
func TestRunThroughKafka(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	brokers := testutil.Kafka(t, 3)
	a, b := f.account(t, 100_00), f.account(t, 0)

	producer := testutil.Client(t, brokers, "")
	rec, _ := event(t, a, b, 40_00)
	dup := *rec
	poison := &kgo.Record{Topic: platform.TopicTransferRequested, Key: []byte(a), Value: []byte("не protobuf")}
	rec2, _ := event(t, a, b, 10_00)
	require.NoError(t, producer.ProduceSync(context.Background(), rec, &dup, poison, rec2).FirstErr())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := testutil.Client(t, brokers, "ledger", platform.TopicTransferRequested)
	c := &ledger.Consumer{Pool: f.pool, Kafka: consumer, Log: testutil.Log(t)}
	go c.Run(ctx)

	testutil.Eventually(t, func() bool { return f.balance(t, b) == 50_00 }, "оба перевода проведены")
	assert.EqualValues(t, 50_00, f.balance(t, a))

	dlq := testutil.ReadAll(t, brokers, platform.TopicTransferDLQ, 1, 10*time.Second)
	require.Len(t, dlq, 1)
	assert.Equal(t, "не protobuf", string(dlq[0].Value))
	assert.Contains(t, platform.HeaderValue(dlq[0], "error"), "не разбирается")
}

// Сервис провёл перевод и упал, не успев зафиксировать смещение в Kafka.
// После перезапуска то же событие приходит снова — деньги не должны списаться второй раз.
func TestCrashBeforeOffsetCommit(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	brokers := testutil.Kafka(t, 1)
	a, b := f.account(t, 100_00), f.account(t, 0)
	rec, _ := event(t, a, b, 25_00)
	require.NoError(t, testutil.Client(t, brokers, "").ProduceSync(context.Background(), rec).FirstErr())

	// «До падения»: событие обработано, но смещение не зафиксировано
	require.NoError(t, f.c.Handle(context.Background(), rec))
	require.EqualValues(t, 75_00, f.balance(t, a))

	// «После перезапуска»: группа читает топик с начала и получает то же событие
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := testutil.Client(t, brokers, "ledger", platform.TopicTransferRequested)
	go (&ledger.Consumer{Pool: f.pool, Kafka: consumer, Log: testutil.Log(t)}).Run(ctx)

	testutil.Eventually(t, func() bool {
		var n int
		_ = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM processed_events`).Scan(&n)
		return n == 1
	}, "событие дошло до сервиса повторно")
	time.Sleep(300 * time.Millisecond)
	assert.EqualValues(t, 75_00, f.balance(t, a), "повтор после падения не списал деньги второй раз")
	assert.Len(t, f.results(t), 1)
}

// База недоступна — событие не теряется: обработчик повторяет, а смещение не фиксируется.
func TestDatabaseOutageIsRetried(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a, b := f.account(t, 100_00), f.account(t, 0)
	rec, _ := event(t, a, b, 1_00)

	broken := &ledger.Consumer{Pool: closedPool(t), Log: testutil.Log(t)}
	err := broken.Handle(context.Background(), rec)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ledger.ErrPoison), "сбой базы — временная ошибка, не повод выбросить событие в DLQ")

	require.NoError(t, f.c.Handle(context.Background(), rec), "после восстановления событие обрабатывается")
	assert.EqualValues(t, 99_00, f.balance(t, a))
}

func TestIsPermanent(t *testing.T) {
	t.Parallel()
	for code, permanent := range map[string]bool{
		"23505": true,  // unique_violation
		"23514": true,  // check_violation
		"22003": true,  // numeric_value_out_of_range
		"40P01": false, // deadlock_detected — повтор поможет
		"40001": false, // serialization_failure
		"57P01": false, // admin_shutdown — база перезапускается
	} {
		err := fmt.Errorf("обёртка: %w", &pgconn.PgError{Code: code})
		assert.Equal(t, permanent, ledger.IsPermanent(err), code)
	}
	assert.True(t, ledger.IsPermanent(fmt.Errorf("%w: битое", ledger.ErrPoison)))
	assert.False(t, ledger.IsPermanent(errors.New("connection refused")))
}

// Событие, которое база отвергает каждый раз (нарушено ограничение), уходит в DLQ,
// а не останавливает партицию: следующие переводы проводятся.
func TestConstraintViolationGoesToDLQAndPartitionMovesOn(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	brokers := testutil.Kafka(t, 1)
	a, b, cursed := f.account(t, 100_00), f.account(t, 0), f.account(t, 0)
	// Счёт, на который база не даёт зачислить: так выглядит постоянная ошибка, которую повтор не исправит
	_, err := f.pool.Exec(context.Background(), `
		CREATE FUNCTION reject_cursed() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.id = '`+cursed+`' THEN RAISE EXCEPTION 'счёт закрыт' USING ERRCODE = 'check_violation'; END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER reject_cursed BEFORE UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION reject_cursed();`)
	require.NoError(t, err)

	bad, _ := event(t, a, cursed, 10_00)
	good, _ := event(t, a, b, 30_00)
	require.NoError(t, testutil.Client(t, brokers, "").ProduceSync(context.Background(), bad, good).FirstErr())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &ledger.Consumer{Pool: f.pool, Kafka: testutil.Client(t, brokers, "ledger", platform.TopicTransferRequested),
		Log: testutil.Log(t)}
	go c.Run(ctx)

	testutil.Eventually(t, func() bool { return f.balance(t, b) == 30_00 }, "перевод после «вечной» ошибки проведён")
	assert.EqualValues(t, 70_00, f.balance(t, a), "отвергнутый перевод ничего не списал")
	dlq := testutil.ReadAll(t, brokers, platform.TopicTransferDLQ, 1, 10*time.Second)
	require.Len(t, dlq, 1)
	assert.Contains(t, platform.HeaderValue(dlq[0], "error"), "счёт закрыт")
}

func closedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:nothing@127.0.0.1:1/none?connect_timeout=1")
	require.NoError(t, err)
	pool.Close()
	return pool
}
