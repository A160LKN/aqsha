package outbox_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/A160LKN/aqsha/internal/ledger"
	"github.com/A160LKN/aqsha/internal/outbox"
	"github.com/A160LKN/aqsha/internal/platform"
	"github.com/A160LKN/aqsha/internal/testutil"
)

const topic = platform.TopicTransferResults

func add(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	ctx := platform.WithClient(platform.WithTrace(context.Background(), "trace-outbox"), "shop")
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	for i := range n {
		require.NoError(t, outbox.Add(ctx, tx, topic, "key", []byte(fmt.Sprintf("event-%03d", i))))
	}
	require.NoError(t, tx.Commit(ctx))
}

func unsent(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n))
	return n
}

func TestFlushPublishesInOrder(t *testing.T) {
	t.Parallel()
	pool := testutil.DB(t, ledger.Migrations())
	brokers := testutil.Kafka(t, 1)
	add(t, pool, 5)

	r := &outbox.Relay{Service: "test", Pool: pool, Kafka: testutil.Client(t, brokers, ""), Log: testutil.Log(t)}
	n, err := r.Flush(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Zero(t, unsent(t, pool))

	recs := testutil.ReadAll(t, brokers, topic, 5, 5*time.Second)
	require.Len(t, recs, 5)
	for i, rec := range recs {
		assert.Equal(t, fmt.Sprintf("event-%03d", i), string(rec.Value), "порядок сохранён")
		assert.Equal(t, "trace-outbox", platform.HeaderValue(rec, platform.TraceHeader))
		assert.Equal(t, "shop", platform.HeaderValue(rec, platform.ClientHeader), "клиент дошёл до сервиса счетов")
	}
}

func TestRolledBackChangeSendsNothing(t *testing.T) {
	t.Parallel()
	pool := testutil.DB(t, ledger.Migrations())
	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	require.NoError(t, outbox.Add(context.Background(), tx, topic, "k", []byte("x")))
	require.NoError(t, tx.Rollback(context.Background()))
	assert.Zero(t, unsent(t, pool), "откатилось изменение — откатилось и событие")
}

// Kafka недоступна: события остаются в outbox и уходят, когда она вернётся.
func TestKafkaOutageKeepsEvents(t *testing.T) {
	t.Parallel()
	pool := testutil.DB(t, ledger.Migrations())
	add(t, pool, 3)

	down, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.RecordDeliveryTimeout(time.Second))
	require.NoError(t, err)
	defer down.Close()
	_, err = (&outbox.Relay{Service: "test", Pool: pool, Kafka: down, Log: testutil.Log(t)}).Flush(context.Background())
	require.Error(t, err)
	assert.Equal(t, 3, unsent(t, pool), "ни одно событие не потеряно")

	brokers := testutil.Kafka(t, 1)
	n, err := (&outbox.Relay{Service: "test", Pool: pool, Kafka: testutil.Client(t, brokers, ""), Log: testutil.Log(t)}).Flush(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Len(t, testutil.ReadAll(t, brokers, topic, 3, 5*time.Second), 3)
}

// Две реплики сервиса отправляют outbox одновременно — каждое событие уходит один раз.
func TestTwoRelaysDoNotDuplicate(t *testing.T) {
	t.Parallel()
	pool := testutil.DB(t, ledger.Migrations())
	brokers := testutil.Kafka(t, 1)
	add(t, pool, 200)

	var wg sync.WaitGroup
	for range 2 {
		r := &outbox.Relay{Service: "test", Pool: pool, Kafka: testutil.Client(t, brokers, ""), Log: testutil.Log(t), Batch: 10}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for unsent(t, pool) > 0 {
				_, _ = r.Flush(context.Background())
			}
		}()
	}
	wg.Wait()

	recs := testutil.ReadAll(t, brokers, topic, 201, 3*time.Second)
	seen := map[string]int{}
	for _, r := range recs {
		seen[string(r.Value)]++
	}
	assert.Len(t, recs, 200)
	for v, n := range seen {
		assert.Equal(t, 1, n, "событие %s отправлено %d раз", v, n)
	}
}

func TestRunStopsWithContext(t *testing.T) {
	t.Parallel()
	pool := testutil.DB(t, ledger.Migrations())
	brokers := testutil.Kafka(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		(&outbox.Relay{Service: "test", Pool: pool, Kafka: testutil.Client(t, brokers, ""), Log: testutil.Log(t), Interval: 20 * time.Millisecond}).Run(ctx)
		close(done)
	}()
	add(t, pool, 2)
	testutil.Eventually(t, func() bool { return unsent(t, pool) == 0 }, "фоновая отправка подхватила события")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run не остановился после отмены контекста")
	}
}
