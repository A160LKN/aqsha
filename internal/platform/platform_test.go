package platform_test

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/A160LKN/aqsha/internal/ledger"
	"github.com/A160LKN/aqsha/internal/platform"
	"github.com/A160LKN/aqsha/internal/testutil"
)

func TestBrokers(t *testing.T) {
	assert.Equal(t, []string{"a:1", "b:2"}, platform.Brokers(" a:1, ,b:2 "))
	assert.Empty(t, platform.Brokers(""))
}

func TestEnv(t *testing.T) {
	t.Setenv("AQSHA_TEST_ENV", "set")
	assert.Equal(t, "set", platform.Env("AQSHA_TEST_ENV", "default"))
	assert.Equal(t, "default", platform.Env("AQSHA_TEST_MISSING", "default"))
}

func TestIDs(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := platform.NewUUID()
		assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, id)
		seen[id] = true
	}
	assert.Len(t, seen, 1000)
	assert.Regexp(t, `^acc_[0-9a-f]{12}$`, platform.NewID("acc"))
}

func TestHeaderValue(t *testing.T) {
	r := &kgo.Record{Headers: []kgo.RecordHeader{{Key: "a", Value: []byte("1")}}}
	assert.Equal(t, "1", platform.HeaderValue(r, "a"))
	assert.Empty(t, platform.HeaderValue(r, "b"))
}

func TestEnsureTopicsIsRepeatable(t *testing.T) {
	brokers := testutil.Kafka(t, 1)
	cl := testutil.Client(t, brokers, "")
	ctx := context.Background()
	require.NoError(t, platform.EnsureTopics(ctx, cl, 3, "new.topic", platform.TopicTransferResults))
	require.NoError(t, platform.EnsureTopics(ctx, cl, 3, "new.topic"), "второй запуск сервиса не падает на существующих топиках")
	details, err := kadm.NewClient(cl).ListTopics(ctx, "new.topic")
	require.NoError(t, err)
	assert.Len(t, details["new.topic"].Partitions, 3)
}

func TestConnectDBFailsFast(t *testing.T) {
	_, err := platform.ConnectDB(context.Background(), "postgres://x:y@127.0.0.1:1/z?connect_timeout=1")
	assert.Error(t, err)
	_, err = platform.ConnectDB(context.Background(), "::not a url::")
	assert.Error(t, err)
}

// Миграции применяются один раз, а несколько реплик, стартующих одновременно, не мешают друг другу.
func TestMigrateOnceUnderConcurrency(t *testing.T) {
	pool := testutil.DB(t, ledger.Migrations()) // уже применены
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 5 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- platform.Migrate(context.Background(), pool, ledger.Migrations()) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&n))
	assert.Equal(t, 2, n)
}

func TestMigrateErrors(t *testing.T) {
	pool := testutil.DB(t, ledger.Migrations())
	err := platform.Migrate(context.Background(), pool, fstest.MapFS{})
	assert.ErrorContains(t, err, "нет файлов миграций")

	broken := fstest.MapFS{"900_broken.sql": {Data: []byte("CREATE TABLE ведомость (")}}
	err = platform.Migrate(context.Background(), pool, broken)
	assert.ErrorContains(t, err, "900_broken.sql")
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations WHERE name = '900_broken.sql'`).Scan(&n))
	assert.Zero(t, n, "упавшая миграция не отмечена применённой")
}

// Перехватчик: идентификатор запроса прокидывается или создаётся, паника превращается в Internal, а не роняет сервер.
func TestUnaryInterceptor(t *testing.T) {
	ic := platform.UnaryInterceptor(testutil.Log(t))
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}

	var seen string
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(platform.TraceHeader, "from-client"))
	_, err := ic(ctx, nil, info, func(ctx context.Context, _ any) (any, error) { seen = platform.Trace(ctx); return nil, nil })
	require.NoError(t, err)
	assert.Equal(t, "from-client", seen)

	_, _ = ic(context.Background(), nil, info, func(ctx context.Context, _ any) (any, error) { seen = platform.Trace(ctx); return nil, nil })
	assert.Len(t, seen, 36, "без заголовка создаётся новый UUID")

	tooLong := metadata.NewIncomingContext(context.Background(), metadata.Pairs(platform.TraceHeader, string(make([]byte, 65))))
	_, _ = ic(tooLong, nil, info, func(ctx context.Context, _ any) (any, error) { seen = platform.Trace(ctx); return nil, nil })
	assert.Len(t, seen, 36, "слишком длинный заголовок от клиента не попадает в логи")

	_, err = ic(context.Background(), nil, info, func(context.Context, any) (any, error) { panic("бум") })
	assert.Equal(t, codes.Internal, status.Code(err))
}

// Сервер поднимает gRPC с health и /metrics, а по сигналу останавливается аккуратно.
func TestServeLifecycle(t *testing.T) {
	grpcAddr, metricsAddr := freePort(t), freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- platform.Serve(ctx, testutil.Log(t), grpcAddr, metricsAddr, func(*grpc.Server) {}, platform.UnaryInterceptor(testutil.Log(t)))
	}()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	testutil.Eventually(t, func() bool {
		r, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
		return err == nil && r.GetStatus() == healthpb.HealthCheckResponse_SERVING
	}, "gRPC health отвечает SERVING")

	for _, path := range []string{"/healthz", "/metrics"} {
		resp, err := http.Get("http://" + metricsAddr + path)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, path)
	}

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("сервер не остановился")
	}
}

func TestServePortBusy(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()
	err = platform.Serve(context.Background(), testutil.Log(t), lis.Addr().String(), freePort(t), func(*grpc.Server) {})
	assert.Error(t, err, "занятый порт — ошибка при старте, а не тихая работа без API")
}

func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}
