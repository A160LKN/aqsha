// Package testutil — окружение для интеграционных тестов: своя база на каждый тест и Kafka в памяти.
package testutil

import (
	"context"
	"io/fs"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/A160LKN/aqsha/internal/platform"
)

// DefaultDSN — PostgreSQL для тестов; в CI задаётся через TEST_DATABASE_URL.
const DefaultDSN = "postgres://aqsha:aqsha@localhost:5432/postgres?sslmode=disable"

// DB создаёт пустую базу с миграциями сервиса и удаляет её после теста.
// Отдельная база на тест — тесты не мешают друг другу и могут идти параллельно.
func DB(t *testing.T, migrations fs.FS) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	admin := os.Getenv("TEST_DATABASE_URL")
	if admin == "" {
		admin = DefaultDSN
	}
	conn, err := pgx.Connect(ctx, admin)
	require.NoError(t, err, "нужен PostgreSQL: задайте TEST_DATABASE_URL")
	name := "test_" + strings.ReplaceAll(platform.NewUUID(), "-", "")[:16]
	_, err = conn.Exec(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)
	u, err := url.Parse(admin)
	require.NoError(t, err)
	u.Path = "/" + name
	pool, err := pgxpool.New(ctx, u.String())
	require.NoError(t, err)
	require.NoError(t, platform.Migrate(ctx, pool, migrations))
	t.Cleanup(func() {
		pool.Close()
		_, _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = conn.Close(context.Background())
	})
	return pool
}

// Kafka поднимает Kafka в памяти (kfake) с нужными топиками и возвращает адреса брокеров.
func Kafka(t *testing.T, partitions int32) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions,
		platform.TopicTransferRequested, platform.TopicTransferResults, platform.TopicTransferDLQ))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

// Client — клиент Kafka, который закроется после теста.
func Client(t *testing.T, brokers []string, group string, topics ...string) *kgo.Client {
	t.Helper()
	cl, err := platform.NewKafka(brokers, group, topics...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return cl
}

// ReadAll читает из топика записи, пока не наберёт want или не истечёт timeout.
func ReadAll(t *testing.T, brokers []string, topic string, want int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []*kgo.Record
	for len(out) < want && ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

// GRPC поднимает gRPC-сервер в памяти (bufconn) — настоящий транспорт без сетевых портов.
func GRPC(t *testing.T, register func(*grpc.Server), interceptors ...grpc.UnaryServerInterceptor) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors...))
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); srv.Stop() })
	return conn
}

// Log — логгер, который пишет в вывод теста только при падении.
func Log(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// Eventually ждёт, пока условие станет истинным.
func Eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 15*time.Second, 50*time.Millisecond, msg)
}
