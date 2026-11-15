package platform

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// Env возвращает переменную окружения или значение по умолчанию.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// SignalContext отменяется по SIGINT/SIGTERM — так Kubernetes и Docker останавливают сервис.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// NewKafka создаёт клиента Kafka. С group — потребитель с ручной фиксацией смещений.
func NewKafka(brokers []string, group string, topics ...string) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
	}
	if group != "" {
		opts = append(opts,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topics...),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
	}
	return kgo.NewClient(opts...)
}

// MaxRecvMsgSize — самое большое сообщение, которое примет сервер. Перевод — это сотни байт;
// предел защищает память сервиса от огромных запросов (по умолчанию в gRPC — 4 МБ).
const MaxRecvMsgSize = 64 << 10

// NewGRPCServer — gRPC-сервер с защитными настройками: предел размера сообщения, число потоков
// на соединение, таймаут рукопожатия и запрет слишком частых keepalive-пингов (защита от флуда).
func NewGRPCServer(interceptors ...grpc.UnaryServerInterceptor) *grpc.Server {
	return grpc.NewServer(
		grpc.ChainUnaryInterceptor(interceptors...),
		grpc.MaxRecvMsgSize(MaxRecvMsgSize),
		grpc.MaxConcurrentStreams(256),
		grpc.ConnectionTimeout(10*time.Second),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 5 * time.Minute}),
	)
}

// Serve запускает gRPC (с health и reflection) и /metrics, ждёт отмены ctx и останавливается аккуратно.
func Serve(ctx context.Context, log *slog.Logger, grpcAddr, metricsAddr string, register func(*grpc.Server), interceptors ...grpc.UnaryServerInterceptor) error {
	srv := NewGRPCServer(interceptors...)
	register(srv)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	metrics := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errs := make(chan error, 2)
	go func() { errs <- srv.Serve(lis) }()
	go func() {
		if err := metrics.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()
	log.Info("сервис запущен", "grpc", grpcAddr, "metrics", metricsAddr)

	select {
	case <-ctx.Done():
	case err := <-errs:
		return err
	}
	log.Info("останавливаюсь")
	hs.Shutdown()
	stopped := make(chan struct{})
	go func() { srv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		srv.Stop()
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return metrics.Shutdown(sctx)
}

// Healthcheck — режим проверки здоровья для Docker: образ distroless без shell и curl,
// поэтому бинарник сам опрашивает свой /healthz и выходит с кодом 0 или 1.
func Healthcheck() {
	if len(os.Args) < 2 || os.Args[1] != "-healthcheck" {
		return
	}
	addr := Env("METRICS_ADDR", ":9100")
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	cl := http.Client{Timeout: 2 * time.Second}
	resp, err := cl.Get("http://" + addr + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}
