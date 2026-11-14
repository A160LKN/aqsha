package platform

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TraceHeader — метаданные gRPC и заголовок Kafka с идентификатором запроса.
// По нему в логах всех сервисов находится вся цепочка одного перевода.
const TraceHeader = "x-request-id"

type traceKey struct{}

// WithTrace кладёт идентификатор запроса в контекст.
func WithTrace(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceKey{}, id)
}

// Trace достаёт идентификатор запроса из контекста.
func Trace(ctx context.Context) string {
	id, _ := ctx.Value(traceKey{}).(string)
	return id
}

// NewLogger — JSON-логи в stdout: их собирает любой стек (Loki, ELK, kubectl logs).
func NewLogger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("service", service)
}

var rpcDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "aqsha_grpc_server_handling_seconds",
	Help:    "Время обработки gRPC-вызовов по методам и кодам ответа.",
	Buckets: prometheus.DefBuckets,
}, []string{"method", "code"})

// UnaryInterceptor — идентификатор запроса, восстановление после паники, лог и метрика каждого вызова.
func UnaryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(TraceHeader); len(v) > 0 && len(v[0]) <= 64 {
				id = v[0]
			}
		}
		if id == "" {
			id = NewUUID()
		}
		ctx = WithTrace(ctx, id)
		_ = grpc.SetHeader(ctx, metadata.Pairs(TraceHeader, id))
		start := time.Now()
		defer func() {
			if p := recover(); p != nil {
				log.Error("паника в обработчике", "method", info.FullMethod, "trace_id", id, "panic", p)
				err = status.Error(codes.Internal, "внутренняя ошибка")
			}
			code := status.Code(err)
			rpcDuration.WithLabelValues(info.FullMethod, code.String()).Observe(time.Since(start).Seconds())
			level := slog.LevelInfo
			if code == codes.Internal || code == codes.Unknown || code == codes.Unavailable {
				level = slog.LevelError
			}
			log.Log(ctx, level, "grpc", "method", info.FullMethod, "code", code.String(),
				"ms", time.Since(start).Milliseconds(), "trace_id", id)
		}()
		return handler(ctx, req)
	}
}
