// Сервис платежей: gRPC PaymentService, отправка событий переводов и приём их итогов.
package main

import (
	"context"
	"errors"
	"os"

	"google.golang.org/grpc"

	paymentsv1 "github.com/A160LKN/aqsha/gen/aqsha/payments/v1"
	"github.com/A160LKN/aqsha/internal/outbox"
	"github.com/A160LKN/aqsha/internal/payments"
	"github.com/A160LKN/aqsha/internal/platform"
)

func main() {
	platform.Healthcheck()
	log := platform.NewLogger("payments")
	ctx, stop := platform.SignalContext()
	defer stop()
	if err := run(ctx); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := platform.NewLogger("payments")
	keys := payments.ParseKeys(os.Getenv("API_KEYS"))
	if len(keys) == 0 {
		return errors.New("API_KEYS не задан: нужен хотя бы один ключ вида ключ:клиент")
	}
	pool, err := platform.ConnectDB(ctx, platform.Env("DATABASE_URL", "postgres://aqsha:aqsha@localhost:5432/payments"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := platform.Migrate(ctx, pool, payments.Migrations()); err != nil {
		return err
	}
	brokers := platform.Brokers(platform.Env("KAFKA_BROKERS", "localhost:9092"))
	producer, err := platform.NewKafka(brokers, "")
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := platform.EnsureTopics(ctx, producer, 3, platform.TopicTransferRequested, platform.TopicTransferResults, platform.TopicTransferDLQ); err != nil {
		return err
	}
	consumer, err := platform.NewKafka(brokers, "payments", platform.TopicTransferResults)
	if err != nil {
		return err
	}
	defer consumer.Close()

	go (&outbox.Relay{Service: "payments", Pool: pool, Kafka: producer, Log: log}).Run(ctx)
	go (&payments.Results{Pool: pool, Kafka: consumer, Log: log}).Run(ctx)

	svc := &payments.Service{Pool: pool, Log: log}
	return platform.Serve(ctx, log, platform.Env("GRPC_ADDR", ":9090"), platform.Env("METRICS_ADDR", ":9100"),
		func(s *grpc.Server) { paymentsv1.RegisterPaymentServiceServer(s, svc) },
		platform.UnaryInterceptor(log), payments.AuthInterceptor(keys))
}
