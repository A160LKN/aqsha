// Сервис счетов: gRPC LedgerService и обработчик событий переводов из Kafka.
package main

import (
	"context"
	"os"

	"google.golang.org/grpc"

	ledgerv1 "github.com/A160LKN/aqsha/gen/aqsha/ledger/v1"
	"github.com/A160LKN/aqsha/internal/ledger"
	"github.com/A160LKN/aqsha/internal/outbox"
	"github.com/A160LKN/aqsha/internal/platform"
)

func main() {
	platform.Healthcheck()
	log := platform.NewLogger("ledger")
	ctx, stop := platform.SignalContext()
	defer stop()
	if err := run(ctx); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := platform.NewLogger("ledger")
	pool, err := platform.ConnectDB(ctx, platform.Env("DATABASE_URL", "postgres://aqsha:aqsha@localhost:5432/ledger"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := platform.Migrate(ctx, pool, ledger.Migrations()); err != nil {
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
	consumer, err := platform.NewKafka(brokers, "ledger", platform.TopicTransferRequested)
	if err != nil {
		return err
	}
	defer consumer.Close()

	go (&outbox.Relay{Service: "ledger", Pool: pool, Kafka: producer, Log: log}).Run(ctx)
	go (&ledger.Consumer{Pool: pool, Kafka: consumer, Log: log}).Run(ctx)

	svc := &ledger.Service{Pool: pool, Log: log}
	return platform.Serve(ctx, log, platform.Env("GRPC_ADDR", ":9090"), platform.Env("METRICS_ADDR", ":9100"),
		func(s *grpc.Server) { ledgerv1.RegisterLedgerServiceServer(s, svc) },
		platform.UnaryInterceptor(log))
}
