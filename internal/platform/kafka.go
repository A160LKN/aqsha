package platform

import (
	"context"
	"errors"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Топики Kafka.
const (
	TopicTransferRequested = "aqsha.transfers.requested"
	TopicTransferResults   = "aqsha.transfers.results"
	TopicTransferDLQ       = "aqsha.transfers.requested.dlq"
)

// Brokers разбирает список брокеров из строки через запятую.
func Brokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// EnsureTopics создаёт топики, если их ещё нет.
func EnsureTopics(ctx context.Context, cl *kgo.Client, partitions int32, topics ...string) error {
	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopics(ctx, partitions, -1, nil, topics...)
	if err != nil {
		return err
	}
	for _, t := range resp.Sorted() {
		if t.Err != nil && !errors.Is(t.Err, kerr.TopicAlreadyExists) {
			return t.Err
		}
	}
	return nil
}

// HeaderValue возвращает значение заголовка записи Kafka.
func HeaderValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
