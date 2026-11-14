-- Transactional outbox: события для Kafka пишутся в той же транзакции, что и данные
CREATE TABLE outbox (
    id         bigserial PRIMARY KEY,
    topic      text NOT NULL,
    key        text NOT NULL,
    payload    bytea NOT NULL,
    trace_id   text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);
CREATE INDEX outbox_unsent ON outbox (id) WHERE sent_at IS NULL;
