CREATE TABLE transfers (
    id              text PRIMARY KEY,
    client_id       text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash    text NOT NULL,
    from_account    text NOT NULL,
    to_account      text NOT NULL,
    amount          bigint NOT NULL CHECK (amount > 0),
    status          text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed')),
    failure_reason  text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    -- Один ключ идемпотентности у клиента — один перевод, даже при одновременных повторах
    UNIQUE (client_id, idempotency_key),
    CHECK (from_account <> to_account)
);
