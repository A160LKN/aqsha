CREATE TABLE accounts (
    id         text PRIMARY KEY,
    owner      text NOT NULL,
    currency   text NOT NULL DEFAULT 'KZT',
    -- База сама не даст уйти в минус, даже если в коде ошибка
    balance    bigint NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Проводки: каждое движение денег. Сумма проводок по счёту равна его балансу.
CREATE TABLE entries (
    id          bigserial PRIMARY KEY,
    account_id  text NOT NULL REFERENCES accounts (id),
    delta       bigint NOT NULL CHECK (delta <> 0),
    transfer_id text,
    deposit_id  text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX entries_account ON entries (account_id);

-- Пополнения: deposit_id уникален, повторный запрос ничего не зачислит
CREATE TABLE deposits (
    deposit_id  text PRIMARY KEY,
    account_id  text NOT NULL REFERENCES accounts (id),
    amount      bigint NOT NULL CHECK (amount > 0),
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Обработанные события Kafka: повторная доставка того же события ничего не изменит
CREATE TABLE processed_events (
    event_id     text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
