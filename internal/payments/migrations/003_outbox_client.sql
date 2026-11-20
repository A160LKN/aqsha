-- Клиент, от имени которого создан перевод, уходит в заголовок Kafka:
-- сервис счетов проверяет, что счёт списания принадлежит этому клиенту
ALTER TABLE outbox ADD COLUMN client_id text NOT NULL DEFAULT '';
