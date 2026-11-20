-- Владелец счёта — клиент API (определяется по x-api-key). Чужой счёт для клиента не существует:
-- его нельзя прочитать, пополнить или списать с него деньги.
-- Счета, открытые до этой миграции, остаются без владельца и недоступны никому.
ALTER TABLE accounts ADD COLUMN client_id text NOT NULL DEFAULT '';
CREATE INDEX accounts_client ON accounts (client_id);

-- Клиент, от имени которого создано событие, уходит в заголовок Kafka
ALTER TABLE outbox ADD COLUMN client_id text NOT NULL DEFAULT '';
