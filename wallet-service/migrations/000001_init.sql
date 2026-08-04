-- Таблица кошельков игроков
CREATE TABLE IF NOT EXISTS wallets (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL UNIQUE,
    balance    BIGINT NOT NULL DEFAULT 0,
    version    BIGINT NOT NULL DEFAULT 0,  -- для optimistic locking
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Баланс не может быть отрицательным
    CONSTRAINT balance_positive CHECK (balance >= 0)
);

-- Таблица транзакций (неизменяемый лог)
CREATE TABLE IF NOT EXISTS transactions (
    id               BIGSERIAL PRIMARY KEY,
    user_id          BIGINT NOT NULL,
    type             VARCHAR(50) NOT NULL,  -- DEPOSIT, DEBIT, CREDIT, WITHDRAWAL
    amount           BIGINT NOT NULL,
    balance_before   BIGINT NOT NULL,
    balance_after    BIGINT NOT NULL,
    reference_id     VARCHAR(255),          -- bet_id, payment_id и т.д.
    reference_type   VARCHAR(50),           -- BET, PAYMENT и т.д.
    idempotency_key  VARCHAR(255) UNIQUE,   -- защита от дублей
    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT amount_positive CHECK (amount > 0)
);

-- Таблица outbox для надёжной доставки событий в Kafka
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_type   VARCHAR(255) NOT NULL,
    payload      JSONB NOT NULL,
    sent         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_wallets_user_id
    ON wallets(user_id);

CREATE INDEX IF NOT EXISTS idx_transactions_user_id
    ON transactions(user_id);

CREATE INDEX IF NOT EXISTS idx_transactions_idempotency
    ON transactions(idempotency_key);

CREATE INDEX IF NOT EXISTS idx_outbox_sent
    ON outbox(sent, created_at)
    WHERE sent = FALSE;