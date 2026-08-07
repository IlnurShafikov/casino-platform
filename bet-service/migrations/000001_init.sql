CREATE TABLE IF NOT EXISTS bets (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    amount          BIGINT NOT NULL,
    game_type       VARCHAR(50) NOT NULL,
    status          VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    win_amount      BIGINT NOT NULL DEFAULT 0,
    idempotency_key VARCHAR(255) UNIQUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT amount_positive CHECK (amount > 0),
    CONSTRAINT win_amount_positive CHECK (win_amount >= 0)
);

CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_type   VARCHAR(255) NOT NULL,
    payload      JSONB NOT NULL,
    sent         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bets_user_id
    ON bets(user_id);

CREATE INDEX IF NOT EXISTS idx_bets_status
    ON bets(status);

CREATE INDEX IF NOT EXISTS idx_outbox_sent
    ON outbox(sent, created_at)
    WHERE sent = FALSE;