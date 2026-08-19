// Package repository содержит слой доступа к данным bet-сервиса: ставки
// и события паттерна transactional outbox для их надёжной публикации.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sharedKafka "github.com/casino/shared/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Возможные значения статуса ставки, хранящиеся в bets.status.
const (
	BetStatusPending   = "PENDING"
	BetStatusActive    = "ACTIVE"
	BetStatusWon       = "WON"
	BetStatusLost      = "LOST"
	BetStatusCancelled = "CANCELLED"
)

// Bet — ставка игрока, хранящаяся в таблице bets.
type Bet struct {
	ID             int64
	UserID         int64
	Amount         int64
	GameType       string
	Status         string
	WinAmount      int64
	IdempotencyKey string
	CreatedAt      time.Time
}

// NewBet создаёт пустую ставку с нулевыми значениями полей.
func NewBet() *Bet {
	return &Bet{}
}

// BetRepository описывает доступ к ставкам и outbox-событиям в БД.
type BetRepository interface {
	// Create создаёт ставку и записывает присвоенный ID в bet.ID.
	Create(ctx context.Context, tx pgx.Tx, bet *Bet) error
	// GetByID возвращает ставку по ID. Если ставка не найдена,
	// возвращает ErrBetNotFound.
	GetByID(ctx context.Context, id int64) (*Bet, error)
	// UpdateStatus обновляет статус и выигрыш ставки.
	UpdateStatus(ctx context.Context, tx pgx.Tx, id int64, status string, winAmount int64) error
	// GetByIdempotencyKey возвращает ставку по ключу идемпотентности.
	// Если ставка не найдена, возвращает (nil, nil).
	GetByIdempotencyKey(ctx context.Context, key string) (*Bet, error)
	// CreateOutboxEvent записывает событие в outbox в рамках переданной транзакции.
	CreateOutboxEvent(ctx context.Context, tx pgx.Tx, eventType string, payload any) error
	// BeginTx начинает новую транзакцию БД.
	BeginTx(ctx context.Context) (pgx.Tx, error)
	// GetPendingOutboxEvents выбирает и блокирует до limit неотправленных
	// outbox-событий в рамках переданной транзакции (SELECT ... FOR UPDATE
	// SKIP LOCKED), чтобы конкурентные воркеры не забрали одни и те же события.
	// Блокировка удерживается до фиксации tx, поэтому MarkOutboxEventSent
	// для выбранных событий должен вызываться в этой же транзакции.
	GetPendingOutboxEvents(ctx context.Context, tx pgx.Tx, limit int) ([]sharedKafka.OutboxEvent, error)
	// MarkOutboxEventSent помечает outbox-событие как отправленное в рамках
	// переданной транзакции.
	MarkOutboxEventSent(ctx context.Context, tx pgx.Tx, id int64) error
}

// betRepository — реализация BetRepository поверх пула соединений pgx.
type betRepository struct {
	db *pgxpool.Pool
}

// NewBetRepository создаёт BetRepository поверх переданного пула соединений.
func NewBetRepository(db *pgxpool.Pool) BetRepository {
	return &betRepository{db: db}
}

// Create реализует BetRepository.
func (b *betRepository) Create(
	ctx context.Context,
	tx pgx.Tx,
	bet *Bet,
) error {
	err := tx.QueryRow(ctx, `
	INSERT INTO bets (user_id, amount, game_type, status, idempotency_key)
	VALUES ($1, $2, $3, $4, $5)
	RETURNING id, created_at
	`,
		bet.UserID,
		bet.Amount,
		bet.GameType,
		bet.Status,
		bet.IdempotencyKey,
	).Scan(&bet.ID, &bet.CreatedAt)
	if err != nil {
		return fmt.Errorf("create bet: %w", err)
	}

	return nil
}

// GetByID реализует BetRepository.
func (b *betRepository) GetByID(
	ctx context.Context,
	id int64,
) (*Bet, error) {
	bet := NewBet()

	err := b.db.QueryRow(ctx, `
	SELECT id, user_id, amount, game_type,
	status, win_amount, idempotency_key, created_at
	FROM bets
	WHERE id = $1
	`, id).Scan(
		&bet.ID,
		&bet.UserID,
		&bet.Amount,
		&bet.GameType,
		&bet.Status,
		&bet.WinAmount,
		&bet.IdempotencyKey,
		&bet.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrBetNotFound
		}

		return nil, fmt.Errorf("get bet: %w", err)
	}

	return bet, nil
}

// UpdateStatus реализует BetRepository.
func (b *betRepository) UpdateStatus(
	ctx context.Context,
	tx pgx.Tx,
	id int64,
	status string,
	winAmount int64,
) error {
	_, err := tx.Exec(ctx, `
	UPDATE bets
	SET status = $1,
		win_amount = $2,
		updated_at = NOW()
	WHERE id = $3
	`, status, winAmount, id)
	if err != nil {
		return fmt.Errorf("update bet status: %w", err)
	}

	return nil
}

// GetByIdempotencyKey реализует BetRepository.
func (b *betRepository) GetByIdempotencyKey(
	ctx context.Context,
	key string,
) (*Bet, error) {
	bet := NewBet()

	err := b.db.QueryRow(ctx, `
	SELECT id, user_id, amount, game_type,
		   status, win_amount, idempotency_key, created_at
	FROM bets
	WHERE idempotency_key = $1
	`, key).Scan(
		&bet.ID,
		&bet.UserID,
		&bet.Amount,
		&bet.GameType,
		&bet.Status,
		&bet.WinAmount,
		&bet.IdempotencyKey,
		&bet.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("get bet by idempotency key: %w", err)
	}

	return bet, nil
}

// CreateOutboxEvent реализует BetRepository.
func (b *betRepository) CreateOutboxEvent(
	ctx context.Context,
	tx pgx.Tx,
	eventType string,
	payload any,
) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	_, err = tx.Exec(ctx, `
	INSERT INTO outbox (event_type, payload)
	VALUES ($1, $2)
	`, eventType, data)

	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	return nil
}

// BeginTx реализует BetRepository.
func (b *betRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := b.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}

	return tx, nil
}

// GetPendingOutboxEvents реализует BetRepository.
func (b *betRepository) GetPendingOutboxEvents(
	ctx context.Context,
	tx pgx.Tx,
	limit int,
) ([]sharedKafka.OutboxEvent, error) {
	rows, err := tx.Query(ctx, `
	SELECT id, event_type, payload
	FROM outbox
	WHERE sent = false
	ORDER BY created_at
	LIMIT $1
	FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending outbox events: %w", err)
	}

	defer rows.Close()

	events := make([]sharedKafka.OutboxEvent, 0, limit)

	for rows.Next() {
		var event sharedKafka.OutboxEvent

		err := rows.Scan(&event.ID, &event.EventType, &event.Payload)
		if err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}

		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return events, nil
}

// MarkOutboxEventSent реализует BetRepository.
func (b *betRepository) MarkOutboxEventSent(
	ctx context.Context,
	tx pgx.Tx,
	id int64,
) error {
	_, err := tx.Exec(ctx, `
	UPDATE outbox SET sent = true WHERE id = $1
	`, id)

	if err != nil {
		return fmt.Errorf("mark outbox event sent: %w", err)
	}

	return nil
}
