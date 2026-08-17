// Package repository содержит слой доступа к данным auth-сервиса:
// пользователей и события паттерна transactional outbox для их
// надёжной публикации.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sharedKafka "github.com/casino/shared/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// User — учётная запись игрока, хранящаяся в таблице users.
type User struct {
	ID           int64
	Email        string
	PasswordHash string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// UserRepository описывает доступ к пользователям и outbox-событиям в БД.
type UserRepository interface {
	// Create создаёт пользователя с уже захешированным паролем.
	// Возвращает ErrEmailAlreadyExists, если email занят.
	Create(ctx context.Context, email, passwordHash string) (*User, error)
	// GetByEmail возвращает пользователя по email. Если пользователь не
	// найден, возвращает ErrUserNotFound.
	GetByEmail(ctx context.Context, email string) (*User, error)
	// BeginTx начинает новую транзакцию БД.
	BeginTx(ctx context.Context) (pgx.Tx, error)
	// CreateOutboxEvent записывает событие в outbox в рамках переданной транзакции.
	CreateOutboxEvent(ctx context.Context, tx pgx.Tx, eventType string, payload any) error
	// GetPendingOutboxEvents выбирает и блокирует до limit неотправленных
	// outbox-событий в рамках переданной транзакции (SELECT ... FOR UPDATE
	// SKIP LOCKED), чтобы конкурентные воркеры не забрали одни и те же события.
	GetPendingOutboxEvents(ctx context.Context, tx pgx.Tx, limit int) ([]sharedKafka.OutboxEvent, error)
	// MarkOutboxEventSent помечает outbox-событие как отправленное в рамках
	// переданной транзакции.
	MarkOutboxEventSent(ctx context.Context, tx pgx.Tx, id int64) error
}

// userRepository — реализация UserRepository поверх пула соединений pgx.
type userRepository struct {
	db *pgxpool.Pool
}

// NewUserRepository создаёт UserRepository поверх переданного пула соединений.
func NewUserRepository(db *pgxpool.Pool) UserRepository {
	return &userRepository{db: db}
}

// Create реализует UserRepository.
func (u *userRepository) Create(
	ctx context.Context,
	email, passwordHash string,
) (*User, error) {
	user := &User{}

	err := u.db.QueryRow(ctx, `
	INSERT INTO users (email, password_hash)
	VALUES ($1, $2)
	RETURNING id, email, password_hash, created_at
	`, email, passwordHash).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.CreatedAt,
	)
	if err != nil {
		// 23505 — код Postgres для unique_violation. Проверка на занятый
		// email уже сделана в сервисе через GetByEmail, но между этой
		// проверкой и INSERT нет ничего атомарного — это подстраховка на
		// случай гонки, когда два запроса на регистрацию с одним email
		// прилетают одновременно.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, ErrEmailAlreadyExists
		}

		return nil, fmt.Errorf("create user: %w", err)
	}

	return user, nil
}

// GetByEmail реализует UserRepository.
func (u *userRepository) GetByEmail(ctx context.Context, email string) (*User, error) {
	user := &User{}

	err := u.db.QueryRow(ctx, `
	SELECT id, email, password_hash, created_at, updated_at
	FROM users
	WHERE email = $1
	`, email).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.CreatedAt,
		&user.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}

		return nil, fmt.Errorf("get user by email: %w", err)
	}

	return user, nil
}

// BeginTx реализует UserRepository.
func (u *userRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := u.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	return tx, nil
}

// CreateOutboxEvent реализует UserRepository.
func (u *userRepository) CreateOutboxEvent(
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

// GetPendingOutboxEvents реализует UserRepository.
func (u *userRepository) GetPendingOutboxEvents(
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return events, nil
}

// MarkOutboxEventSent реализует UserRepository.
func (u *userRepository) MarkOutboxEventSent(
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
