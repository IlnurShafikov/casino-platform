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

// Wallet — кошелёк игрока с текущим балансом и версией для optimistic locking.
type Wallet struct {
	ID        int64
	UserID    int64
	Balance   int64
	Version   int64
	UpdatedAt time.Time
}

// Transaction — неизменяемая запись об одной операции с кошельком
// (пополнение, списание и т.д.), используется как история и для идемпотентности.
type Transaction struct {
	ID             int64
	UserID         int64
	Type           string
	Amount         int64
	BalanceBefore  int64
	BalanceAfter   int64
	ReferenceID    string
	ReferenceType  string
	IdempotencyKey string
}

// WalletRepository инкапсулирует доступ к хранилищу кошельков и транзакций.
type WalletRepository interface {
	// Create создаёт новый кошелёк с нулевым балансом для указанного userID.
	Create(ctx context.Context, userID int64) (*Wallet, error)
	// GetByUserID возвращает кошелёк по userID без блокировки.
	// Возвращает ErrWalletNotFound, если кошелёк не найден.
	GetByUserID(ctx context.Context, userID int64) (*Wallet, error)
	// GetByUserIDForUpdate возвращает кошелёк с блокировкой строки (SELECT ... FOR UPDATE)
	// в рамках переданной транзакции tx. Должна использоваться перед изменением баланса.
	GetByUserIDForUpdate(ctx context.Context, tx pgx.Tx, userID int64) (*Wallet, error)
	// UpdateBalance атомарно обновляет баланс кошелька с проверкой версии (optimistic locking).
	// Возвращает ErrDuplicateKey, если версия успела измениться (конфликт конкурентных обновлений).
	UpdateBalance(ctx context.Context, tx pgx.Tx, wallet *Wallet, newBalance int64) error
	// CreateTransaction сохраняет запись о выполненной операции в неизменяемый лог транзакций.
	CreateTransaction(ctx context.Context, tx pgx.Tx, t *Transaction) error
	// GetTransactionByIdempotencyKey ищет ранее выполненную транзакцию по idempotency-ключу.
	// Возвращает (nil, nil), если транзакция с таким ключом ещё не выполнялась.
	GetTransactionByIdempotencyKey(ctx context.Context, key string) (*Transaction, error)
	// BeginTx открывает новую транзакцию БД.
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

type walletRepository struct {
	db *pgxpool.Pool
}

// NewWalletRepository создаёт WalletRepository поверх пула соединений pgx.
func NewWalletRepository(db *pgxpool.Pool) WalletRepository {
	return &walletRepository{db: db}
}

// Create создаёт новый кошелёк с нулевым балансом для указанного userID.
func (r *walletRepository) Create(ctx context.Context, userID int64) (*Wallet, error) {
	wallet := &Wallet{}

	err := r.db.QueryRow(ctx, `
	INSERT INTO wallets (user_id, balance, version)
	VALUES ($1, 0, 0)
	ON CONFLICT (user_id) DO UPDATE SET user_id = wallets.user_id
	RETURNING id, user_id, balance, version
	`, userID).Scan(
		&wallet.ID,
		&wallet.UserID,
		&wallet.Balance,
		&wallet.Version,
	)
	if err != nil {
		return nil, fmt.Errorf("create wallet: %w", err)
	}

	return wallet, nil
}

// GetByUserID возвращает кошелёк по userID без блокировки.
// Возвращает ErrWalletNotFound, если кошелёк не найден.
func (r *walletRepository) GetByUserID(ctx context.Context, userID int64) (*Wallet, error) {
	wallet := &Wallet{}

	err := r.db.QueryRow(ctx, `
	SELECT id, user_id, balance, version, updated_at
	FROM wallets
	WHERE user_id = $1
	`, userID).Scan(
		&wallet.ID,
		&wallet.UserID,
		&wallet.Balance,
		&wallet.Version,
		&wallet.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}

		return nil, fmt.Errorf("get wallet by user id: %w", err)
	}

	return wallet, nil
}

// GetByUserIDForUpdate возвращает кошелёк с блокировкой строки (SELECT ... FOR UPDATE)
// в рамках переданной транзакции tx. Должна использоваться перед изменением баланса.
func (r *walletRepository) GetByUserIDForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	userID int64,
) (*Wallet, error) {
	wallet := &Wallet{}

	err := tx.QueryRow(ctx, `
	SELECT id, user_id, balance, version, updated_at
	FROM wallets
	WHERE user_id = $1
	FOR UPDATE
	`, userID).Scan(
		&wallet.ID,
		&wallet.UserID,
		&wallet.Balance,
		&wallet.Version,
		&wallet.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}

		return nil, fmt.Errorf("get wallet for update: %w", err)
	}

	return wallet, nil
}

// UpdateBalance атомарно обновляет баланс кошелька с проверкой версии (optimistic locking).
// Возвращает ErrDuplicateKey, если версия успела измениться (конфликт конкурентных обновлений).
func (r *walletRepository) UpdateBalance(
	ctx context.Context,
	tx pgx.Tx,
	wallet *Wallet,
	newBalance int64,
) error {
	result, err := tx.Exec(ctx, `
	UPDATE wallets
	SET balance = $1,
	version = version + 1,
	updated_at = NOW()
	WHERE user_id = $2
	AND version = $3
	`, newBalance, wallet.UserID, wallet.Version)

	if err != nil {
		return fmt.Errorf("update balance: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("version conflict: %w", ErrDuplicateKey)
	}

	return nil
}

// CreateTransaction сохраняет запись о выполненной операции в неизменяемый лог транзакций.
func (r *walletRepository) CreateTransaction(
	ctx context.Context,
	tx pgx.Tx,
	t *Transaction,
) error {
	// RETURNING id — PostgreSQL возвращает ID созданной записи
	// Scan записывает его обратно в структуру через указатель
	err := tx.QueryRow(ctx, `
		INSERT INTO transactions (
			user_id,
			type,
			amount,
			balance_before,
			balance_after,
			reference_id,
			reference_type,
			idempotency_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`,
		t.UserID,
		t.Type,
		t.Amount,
		t.BalanceBefore,
		t.BalanceAfter,
		t.ReferenceID,
		t.ReferenceType,
		t.IdempotencyKey,
	).Scan(&t.ID) // ← записываем ID обратно в структуру!

	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}
	return nil
}

// GetTransactionByIdempotencyKey ищет ранее выполненную транзакцию по idempotency-ключу.
// Возвращает (nil, nil), если транзакция с таким ключом ещё не выполнялась.
func (r *walletRepository) GetTransactionByIdempotencyKey(
	ctx context.Context,
	key string,
) (*Transaction, error) {
	t := &Transaction{}

	err := r.db.QueryRow(ctx, `
	SELECT id, user_id, type, amount,
	balance_before, balance_after,
	idempotency_key
	FROM transactions
	WHERE idempotency_key = $1
	`, key).Scan(
		&t.ID,
		&t.UserID,
		&t.Type,
		&t.Amount,
		&t.BalanceBefore,
		&t.BalanceAfter,
		&t.IdempotencyKey,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("get transactions: %w", err)
	}

	return t, nil
}

// BeginTx открывает новую транзакцию БД.
func (r *walletRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	return tx, nil
}

func (r *walletRepository) CreateOutboxEvent(
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

func (r *walletRepository) GetPendingOutboxEvents(
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

	return events, nil
}

func (r *walletRepository) MarkOutboxEventSent(
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
