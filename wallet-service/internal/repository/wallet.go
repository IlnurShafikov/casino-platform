package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrWalletNotFound возвращается, когда кошелёк с указанным user_id отсутствует в БД.
	ErrWalletNotFound = errors.New("wallet not found")
	// ErrInsufficientFunds возвращается при попытке списать больше, чем есть на балансе.
	ErrInsufficientFunds = errors.New("insufficient funds")
	// ErrDuplicateKey возвращается при конфликте уникального ограничения
	// (например, при гонке на optimistic locking по полю version).
	ErrDuplicateKey = errors.New("duplicate key")
	// ErrInvalidAmount возвращается, когда сумма операции не положительна.
	ErrInvalidAmount = errors.New("amount must be positive")
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
