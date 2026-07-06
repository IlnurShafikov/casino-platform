package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrWalletNotFound    = errors.New("wallet not found")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrDuplicateKey      = errors.New("duplicate key")
)

type Wallet struct {
	ID      int64
	UserID  int64
	Balance int64
	Version int64
}

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

type WalletRepository interface {
	// Создать кошелёк
	Create(ctx context.Context, userID int64) (*Wallet, error)
	// Получить кошелёк по userID
	GetByUserID(ctx context.Context, userID int64) (*Wallet, error)
	// Получить кошелёк с блокировкой FOR UPDATE
	GetByUserIDForUpdate(ctx context.Context, tx pgx.Tx, userID int64) (*Wallet, error)
	// Обновить баланс
	UpdateBalance(ctx context.Context, tx pgx.Tx, wallet *Wallet, newBalance int64) error
	// Создать транзакцию
	CreateTransaction(ctx context.Context, tx pgx.Tx, t *Transaction) error
	// Проверить idempotency key
	GetTransactionByIdempotencyKey(ctx context.Context, key string) (*Transaction, error)
	// Начать транзакцию БД
	BeginTx(ctx context.Context) (pgx.Tx, error)
}

type walletRepository struct {
	db *pgxpool.Pool
}

func NewWalletRepository(db *pgxpool.Pool) WalletRepository {
	return &walletRepository{db: db}
}

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

func (r *walletRepository) GetByUserID(ctx context.Context, userID int64) (*Wallet, error) {
	wallet := &Wallet{}

	err := r.db.QueryRow(ctx, `
	SELECT id, user_id, balance, version
	FROM wallets
	WHERE user_id = $1
	`, userID).Scan(
		&wallet.ID,
		&wallet.UserID,
		&wallet.Balance,
		&wallet.Version,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}

		return nil, fmt.Errorf("get wallet by user id: %w", err)
	}

	return wallet, nil
}

func (r *walletRepository) GetByUserIDForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	userID int64,
) (*Wallet, error) {
	wallet := &Wallet{}

	err := tx.QueryRow(ctx, `
	SELECT id, user_id, balance, version
	FROM wallets
	WHERE user_id =$1
	FOR UPDATE
	`, userID).Scan(
		&wallet.ID,
		&wallet.UserID,
		&wallet.Balance,
		&wallet.Version,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}

		return nil, fmt.Errorf("get wallet for update: %w", err)
	}

	return wallet, nil
}

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
	update_at = NOW()
	WHERE user_id = $2
	AND version = $3
	`, newBalance, wallet.UserID, wallet.Version)

	if err != nil {
		return fmt.Errorf("update balance: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("version conflit: %w", ErrDuplicateKey)
	}

	return nil
}

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

func (r *walletRepository) GetTransactionByIdempotencyKey(
	ctx context.Context,
	key string,
) (*Transaction, error) {
	t := &Transaction{}

	err := r.db.QueryRow(ctx, `
	SELECT id, user_id, type, amount,
	balance_before, balance_after,
	idempotency_key
	ROM transactions
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

func (r *walletRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	return tx, nil
}
