package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/casino/wallet-service/internal/repository"
	"github.com/google/uuid"
)

const (
	TransactionTypeDeposit = "DEPOSIT"
	TransactionTypeDebit   = "DEBIT"
	TransactionTypeCedit   = "CREDIT"
)

type (
	// Создать кошелёк
	CreateWalletRequest struct {
		UserID int64 `json:"user_id"`
	}

	// Пополнить баланс
	DepositRequest struct {
		UserID         int64  `json:"user_id"`
		Amount         int64  `json:"amount"`
		IdempotencyKey string `json:"idempotency_key"`
	}

	// Списать деньги
	DebitRequest struct {
		UserID         int64  `json:"user_id"`
		Amount         int64  `json:"amount"`
		IdempotencyKey string `json:"idempotency_key"`
		ReferenceID    string `json:"reference_id"`   // bet_id
		ReferenceType  string `json:"reference_type"` // BET
	}

	// Ответ с балансом
	BalanceResponse struct {
		UserID    int64     `json:"user_id"`
		Balance   int64     `json:"balance"`
		UpdatedAt time.Time `json:"updated_at"`
	}

	// Ответ с транзакцией
	TransactionResponse struct {
		ID            int64  `json:"id"`
		UserID        int64  `json:"user_id"`
		Type          string `json:"type"`
		Amount        int64  `json:"amount"`
		BalanceBefore int64  `json:"balance_before"`
		BalanceAfter  int64  `json:"balance_after"`
	}
)

type WalletService interface {
	CreateWallet(ctx context.Context, req CreateWalletRequest) error
	GetBalance(ctx context.Context, userID int64) (*BalanceResponse, error)
	Deposit(ctx context.Context, req DepositRequest) (*TransactionResponse, error)
	Debit(ctx context.Context, req DebitRequest) (*TransactionResponse, error)
}

type walletService struct {
	repo   repository.WalletRepository
	logger *slog.Logger
}

func NewWalletService(
	repo repository.WalletRepository,
	logger *slog.Logger,
) WalletService {
	return &walletService{
		repo:   repo,
		logger: logger,
	}
}

func (w *walletService) CreateWallet(
	ctx context.Context,
	req CreateWalletRequest,
) error {
	w.logger.InfoContext(ctx, "create wallet", "user_id", req.UserID)

	_, err := w.repo.Create(ctx, req.UserID)
	if err != nil {
		w.logger.ErrorContext(ctx, "create wallet failed",
			"user_id", req.UserID,
			"error", err.Error())

		return fmt.Errorf("create wallet: %w", err)
	}

	w.logger.InfoContext(ctx, "wallet is created", "user_id", req.UserID)

	return nil
}

func (w *walletService) GetBalance(
	ctx context.Context,
	userID int64,
) (*BalanceResponse, error) {
	w.logger.InfoContext(ctx, "get balance", "user_id", userID)

	wallet, err := w.repo.GetByUserID(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrWalletNotFound) {
			return nil, err
		}

		return nil, fmt.Errorf("get balance: %w", err)
	}

	return &BalanceResponse{
		UserID:  userID,
		Balance: wallet.Balance,
	}, nil
}

// Deposit — пополняем баланс игрока
// Здесь уже сложнее — нужна транзакция БД и idempotency
func (w *walletService) Deposit(
	ctx context.Context,
	req DepositRequest,
) (*TransactionResponse, error) {
	// Валидация входных данных
	// Делаем ДО любых обращений к БД
	if req.Amount <= 0 {
		return nil, errors.New("the amount must be positive")
	}

	// Если клиент не передал idempotency key —
	// генерируем сами
	// uuid.New() — уникальный ID (например: "550e8400-e29b-41d4-a716-446655440000")
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = uuid.NewString()
	}

	w.logger.InfoContext(ctx, "add deposite",
		"user_id", req.UserID,
		"amount", req.Amount,
		"idempotency_key", req.IdempotencyKey,
	)

	// ШАГ 1: Проверяем идемпотентность
	// Искали ли мы уже эту операцию?
	// Если да — возвращаем тот же ответ без повторного выполнения
	existing, err := w.repo.GetTransactionByIdempotencyKey(
		ctx,
		req.IdempotencyKey,
	)

	if err != nil {
		return nil, fmt.Errorf("check idempotency: %w", err)
	}

	if existing != nil {
		// Уже обрабатывали этот запрос!
		// Возвращаем сохранённый результат
		w.logger.InfoContext(ctx, "repeat request, return the cached response",
			"idempotency_key", req.IdempotencyKey,
		)

		return &TransactionResponse{
			ID:            existing.ID,
			UserID:        existing.UserID,
			Type:          existing.Type,
			Amount:        existing.Amount,
			BalanceBefore: existing.BalanceBefore,
			BalanceAfter:  existing.BalanceAfter,
		}, nil
	}

	// ШАГ 2: Начинаем транзакцию БД
	// Всё что ниже выполняется атомарно:
	// либо всё успешно, либо всё откатывается
	tx, err := w.repo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("start transaction: %w", err)
	}

	// defer — выполнится при выходе из функции
	// Rollback безопасен даже после Commit
	// Если Commit уже был — Rollback ничего не сделает
	defer tx.Rollback(ctx)

	// ШАГ 3: Получаем кошелёк с блокировкой FOR UPDATE
	// Никто другой не сможет изменить этот кошелёк
	// пока мы не сделаем Commit
	wallet, err := w.repo.GetByUserIDForUpdate(ctx, tx, req.UserID)
	if err != nil {
		return nil, fmt.Errorf("get wallet: %w", err)
	}

	// ШАГ 4: Считаем новый баланс
	balanceBefore := wallet.Balance
	balanceAfter := wallet.Balance + req.Amount

	// ШАГ 5: Обновляем баланс в БД
	err = w.repo.UpdateBalance(ctx, tx, wallet, balanceAfter)
	if err != nil {
		return nil, fmt.Errorf("update balance: %w", err)
	}

	// ШАГ 6: Записываем транзакцию в лог
	// Это неизменяемая история всех операций
	// Регулятор может запросить в любой момент
	t := &repository.Transaction{
		UserID:         req.UserID,
		Type:           TransactionTypeDeposit,
		Amount:         req.Amount,
		BalanceBefore:  balanceBefore,
		BalanceAfter:   balanceAfter,
		IdempotencyKey: req.IdempotencyKey,
	}

	err = w.repo.CreateTransaction(ctx, tx, t)
	if err != nil {
		return nil, fmt.Errorf("create transaction: %w", err)
	}

	// ШАГ 7: Коммитим — применяем все изменения
	// После этого данные сохранены в БД
	// defer Rollback выше уже ничего не сделает
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	w.logger.InfoContext(ctx, "баланс пополнен",
		"user_id", req.UserID,
		"amount", req.Amount,
		"balance_before", balanceBefore,
		"balance_after", balanceAfter,
	)

	return &TransactionResponse{
		ID:            t.ID,
		UserID:        req.UserID,
		Type:          TransactionTypeDeposit,
		Amount:        req.Amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
	}, nil
}

// Debit — списываем деньги когда игрок делает ставку
// Самая критичная операция — нельзя списать больше чем есть
func (w *walletService) Debit(
	ctx context.Context,
	req DebitRequest,
) (*TransactionResponse, error) {
	if req.Amount <= 0 {
		return nil, errors.New("the amount must be positive")
	}

	if req.IdempotencyKey == "" {
		req.IdempotencyKey = uuid.NewString()
	}

	w.logger.InfoContext(ctx, "debit funds",
		"user_id", req.UserID,
		"amount", req.Amount,
		"reference_id", req.ReferenceID)

	existing, err := w.repo.GetTransactionByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("check idempotency: %w", err)
	}

	if existing != nil {
		w.logger.InfoContext(ctx, "repeated debit request",
			"idempotency_key", req.IdempotencyKey)

		return &TransactionResponse{
			ID:            existing.ID,
			UserID:        existing.UserID,
			Type:          existing.Type,
			Amount:        existing.Amount,
			BalanceBefore: existing.BalanceBefore,
			BalanceAfter:  existing.BalanceAfter,
		}, nil
	}

	tx, err := w.repo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("start transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	// Получаем кошелёк с блокировкой FOR UPDATE
	// ВАЖНО: блокируем ДО проверки баланса
	// Иначе другой запрос может изменить баланс
	// между нашей проверкой и нашим UPDATE
	wallet, err := w.repo.GetByUserIDForUpdate(ctx, tx, req.UserID)
	if err != nil {
		return nil, fmt.Errorf("get wallet: %w", err)
	}

	// Главная проверка — хватает ли денег?
	// Делаем ПОСЛЕ блокировки строки
	if wallet.Balance < req.Amount {
		w.logger.WarnContext(ctx, "insufficient funds",
			"user_id", req.UserID,
			"balance", wallet.Balance,
			"requested", req.Amount)

		// Возвращаем конкретную ошибку
		// Handler вернёт 422 клиенту
		return nil, repository.ErrInsufficientFunds
	}

	balanceBefore := wallet.Balance
	balanceAfter := wallet.Balance - req.Amount

	// Обновляем баланс
	err = w.repo.UpdateBalance(ctx, tx, wallet, balanceAfter)
	if err != nil {
		return nil, fmt.Errorf("update balance: %w", err)
	}

	// Записываем транзакцию
	// ReferenceID и ReferenceType — ссылка на ставку
	// Чтобы в истории было видно: "списано за ставку bet-123"
	t := &repository.Transaction{
		UserID:         req.UserID,
		Type:           TransactionTypeDebit,
		Amount:         req.Amount,
		BalanceBefore:  balanceBefore,
		BalanceAfter:   balanceAfter,
		ReferenceID:    req.ReferenceID,
		ReferenceType:  req.ReferenceType,
		IdempotencyKey: req.IdempotencyKey,
	}

	err = w.repo.CreateTransaction(ctx, tx, t)
	if err != nil {
		return nil, fmt.Errorf("create transaction: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("tx commit: %w", err)
	}

	w.logger.InfoContext(ctx, "средства списаны успешно",
		"user_id", req.UserID,
		"amount", req.Amount,
		"balance_before", balanceBefore,
		"balance_after", balanceAfter,
	)

	return &TransactionResponse{
		ID:            t.ID,
		UserID:        req.UserID,
		Type:          TransactionTypeDebit,
		Amount:        req.Amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
	}, nil
}
