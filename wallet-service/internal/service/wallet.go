package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/casino/shared/events"
	"github.com/casino/wallet-service/internal/cache"
	"github.com/casino/wallet-service/internal/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Типы транзакций, сохраняемые в repository.Transaction.Type.
const (
	TransactionTypeDeposit = "DEPOSIT"
	TransactionTypeDebit   = "DEBIT"
	TransactionTypeCredit  = "CREDIT"
)

type (
	// CreateWalletRequest — запрос на создание кошелька для игрока.
	CreateWalletRequest struct {
		UserID int64 `json:"user_id"`
	}

	// DepositRequest — запрос на пополнение баланса игрока.
	DepositRequest struct {
		UserID         int64  `json:"user_id"`
		Amount         int64  `json:"amount"`
		IdempotencyKey string `json:"idempotency_key"`
	}

	// DebitRequest — запрос на списание средств с баланса игрока
	// (например, при размещении ставки).
	DebitRequest struct {
		UserID         int64  `json:"user_id"`
		Amount         int64  `json:"amount"`
		IdempotencyKey string `json:"idempotency_key"`
		ReferenceID    string `json:"reference_id"`   // bet_id
		ReferenceType  string `json:"reference_type"` // BET
	}

	// BalanceResponse — текущий баланс кошелька игрока.
	BalanceResponse struct {
		UserID    int64     `json:"user_id"`
		Balance   int64     `json:"balance"`
		UpdatedAt time.Time `json:"updated_at"`
	}

	// TransactionResponse — результат выполненной операции с кошельком.
	TransactionResponse struct {
		ID            int64  `json:"id"`
		UserID        int64  `json:"user_id"`
		Type          string `json:"type"`
		Amount        int64  `json:"amount"`
		BalanceBefore int64  `json:"balance_before"`
		BalanceAfter  int64  `json:"balance_after"`
	}
)

// WalletService реализует бизнес-логику работы с кошельками игроков:
// создание, получение баланса, пополнение и списание с гарантией идемпотентности.
type WalletService interface {
	// CreateWallet создаёт новый кошелёк для игрока.
	CreateWallet(ctx context.Context, req CreateWalletRequest) error
	// GetBalance возвращает текущий баланс кошелька игрока.
	// Возвращает repository.ErrWalletNotFound, если кошелёк не найден.
	GetBalance(ctx context.Context, userID int64) (*BalanceResponse, error)
	// Deposit пополняет баланс игрока. Повторный вызов с тем же IdempotencyKey
	// не приводит к повторному начислению — возвращается результат первого вызова.
	Deposit(ctx context.Context, req DepositRequest) (*TransactionResponse, error)
	// Debit списывает средства с баланса игрока. Повторный вызов с тем же IdempotencyKey
	// не приводит к повторному списанию — возвращается результат первого вызова.
	// Возвращает repository.ErrInsufficientFunds, если средств недостаточно.
	Debit(ctx context.Context, req DebitRequest) (*TransactionResponse, error)
	// HandleBetPlaced обрабатывает событие bet.placed от bet-service: пытается
	// списать деньги под размещённую ставку и публикует результат
	// (money.debited или money.debit.failed) через transactional outbox.
	HandleBetPlaced(ctx context.Context, event events.BetPlaced) error
	// HandleBetSettled обрабатывает событие bet.settled от bet-service:
	// зачисляет выигрыш на баланс, если ставка выиграна.
	HandleBetSettled(ctx context.Context, event events.BetSettled) error
}

type walletService struct {
	repo   repository.WalletRepository
	cache  cache.WalletCache
	logger *slog.Logger
}

// NewWalletService создаёт WalletService поверх переданного репозитория.
func NewWalletService(
	repo repository.WalletRepository,
	cache cache.WalletCache,
	logger *slog.Logger,
) WalletService {
	return &walletService{
		repo:   repo,
		cache:  cache,
		logger: logger,
	}
}

// CreateWallet создаёт новый кошелёк для игрока.
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

// GetBalance возвращает текущий баланс кошелька игрока.
// Возвращает repository.ErrWalletNotFound, если кошелёк не найден.
func (w *walletService) GetBalance(
	ctx context.Context,
	userID int64,
) (*BalanceResponse, error) {
	w.logger.InfoContext(ctx, "get balance", "user_id", userID)

	cachedBalance, err := w.cache.GetBalance(ctx, userID)
	if err != nil {
		w.logger.WarnContext(ctx, "cache get failed, failling back to db",
			"user_id", userID,
			"error", err.Error(),
		)
	}

	if cachedBalance != -1 {
		w.logger.InfoContext(ctx, "balance from cached",
			"user_id", userID,
			"balance", cachedBalance,
		)

		return &BalanceResponse{
			UserID:  userID,
			Balance: cachedBalance,
		}, nil
	}

	wallet, err := w.repo.GetByUserID(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrWalletNotFound) {
			return nil, err
		}

		return nil, fmt.Errorf("get balance: %w", err)
	}

	if err := w.cache.SetBalance(ctx, userID, wallet.Balance); err != nil {
		w.logger.WarnContext(ctx, "cached set failed",
			"user_id", userID,
			"error", err.Error(),
		)
	}

	w.logger.InfoContext(ctx, "balance from db",
		"user_id", userID,
		"balance", wallet.Balance,
	)

	return &BalanceResponse{
		UserID:    userID,
		Balance:   wallet.Balance,
		UpdatedAt: wallet.UpdatedAt,
	}, nil
}

// Deposit пополняет баланс игрока в рамках БД-транзакции с блокировкой строки кошелька.
// Повторный вызов с тем же IdempotencyKey не приводит к повторному начислению —
// возвращается результат первого вызова.
func (w *walletService) Deposit(
	ctx context.Context,
	req DepositRequest,
) (*TransactionResponse, error) {
	// Валидация входных данных
	// Делаем ДО любых обращений к БД
	if req.Amount <= 0 {
		return nil, repository.ErrInvalidAmount
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

	if err := w.cache.InvalidateBalance(ctx, req.UserID); err != nil {
		w.logger.WarnContext(ctx, "cached invalidate failed",
			"user_id", req.UserID,
			"error", err.Error(),
		)
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

// debitWithinTx списывает amount с баланса req.UserID в рамках уже открытой
// транзакции tx: блокирует кошелёк (FOR UPDATE), проверяет баланс, обновляет
// его и записывает Transaction. Не открывает и не коммитит транзакцию, не
// проверяет идемпотентность — это ответственность вызывающего, у которого
// может быть своя семантика повторных вызовов (HTTP-ретрай по ключу vs
// at-least-once доставка из Kafka).
func (w *walletService) debitWithinTx(
	ctx context.Context,
	tx pgx.Tx,
	req DebitRequest,
) (*TransactionResponse, error) {
	wallet, err := w.repo.GetByUserIDForUpdate(ctx, tx, req.UserID)
	if err != nil {
		return nil, fmt.Errorf("get wallet: %w", err)
	}

	if wallet.Balance < req.Amount {
		w.logger.WarnContext(ctx, "insufficient funds",
			"user_id", req.UserID,
			"balance", wallet.Balance,
			"requested", req.Amount,
		)

		return nil, repository.ErrInsufficientFunds
	}

	balanceBefore := wallet.Balance
	balanceAfter := wallet.Balance - req.Amount

	err = w.repo.UpdateBalance(ctx, tx, wallet, balanceAfter)
	if err != nil {
		return nil, fmt.Errorf("update balance: %w", err)
	}

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

	return &TransactionResponse{
		ID:            t.ID,
		UserID:        req.UserID,
		Type:          TransactionTypeDebit,
		Amount:        req.Amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
	}, nil
}

// Debit списывает средства с баланса игрока в рамках БД-транзакции с блокировкой
// строки кошелька; блокировка берётся до проверки баланса, чтобы исключить гонку
// между чтением баланса и его обновлением. Повторный вызов с тем же IdempotencyKey
// не приводит к повторному списанию — возвращается результат первого вызова.
// Возвращает repository.ErrInsufficientFunds, если средств недостаточно.
func (w *walletService) Debit(
	ctx context.Context,
	req DebitRequest,
) (*TransactionResponse, error) {
	if req.Amount <= 0 {
		return nil, repository.ErrInvalidAmount
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

	resp, err := w.debitWithinTx(ctx, tx, req)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("tx commit: %w", err)
	}

	if err := w.cache.InvalidateBalance(ctx, req.UserID); err != nil {
		w.logger.WarnContext(ctx, "cached invalidate failed",
			"user_id", req.UserID,
			"error", err.Error(),
		)
	}

	w.logger.InfoContext(ctx, "средства списаны успешно",
		"user_id", req.UserID,
		"amount", req.Amount,
		"balance_before", resp.BalanceBefore,
		"balance_after", resp.BalanceAfter,
	)

	return resp, nil
}

func (w *walletService) HandleBetPlaced(ctx context.Context, event events.BetPlaced) error {
	w.logger.InfoContext(ctx, "bet placed event received",
		"bet_id", event.BetID,
		"user_id", event.UserID,
		"amount", event.Amount,
	)

	req := DebitRequest{
		UserID:         event.UserID,
		Amount:         event.Amount,
		IdempotencyKey: "bet-placed:" + event.BetID,
		ReferenceID:    event.BetID,
		ReferenceType:  "BET",
	}

	existing, err := w.repo.GetTransactionByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("check idempotency: %w", err)
	}

	if existing != nil {
		w.logger.InfoContext(ctx, "bet.placed already processed, skipping",
			"bet_id", event.BetID,
		)

		return nil
	}

	tx, err := w.repo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback(ctx)

	_, debitErr := w.debitWithinTx(ctx, tx, req)

	switch {
	case debitErr == nil:
		outboxEvent := events.MoneyDebited{
			BetID:     event.BetID,
			UserID:    event.UserID,
			Amount:    event.Amount,
			DebitedAt: time.Now(),
		}

		if err := w.repo.CreateOutboxEvent(ctx, tx,
			events.TopicMoneyDebited, outboxEvent); err != nil {
			return fmt.Errorf("create outbox event: %w", err)
		}
	case errors.Is(debitErr, repository.ErrInsufficientFunds):
		outboxEvent := events.MoneyDebitFailed{
			BetID:    event.BetID,
			UserID:   event.UserID,
			Amount:   event.Amount,
			Reason:   "insufficient funds",
			FailedAt: time.Now(),
		}

		if err := w.repo.CreateOutboxEvent(ctx, tx,
			events.TopicMoneyDebitFailed, outboxEvent); err != nil {
			return fmt.Errorf("create outbox event: %w", err)
		}
	default:
		return fmt.Errorf("debit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tx commit: %w", err)
	}

	if err := w.cache.InvalidateBalance(ctx, event.UserID); err != nil {
		w.logger.WarnContext(ctx, "cache invalidate failed",
			"user_id", event.UserID,
			"error", err.Error(),
		)
	}

	return nil

}

func (w *walletService) HandleBetSettled(
	ctx context.Context,
	event events.BetSettled,
) error {
	w.logger.InfoContext(ctx, "bet settled event received",
		"bet_id", event.BetID,
		"user_id", event.UserID,
		"won", event.Won,
		"win_amount", event.WinAmount,
	)

	if !event.Won || event.WinAmount <= 0 {
		return nil
	}

	_, err := w.Deposit(ctx, DepositRequest{
		UserID:         event.UserID,
		Amount:         event.WinAmount,
		IdempotencyKey: "bet-settled:" + event.BetID,
	})
	if err != nil {
		return fmt.Errorf("credit winnings: %w", err)
	}

	return nil
}
