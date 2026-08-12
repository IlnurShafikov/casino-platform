// Package service содержит бизнес-логику bet-сервиса: приём ставок и
// обработку событий от wallet-сервиса, определяющих их дальнейшую судьбу.
package service

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/casino/bet-service/internal/kafka"
	"github.com/casino/bet-service/internal/repository"
	"github.com/casino/shared/events"
	"github.com/google/uuid"
)

// Допустимые значения GameType в PlaceBetRequest.
const (
	GameTypeSlot     = "slot"
	GameTypeRoulette = "roulette"
	GameTypePoker    = "poker"
)

type (
	// PlaceBetRequest — запрос на размещение ставки.
	// IdempotencyKey опционален: если пуст, сервис сгенерирует его сам,
	// поэтому повторная отправка запроса без ключа не защищена от дублей —
	// клиент должен передавать один и тот же ключ при ретраях.
	PlaceBetRequest struct {
		UserID         int64  `json:"user_id"`
		Amount         int64  `json:"amount"`
		GameType       string `json:"game_type"`
		IdempotencyKey string `json:"idempotency_key"`
	}

	// BetResponse — представление ставки, отдаваемое наружу через HTTP.
	BetResponse struct {
		ID        int64     `json:"id"`
		UserID    int64     `json:"user_id"`
		Amount    int64     `json:"amount"`
		GameType  string    `json:"game_type"`
		Status    string    `json:"status"`
		WinAmount int64     `json:"win_amount"`
		CreatedAt time.Time `json:"created_at"`
	}
)

// BetService описывает бизнес-операции над ставками: приём новой ставки
// и обработку асинхронных ответов wallet-сервиса о результате списания
// денег (см. shared/events).
type BetService interface {
	// PlaceBet создаёт ставку в статусе PENDING и публикует событие
	// bet.placed, чтобы wallet-сервис попытался списать деньги.
	PlaceBet(ctx context.Context, req PlaceBetRequest) (*BetResponse, error)
	// GetBet возвращает ставку по её ID.
	GetBet(ctx context.Context, betID int64) (*BetResponse, error)
	// HandleMoneyDebited обрабатывает подтверждение списания денег:
	// разыгрывает исход ставки и переводит её в WON/LOST.
	HandleMoneyDebited(ctx context.Context, event events.MoneyDebited) error
	// HandleMoneyDebitFailed обрабатывает отказ в списании денег
	// (например, недостаточно средств) и отменяет ставку.
	HandleMoneyDebitFailed(ctx context.Context, event events.MoneyDebitFailed) error
}

// betService — реализация BetService.
type betService struct {
	repo     repository.BetRepository
	producer kafka.Producer
	logger   *slog.Logger
}

// NewBetService создаёт BetService поверх переданных репозитория и
// Kafka-продюсера.
func NewBetService(
	repo repository.BetRepository,
	producer kafka.Producer,
	logger *slog.Logger,
) BetService {
	return &betService{
		repo:     repo,
		producer: producer,
		logger:   logger,
	}
}

// PlaceBet реализует BetService.
func (b *betService) PlaceBet(
	ctx context.Context,
	req PlaceBetRequest,
) (*BetResponse, error) {
	if req.Amount <= 0 {
		return nil, ErrAmountMustBePositive
	}

	if req.GameType == "" {
		return nil, ErrGameTypeIsRequired
	}

	if req.IdempotencyKey == "" {
		req.IdempotencyKey = uuid.NewString()
	}

	b.logger.InfoContext(ctx, "placing bet",
		"user_id", req.UserID,
		"amount", req.Amount,
		"game_type", req.GameType,
		"idempotency_key", req.IdempotencyKey,
	)

	// Проверяем идемпотентность до открытия транзакции: если ставка с
	// таким ключом уже есть, просто возвращаем её — незачем платить за
	// лишний BEGIN/COMMIT ради запроса, который ничего не изменит.
	existing, err := b.repo.GetByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("check idempotency: %w", err)
	}

	if existing != nil {
		b.logger.InfoContext(ctx, "duplicate bet request",
			"idempotency_key", req.IdempotencyKey,
			"bet_id", existing.ID,
		)

		return betToResponse(existing), nil
	}

	tx, err := b.repo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback(ctx)

	bet := &repository.Bet{
		UserID:         req.UserID,
		Amount:         req.Amount,
		GameType:       req.GameType,
		Status:         repository.BetStatusPending,
		IdempotencyKey: req.IdempotencyKey,
	}

	err = b.repo.Create(ctx, tx, bet)
	if err != nil {
		return nil, fmt.Errorf("create bet: %w", err)
	}

	event := events.BetPlaced{
		BetID:     fmt.Sprintf("%d", bet.ID),
		UserID:    req.UserID,
		Amount:    req.Amount,
		GameType:  req.GameType,
		CreatedAt: time.Now(),
	}

	// Событие пишем в outbox в той же транзакции, что и саму ставку —
	// это и есть transactional outbox: либо обе записи закоммитятся,
	// либо ни одна, поэтому wallet-сервис не может получить событие о
	// ставке, которой на самом деле нет в БД.
	err = b.repo.CreateOutboxEvent(ctx, tx, events.TopicBetPlaced, event)
	if err != nil {
		return nil, fmt.Errorf("create outbox event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	b.logger.InfoContext(ctx, "bet placed successfully",
		"bet_id", bet.ID,
		"user_id", req.UserID,
		"amount", req.Amount,
		"status", bet.Status,
	)

	return betToResponse(bet), nil
}

func (b *betService) GetBet(ctx context.Context, betID int64) (*BetResponse, error) {
	bet, err := b.repo.GetByID(ctx, betID)
	if err != nil {
		return nil, fmt.Errorf("get bet: %w", err)
	}

	return betToResponse(bet), nil
}

// betToResponse преобразует репозиторную модель ставки в BetResponse.
func betToResponse(bet *repository.Bet) *BetResponse {
	return &BetResponse{
		ID:        bet.ID,
		UserID:    bet.UserID,
		Amount:    bet.Amount,
		GameType:  bet.GameType,
		Status:    bet.Status,
		WinAmount: bet.WinAmount,
		CreatedAt: bet.CreatedAt,
	}
}

// HandleMoneyDebited реализует BetService.
func (b *betService) HandleMoneyDebited(
	ctx context.Context,
	event events.MoneyDebited,
) error {
	b.logger.InfoContext(ctx, "money debited, activating bet",
		"bet_id", event.BetID,
		"user_id", event.UserID,
		"amount", event.Amount,
	)

	// BetID в событии — строка (общий формат для всех событий в shared/events),
	// а в БД ставки хранятся с числовым ID, поэтому парсим обратно.
	var betID int64
	_, err := fmt.Sscanf(event.BetID, "%d", &betID)
	if err != nil {
		return fmt.Errorf("parse bet_id: %w", err)
	}

	bet, err := b.repo.GetByID(ctx, betID)
	if err != nil {
		return fmt.Errorf("get bet: %w", err)
	}

	if bet.Status != repository.BetStatusPending {
		b.logger.InfoContext(ctx, "bet already settled, skipping",
			"bet_id", betID,
			"status", bet.Status,
		)

		return nil
	}

	tx, err := b.repo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback(ctx)

	// Деньги списаны — теперь и только теперь разыгрываем исход. Раньше
	// (пока деньги не списаны) выигрыш начислять нельзя: иначе игрок
	// может выиграть за ставку, которая по факту не оплачена.
	won, winAmount := simulateGame(event.Amount)

	finalStatus := repository.BetStatusLost
	if won {
		finalStatus = repository.BetStatusWon
	}

	err = b.repo.UpdateStatus(ctx, tx, betID, finalStatus, winAmount)
	if err != nil {
		return fmt.Errorf("update bet status: %w", err)
	}

	settledEvent := events.BetSettled{
		BetID:     event.BetID,
		UserID:    event.UserID,
		Won:       won,
		WinAmount: winAmount,
		SettledAt: time.Now(),
	}

	err = b.repo.CreateOutboxEvent(
		ctx, tx,
		events.TopicBetSettled,
		settledEvent,
	)
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

// HandleMoneyDebitFailed реализует BetService.
func (b *betService) HandleMoneyDebitFailed(
	ctx context.Context,
	event events.MoneyDebitFailed,
) error {
	b.logger.InfoContext(ctx, "money debit failed, canceling bet",
		"bet_id", event.BetID,
		"reason", event.Reason,
	)

	var betID int64

	_, err := fmt.Sscanf(event.BetID, "%d", &betID)
	if err != nil {
		return fmt.Errorf("parse bet_id: %w", err)
	}

	bet, err := b.repo.GetByID(ctx, betID)
	if err != nil {
		return fmt.Errorf("get bet: %w", err)
	}

	if bet.Status != repository.BetStatusPending {
		b.logger.InfoContext(ctx, "bet already resolved, skipping",
			"bet_id", betID,
			"status", bet.Status,
		)

		return nil
	}

	tx, err := b.repo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback(ctx)

	err = b.repo.UpdateStatus(ctx, tx, betID, repository.BetStatusCancelled, 0)
	if err != nil {
		return fmt.Errorf("cancel bet: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	b.logger.InfoContext(ctx, "bet cancelled",
		"bet_id", betID,
	)

	return nil
}

// simulateGame — заглушка игровой логики: разыгрывает случайный исход
// по фиксированной таблице множителей. Из 9 исходов 5 — проигрыш (0),
// остальные — выигрыш с множителем от 1x до 4x ставки. Настоящую игровую
// механику (слоты, рулетка и т.д. со своим RTP) сюда предстоит подставить
// позже — сигнатура рассчитана на замену этой функции без изменения
// вызывающего кода.
func simulateGame(amount int64) (bool, int64) {
	multipliers := []int64{0, 1, 0, 2, 0, 3, 0, 4, 0}

	idx := rand.Int63n(int64(len(multipliers) - 1))
	winMulti := multipliers[idx]

	if winMulti == 0 {
		return false, 0
	}

	return true, amount * winMulti
}
