package service

import (
	"context"
	"fmt"
	"time"

	"github.com/casino/game-service/internal/betclient"
)

const (
	// pollInterval — как часто опрашивать bet-service, пока ставка не
	// разыграна. Реальное разыгрывание занимает доли секунды (см. логи
	// bet-service/wallet-service), так что 150мс — с запасом часто, но
	// не настолько, чтобы заваливать bet-service лишними запросами.
	pollInterval = 150 * time.Millisecond
	// pollTimeout — сколько всего ждать результат, прежде чем сдаться.
	// Ставка при этом никуда не денется — просто клиент не дождался
	// ответа синхронно и должен будет проверить её сам через bet-service.
	pollTimeout = 5 * time.Second
)

type PlayRequest struct {
	GameType string
	Amount   int64
}

// GameService оркестрирует один "раунд игры" поверх bet-service.
type GameService interface {
	// Play размещает ставку и ждёт её результат (WON/LOST/CANCELLED),
	// возвращая готовый ответ одним синхронным вызовом. authHeader —
	// значение заголовка Authorization, пробрасываемое от игрока как есть.
	Play(ctx context.Context, authHeader string, req PlayRequest) (*betclient.Bet, error)
}

type gameService struct {
	betClient betclient.Client
}

func NewGameService(betClient betclient.Client) GameService {
	return &gameService{
		betClient: betClient,
	}
}

func (s *gameService) Play(
	ctx context.Context,
	authHeader string,
	req PlayRequest,
) (*betclient.Bet, error) {
	bet, err := s.betClient.PlaceBet(ctx, authHeader, betclient.PlaceBetRequest{
		Amount:   req.Amount,
		GameType: req.GameType,
	})
	if err != nil {
		return nil, fmt.Errorf("place bet: %w", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for bet.Status == betclient.StatusPending {
		select {
		case <-pollCtx.Done():
			return nil, fmt.Errorf("bet %d: %w", bet.ID, ErrBetSettleTimeout)
		case <-ticker.C:
			bet, err = s.betClient.GetBet(ctx, authHeader, bet.ID)
			if err != nil {
				return nil, fmt.Errorf("get bet: %w", err)
			}
		}
	}

	return bet, nil
}
