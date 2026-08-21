package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/casino/bet-service/internal/repository"
	"github.com/casino/bet-service/internal/service"
	"github.com/casino/shared/events"
)

func newTestBetService(repo *mockBetRepository, games map[string]service.Game) service.BetService {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	return service.NewBetService(repo, mockProducer{}, games, logger)
}

// testGames возвращает реестр игр для тестов, где сам исход Play()
// неважен (например, валидация PlaceBet никогда не доходит до Play) —
// нужно только чтобы GameTypeSlot был зарегистрирован.
func testGames() map[string]service.Game {
	return map[string]service.Game{
		service.GameTypeSlot: mockGame{won: true, winAmount: 1000},
	}
}

func TestBetService_PlaceBet_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		req     service.PlaceBetRequest
		wantErr error
	}{
		{
			name:    "negative amount",
			req:     service.PlaceBetRequest{UserID: 1, Amount: -100, GameType: service.GameTypeSlot},
			wantErr: service.ErrAmountMustBePositive,
		},
		{
			name:    "zero amount",
			req:     service.PlaceBetRequest{UserID: 1, Amount: 0, GameType: service.GameTypeSlot},
			wantErr: service.ErrAmountMustBePositive,
		},
		{
			name:    "missing game type",
			req:     service.PlaceBetRequest{UserID: 1, Amount: 100},
			wantErr: service.ErrGameTypeIsRequired,
		},
		{
			name:    "unknown game type",
			req:     service.PlaceBetRequest{UserID: 1, Amount: 100, GameType: "banana"},
			wantErr: service.ErrUnknownGameType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc := newTestBetService(newMockBetRepository(), testGames())

			_, err := svc.PlaceBet(context.Background(), tt.req)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("PlaceBet() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestBetService_PlaceBet_Success(t *testing.T) {
	t.Parallel()

	repo := newMockBetRepository()
	svc := newTestBetService(repo, testGames())

	req := service.PlaceBetRequest{
		UserID:   1,
		Amount:   500,
		GameType: service.GameTypeSlot,
	}

	got, err := svc.PlaceBet(context.Background(), req)
	if err != nil {
		t.Fatalf("PlaceBet() unexpected error: %v", err)
	}

	if got.Status != repository.BetStatusPending {
		t.Errorf("Status = %q, want %q", got.Status, repository.BetStatusPending)
	}

	if got.UserID != req.UserID || got.Amount != req.Amount || got.GameType != req.GameType {
		t.Errorf("PlaceBet() = %+v, want it to echo back %+v", got, req)
	}

	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero, want it populated")
	}

	if len(repo.outboxEvents) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(repo.outboxEvents))
	}

	if repo.outboxEvents[0].eventType != events.TopicBetPlaced {
		t.Errorf("outbox event type = %q, want %q", repo.outboxEvents[0].eventType, events.TopicBetPlaced)
	}
}

func TestBetService_PlaceBet_Idempotent(t *testing.T) {
	t.Parallel()

	repo := newMockBetRepository()
	repo.seedBet(&repository.Bet{
		ID:             42,
		UserID:         1,
		Amount:         500,
		GameType:       service.GameTypeSlot,
		Status:         repository.BetStatusWon,
		WinAmount:      1000,
		IdempotencyKey: "dup-key",
	})

	svc := newTestBetService(repo, testGames())

	got, err := svc.PlaceBet(context.Background(), service.PlaceBetRequest{
		UserID:         1,
		Amount:         500,
		GameType:       service.GameTypeSlot,
		IdempotencyKey: "dup-key",
	})
	if err != nil {
		t.Fatalf("PlaceBet() unexpected error: %v", err)
	}

	if got.ID != 42 {
		t.Errorf("ID = %d, want 42 (existing bet, not a new one)", got.ID)
	}

	if got.Status != repository.BetStatusWon {
		t.Errorf("Status = %q, want %q (must reflect the existing bet, not re-created as PENDING)", got.Status, repository.BetStatusWon)
	}

	if len(repo.outboxEvents) != 0 {
		t.Errorf("outbox events = %d, want 0 (duplicate request must not re-publish bet.placed)", len(repo.outboxEvents))
	}
}
