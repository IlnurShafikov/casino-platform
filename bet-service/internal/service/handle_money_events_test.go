package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/casino/bet-service/internal/repository"
	"github.com/casino/bet-service/internal/service"
	"github.com/casino/shared/events"
)

func TestBetService_HandleMoneyDebited_SkipsAlreadySettledBet(t *testing.T) {
	t.Parallel()

	repo := newMockBetRepository()
	repo.seedBet(&repository.Bet{
		ID:        1,
		UserID:    7,
		Amount:    500,
		GameType:  service.GameTypeSlot,
		Status:    repository.BetStatusWon,
		WinAmount: 1000,
	})

	svc := newTestBetService(repo)

	err := svc.HandleMoneyDebited(context.Background(), events.MoneyDebited{
		BetID:  "1",
		UserID: 7,
		Amount: 500,
	})
	if err != nil {
		t.Fatalf("HandleMoneyDebited() unexpected error: %v", err)
	}

	if repo.updateStatusCalls != 0 {
		t.Errorf("UpdateStatus called %d times, want 0 (redelivery of an already-settled bet must be a no-op)", repo.updateStatusCalls)
	}

	if len(repo.outboxEvents) != 0 {
		t.Errorf("outbox events = %d, want 0 (must not re-publish bet.settled)", len(repo.outboxEvents))
	}

	if repo.bets[1].Status != repository.BetStatusWon {
		t.Errorf("Status = %q, want unchanged %q", repo.bets[1].Status, repository.BetStatusWon)
	}
}

func TestBetService_HandleMoneyDebited_BetNotFound(t *testing.T) {
	t.Parallel()

	repo := newMockBetRepository()
	svc := newTestBetService(repo)

	err := svc.HandleMoneyDebited(context.Background(), events.MoneyDebited{
		BetID:  "999",
		UserID: 7,
		Amount: 500,
	})

	if !errors.Is(err, repository.ErrBetNotFound) {
		t.Fatalf("HandleMoneyDebited() error = %v, want %v", err, repository.ErrBetNotFound)
	}
}

// TestBetService_HandleMoneyDebited_SettlesPendingBet не проверяет
// конкретно выигрыш/проигрыш — simulateGame использует math/rand
// напрямую и не детерминирован снаружи. Проверяются только инварианты,
// которые обязаны выполняться при любом исходе: ставка ушла из PENDING,
// опубликовано ровно одно bet.settled, WinAmount согласован со статусом.
func TestBetService_HandleMoneyDebited_SettlesPendingBet(t *testing.T) {
	t.Parallel()

	repo := newMockBetRepository()
	repo.seedBet(&repository.Bet{
		ID:       1,
		UserID:   7,
		Amount:   500,
		GameType: service.GameTypeSlot,
		Status:   repository.BetStatusPending,
	})

	svc := newTestBetService(repo)

	err := svc.HandleMoneyDebited(context.Background(), events.MoneyDebited{
		BetID:  "1",
		UserID: 7,
		Amount: 500,
	})
	if err != nil {
		t.Fatalf("HandleMoneyDebited() unexpected error: %v", err)
	}

	bet := repo.bets[1]

	if bet.Status != repository.BetStatusWon && bet.Status != repository.BetStatusLost {
		t.Fatalf("Status = %q, want WON or LOST", bet.Status)
	}

	if bet.Status == repository.BetStatusWon && bet.WinAmount <= 0 {
		t.Errorf("WON bet has WinAmount = %d, want > 0", bet.WinAmount)
	}

	if bet.Status == repository.BetStatusLost && bet.WinAmount != 0 {
		t.Errorf("LOST bet has WinAmount = %d, want 0", bet.WinAmount)
	}

	if len(repo.outboxEvents) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(repo.outboxEvents))
	}

	if repo.outboxEvents[0].eventType != events.TopicBetSettled {
		t.Errorf("outbox event type = %q, want %q", repo.outboxEvents[0].eventType, events.TopicBetSettled)
	}
}

func TestBetService_HandleMoneyDebitFailed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		seed            *repository.Bet
		wantStatus      string
		wantUpdateCalls int
	}{
		{
			name: "cancels a pending bet",
			seed: &repository.Bet{
				ID: 1, UserID: 7, Amount: 500, GameType: service.GameTypeSlot,
				Status: repository.BetStatusPending,
			},
			wantStatus:      repository.BetStatusCancelled,
			wantUpdateCalls: 1,
		},
		{
			name: "skips an already resolved bet",
			seed: &repository.Bet{
				ID: 1, UserID: 7, Amount: 500, GameType: service.GameTypeSlot,
				Status: repository.BetStatusCancelled,
			},
			wantStatus:      repository.BetStatusCancelled,
			wantUpdateCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := newMockBetRepository()
			repo.seedBet(tt.seed)

			svc := newTestBetService(repo)

			err := svc.HandleMoneyDebitFailed(context.Background(), events.MoneyDebitFailed{
				BetID:  "1",
				UserID: 7,
				Amount: 500,
				Reason: "insufficient funds",
			})
			if err != nil {
				t.Fatalf("HandleMoneyDebitFailed() unexpected error: %v", err)
			}

			if repo.bets[1].Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", repo.bets[1].Status, tt.wantStatus)
			}

			if repo.updateStatusCalls != tt.wantUpdateCalls {
				t.Errorf("UpdateStatus called %d times, want %d", repo.updateStatusCalls, tt.wantUpdateCalls)
			}
		})
	}
}

func TestBetService_HandleMoneyDebitFailed_BetNotFound(t *testing.T) {
	t.Parallel()

	repo := newMockBetRepository()
	svc := newTestBetService(repo)

	err := svc.HandleMoneyDebitFailed(context.Background(), events.MoneyDebitFailed{
		BetID:  "999",
		UserID: 7,
		Amount: 500,
	})

	if !errors.Is(err, repository.ErrBetNotFound) {
		t.Fatalf("HandleMoneyDebitFailed() error = %v, want %v", err, repository.ErrBetNotFound)
	}
}
