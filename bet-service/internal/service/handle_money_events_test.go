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

	svc := newTestBetService(repo, testGames())

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
	svc := newTestBetService(repo, testGames())

	err := svc.HandleMoneyDebited(context.Background(), events.MoneyDebited{
		BetID:  "999",
		UserID: 7,
		Amount: 500,
	})

	if !errors.Is(err, repository.ErrBetNotFound) {
		t.Fatalf("HandleMoneyDebited() error = %v, want %v", err, repository.ErrBetNotFound)
	}
}

func TestBetService_HandleMoneyDebited_SettlesPendingBet_Won(t *testing.T) {
	t.Parallel()

	repo := newMockBetRepository()
	repo.seedBet(&repository.Bet{
		ID:       1,
		UserID:   7,
		Amount:   500,
		GameType: service.GameTypeSlot,
		Status:   repository.BetStatusPending,
	})

	games := map[string]service.Game{
		service.GameTypeSlot: mockGame{won: true, winAmount: 2000},
	}

	svc := newTestBetService(repo, games)

	err := svc.HandleMoneyDebited(context.Background(), events.MoneyDebited{
		BetID:  "1",
		UserID: 7,
		Amount: 500,
	})
	if err != nil {
		t.Fatalf("HandleMoneyDebited() unexpected error: %v", err)
	}

	bet := repo.bets[1]

	if bet.Status != repository.BetStatusWon {
		t.Errorf("Status = %q, want %q", bet.Status, repository.BetStatusWon)
	}

	if bet.WinAmount != 2000 {
		t.Errorf("WinAmount = %d, want 2000", bet.WinAmount)
	}

	if len(repo.outboxEvents) != 1 || repo.outboxEvents[0].eventType != events.TopicBetSettled {
		t.Fatalf("outbox events = %+v, want one bet.settled event", repo.outboxEvents)
	}
}

func TestBetService_HandleMoneyDebited_SettlesPendingBet_Lost(t *testing.T) {
	t.Parallel()

	repo := newMockBetRepository()
	repo.seedBet(&repository.Bet{
		ID:       1,
		UserID:   7,
		Amount:   500,
		GameType: service.GameTypeSlot,
		Status:   repository.BetStatusPending,
	})

	games := map[string]service.Game{
		service.GameTypeSlot: mockGame{won: false, winAmount: 0},
	}

	svc := newTestBetService(repo, games)

	err := svc.HandleMoneyDebited(context.Background(), events.MoneyDebited{
		BetID:  "1",
		UserID: 7,
		Amount: 500,
	})
	if err != nil {
		t.Fatalf("HandleMoneyDebited() unexpected error: %v", err)
	}

	bet := repo.bets[1]

	if bet.Status != repository.BetStatusLost {
		t.Errorf("Status = %q, want %q", bet.Status, repository.BetStatusLost)
	}

	if bet.WinAmount != 0 {
		t.Errorf("WinAmount = %d, want 0", bet.WinAmount)
	}

	if len(repo.outboxEvents) != 1 || repo.outboxEvents[0].eventType != events.TopicBetSettled {
		t.Fatalf("outbox events = %+v, want one bet.settled event", repo.outboxEvents)
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

			svc := newTestBetService(repo, testGames())

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
	svc := newTestBetService(repo, testGames())

	err := svc.HandleMoneyDebitFailed(context.Background(), events.MoneyDebitFailed{
		BetID:  "999",
		UserID: 7,
		Amount: 500,
	})

	if !errors.Is(err, repository.ErrBetNotFound) {
		t.Fatalf("HandleMoneyDebitFailed() error = %v, want %v", err, repository.ErrBetNotFound)
	}
}
