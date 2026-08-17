package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/casino/shared/events"
	"github.com/casino/wallet-service/internal/repository"
	"github.com/casino/wallet-service/internal/service"
)

func newTestWalletService(repo *mockWalletRepository, cache *mockCache) service.WalletService {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	return service.NewWalletService(repo, cache, logger)
}

func TestWalletService_Debit_Validation(t *testing.T) {
	t.Parallel()

	svc := newTestWalletService(newMockWalletRepository(), &mockCache{})

	_, err := svc.Debit(context.Background(), service.DebitRequest{UserID: 1, Amount: 0})

	if !errors.Is(err, repository.ErrInvalidAmount) {
		t.Fatalf("Debit() error = %v, want %v", err, repository.ErrInvalidAmount)
	}
}

func TestWalletService_Debit_InsufficientFunds(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 100})

	svc := newTestWalletService(repo, &mockCache{})

	_, err := svc.Debit(context.Background(), service.DebitRequest{UserID: 1, Amount: 500})

	if !errors.Is(err, repository.ErrInsufficientFunds) {
		t.Fatalf("Debit() error = %v, want %v", err, repository.ErrInsufficientFunds)
	}

	if repo.wallets[1].Balance != 100 {
		t.Errorf("Balance = %d, want unchanged 100", repo.wallets[1].Balance)
	}
}

func TestWalletService_Debit_Idempotent(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 1000})
	repo.transactions["dup-key"] = &repository.Transaction{
		ID: 1, UserID: 1, Type: service.TransactionTypeDebit,
		Amount: 300, BalanceBefore: 1000, BalanceAfter: 700,
		IdempotencyKey: "dup-key",
	}

	svc := newTestWalletService(repo, &mockCache{})

	got, err := svc.Debit(context.Background(), service.DebitRequest{
		UserID: 1, Amount: 300, IdempotencyKey: "dup-key",
	})
	if err != nil {
		t.Fatalf("Debit() unexpected error: %v", err)
	}

	if got.BalanceAfter != 700 {
		t.Errorf("BalanceAfter = %d, want 700 (cached result)", got.BalanceAfter)
	}

	if repo.wallets[1].Balance != 1000 {
		t.Errorf("Balance = %d, want unchanged 1000 (must not debit twice)", repo.wallets[1].Balance)
	}
}

func TestWalletService_Debit_Success(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 1000})
	cache := &mockCache{}

	svc := newTestWalletService(repo, cache)

	got, err := svc.Debit(context.Background(), service.DebitRequest{UserID: 1, Amount: 300})
	if err != nil {
		t.Fatalf("Debit() unexpected error: %v", err)
	}

	if got.BalanceBefore != 1000 || got.BalanceAfter != 700 {
		t.Errorf("BalanceBefore/After = %d/%d, want 1000/700", got.BalanceBefore, got.BalanceAfter)
	}

	if repo.wallets[1].Balance != 700 {
		t.Errorf("stored balance = %d, want 700", repo.wallets[1].Balance)
	}

	if cache.invalidateCalls != 1 {
		t.Errorf("cache invalidated %d times, want 1", cache.invalidateCalls)
	}
}

func TestWalletService_HandleBetPlaced_Success(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 1000})

	svc := newTestWalletService(repo, &mockCache{})

	err := svc.HandleBetPlaced(context.Background(), events.BetPlaced{
		BetID: "5", UserID: 1, Amount: 300,
	})
	if err != nil {
		t.Fatalf("HandleBetPlaced() unexpected error: %v", err)
	}

	if repo.wallets[1].Balance != 700 {
		t.Errorf("Balance = %d, want 700", repo.wallets[1].Balance)
	}

	if len(repo.outboxEvents) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(repo.outboxEvents))
	}

	if repo.outboxEvents[0].eventType != events.TopicMoneyDebited {
		t.Errorf("outbox event type = %q, want %q", repo.outboxEvents[0].eventType, events.TopicMoneyDebited)
	}
}

func TestWalletService_HandleBetPlaced_InsufficientFunds(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 100})

	svc := newTestWalletService(repo, &mockCache{})

	err := svc.HandleBetPlaced(context.Background(), events.BetPlaced{
		BetID: "5", UserID: 1, Amount: 300,
	})
	if err != nil {
		t.Fatalf("HandleBetPlaced() unexpected error: %v (insufficient funds must not be a Go error, it's an expected outcome published as money.debit.failed)", err)
	}

	if repo.wallets[1].Balance != 100 {
		t.Errorf("Balance = %d, want unchanged 100", repo.wallets[1].Balance)
	}

	if len(repo.outboxEvents) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(repo.outboxEvents))
	}

	if repo.outboxEvents[0].eventType != events.TopicMoneyDebitFailed {
		t.Errorf("outbox event type = %q, want %q", repo.outboxEvents[0].eventType, events.TopicMoneyDebitFailed)
	}
}

func TestWalletService_HandleBetPlaced_Idempotent(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 1000})
	repo.transactions["bet-placed:5"] = &repository.Transaction{
		ID: 1, UserID: 1, Amount: 300, IdempotencyKey: "bet-placed:5",
	}

	svc := newTestWalletService(repo, &mockCache{})

	err := svc.HandleBetPlaced(context.Background(), events.BetPlaced{
		BetID: "5", UserID: 1, Amount: 300,
	})
	if err != nil {
		t.Fatalf("HandleBetPlaced() unexpected error: %v", err)
	}

	if repo.wallets[1].Balance != 1000 {
		t.Errorf("Balance = %d, want unchanged 1000 (redelivery must not debit again)", repo.wallets[1].Balance)
	}

	if len(repo.outboxEvents) != 0 {
		t.Errorf("outbox events = %d, want 0 (redelivery must not re-publish)", len(repo.outboxEvents))
	}
}

func TestWalletService_HandleBetSettled_CreditsWinnings(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 1000})

	svc := newTestWalletService(repo, &mockCache{})

	err := svc.HandleBetSettled(context.Background(), events.BetSettled{
		BetID: "5", UserID: 1, Won: true, WinAmount: 500,
	})
	if err != nil {
		t.Fatalf("HandleBetSettled() unexpected error: %v", err)
	}

	if repo.wallets[1].Balance != 1500 {
		t.Errorf("Balance = %d, want 1500", repo.wallets[1].Balance)
	}
}

func TestWalletService_HandleBetSettled_NoOpWhenLost(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 1000})

	svc := newTestWalletService(repo, &mockCache{})

	err := svc.HandleBetSettled(context.Background(), events.BetSettled{
		BetID: "5", UserID: 1, Won: false, WinAmount: 0,
	})
	if err != nil {
		t.Fatalf("HandleBetSettled() unexpected error: %v", err)
	}

	if repo.wallets[1].Balance != 1000 {
		t.Errorf("Balance = %d, want unchanged 1000", repo.wallets[1].Balance)
	}

	if _, ok := repo.transactions["bet-settled:5"]; ok {
		t.Error("a transaction was recorded for a lost bet, want none")
	}
}

func TestWalletService_HandleBetSettled_Idempotent(t *testing.T) {
	t.Parallel()

	repo := newMockWalletRepository()
	repo.seedWallet(&repository.Wallet{UserID: 1, Balance: 1000})

	svc := newTestWalletService(repo, &mockCache{})

	event := events.BetSettled{BetID: "9", UserID: 1, Won: true, WinAmount: 500}

	if err := svc.HandleBetSettled(context.Background(), event); err != nil {
		t.Fatalf("HandleBetSettled() first call: unexpected error: %v", err)
	}

	if err := svc.HandleBetSettled(context.Background(), event); err != nil {
		t.Fatalf("HandleBetSettled() second call: unexpected error: %v", err)
	}

	if repo.wallets[1].Balance != 1500 {
		t.Errorf("Balance = %d, want 1500 (redelivery must not credit twice)", repo.wallets[1].Balance)
	}
}
