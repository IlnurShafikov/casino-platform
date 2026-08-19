package service_test

import (
	"context"
	"time"

	"github.com/casino/bet-service/internal/repository"
	sharedKafka "github.com/casino/shared/kafka"
	"github.com/jackc/pgx/v5"
)

type mockProducer struct{}

func (mockProducer) Publish(string, string, any) error { return nil }
func (mockProducer) Close() error                      { return nil }

type mockTx struct {
	pgx.Tx
}

func (mockTx) Commit(context.Context) error   { return nil }
func (mockTx) Rollback(context.Context) error { return nil }

type mockOutboxEvent struct {
	eventType string
	payload   any
}

type mockBetRepository struct {
	bets              map[int64]*repository.Bet
	byIdempotency     map[string]int64
	outboxEvents      []mockOutboxEvent
	nextID            int64
	updateStatusCalls int

	createErr error
}

func newMockBetRepository() *mockBetRepository {
	return &mockBetRepository{
		bets:          make(map[int64]*repository.Bet),
		byIdempotency: make(map[string]int64),
	}
}

func (m *mockBetRepository) seedBet(bet *repository.Bet) {
	stored := *bet
	m.bets[bet.ID] = &stored

	if bet.IdempotencyKey != "" {
		m.byIdempotency[bet.IdempotencyKey] = bet.ID
	}

	if bet.ID >= m.nextID {
		m.nextID = bet.ID
	}
}

func (m *mockBetRepository) Create(_ context.Context, _ pgx.Tx, bet *repository.Bet) error {
	if m.createErr != nil {
		return m.createErr
	}

	m.nextID++
	bet.ID = m.nextID
	bet.CreatedAt = time.Now()

	stored := *bet
	m.bets[bet.ID] = &stored
	m.byIdempotency[bet.IdempotencyKey] = bet.ID

	return nil
}

func (m *mockBetRepository) GetByID(_ context.Context, id int64) (*repository.Bet, error) {
	bet, ok := m.bets[id]
	if !ok {
		return nil, repository.ErrBetNotFound
	}

	return bet, nil
}

func (m *mockBetRepository) UpdateStatus(
	_ context.Context,
	_ pgx.Tx,
	id int64,
	status string,
	winAmount int64,
) error {
	m.updateStatusCalls++

	bet, ok := m.bets[id]
	if !ok {
		return repository.ErrBetNotFound
	}

	bet.Status = status
	bet.WinAmount = winAmount

	return nil
}

func (m *mockBetRepository) GetByIdempotencyKey(
	_ context.Context,
	key string,
) (*repository.Bet, error) {
	id, ok := m.byIdempotency[key]
	if !ok {
		return nil, nil
	}

	return m.bets[id], nil
}

func (m *mockBetRepository) CreateOutboxEvent(
	_ context.Context,
	_ pgx.Tx,
	eventType string,
	payload any,
) error {
	m.outboxEvents = append(m.outboxEvents, mockOutboxEvent{
		eventType: eventType, payload: payload})

	return nil
}

func (m *mockBetRepository) BeginTx(context.Context) (pgx.Tx, error) {
	return mockTx{}, nil
}

func (m *mockBetRepository) GetPendingOutboxEvents(
	context.Context,
	pgx.Tx,
	int,
) ([]sharedKafka.OutboxEvent, error) {
	return nil, nil
}

func (m *mockBetRepository) MarkOutboxEventSent(
	context.Context,
	pgx.Tx,
	int64,
) error {
	return nil
}
