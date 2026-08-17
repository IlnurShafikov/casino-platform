package service_test

import (
	"context"
	"fmt"

	"github.com/casino/wallet-service/internal/repository"
	"github.com/jackc/pgx/v5"
)

// mockTx — тот же минимальный pgx.Tx-стаб, что и в bet-service: сервис
// вызывает на нём только Commit/Rollback, остальное унаследовано от
// встроенного nil-интерфейса и запаникует, если когда-нибудь понадобится.
type mockTx struct {
	pgx.Tx
}

func (mockTx) Commit(context.Context) error   { return nil }
func (mockTx) Rollback(context.Context) error { return nil }

// mockOutboxEvent фиксирует один вызов mockWalletRepository.CreateOutboxEvent.
type mockOutboxEvent struct {
	eventType string
	payload   any
}

// mockWalletRepository — репозиторий в памяти для юнит-тестов. Не
// потокобезопасен — каждый тест создаёт свой экземпляр.
type mockWalletRepository struct {
	wallets      map[int64]*repository.Wallet
	transactions map[string]*repository.Transaction // ключ — IdempotencyKey
	outboxEvents []mockOutboxEvent
	nextTxID     int64
}

func newMockWalletRepository() *mockWalletRepository {
	return &mockWalletRepository{
		wallets:      make(map[int64]*repository.Wallet),
		transactions: make(map[string]*repository.Transaction),
	}
}

// seedWallet кладёт кошелёк напрямую, минуя Create, чтобы тест мог
// задать стартовый баланс.
func (f *mockWalletRepository) seedWallet(w *repository.Wallet) {
	stored := *w
	f.wallets[w.UserID] = &stored
}

func (f *mockWalletRepository) Create(_ context.Context, userID int64) (*repository.Wallet, error) {
	if existing, ok := f.wallets[userID]; ok {
		cp := *existing
		return &cp, nil
	}

	w := &repository.Wallet{ID: int64(len(f.wallets)) + 1, UserID: userID}
	f.wallets[userID] = w

	cp := *w

	return &cp, nil
}

func (f *mockWalletRepository) GetByUserID(_ context.Context, userID int64) (*repository.Wallet, error) {
	w, ok := f.wallets[userID]
	if !ok {
		return nil, repository.ErrWalletNotFound
	}

	cp := *w

	return &cp, nil
}

func (f *mockWalletRepository) GetByUserIDForUpdate(
	ctx context.Context,
	_ pgx.Tx,
	userID int64,
) (*repository.Wallet, error) {
	return f.GetByUserID(ctx, userID)
}

func (f *mockWalletRepository) UpdateBalance(
	_ context.Context,
	_ pgx.Tx,
	wallet *repository.Wallet,
	newBalance int64,
) error {
	current, ok := f.wallets[wallet.UserID]
	if !ok || current.Version != wallet.Version {
		return fmt.Errorf("version conflict: %w", repository.ErrDuplicateKey)
	}

	current.Balance = newBalance
	current.Version++

	return nil
}

func (f *mockWalletRepository) CreateTransaction(_ context.Context, _ pgx.Tx, t *repository.Transaction) error {
	f.nextTxID++
	t.ID = f.nextTxID

	stored := *t
	f.transactions[t.IdempotencyKey] = &stored

	return nil
}

func (f *mockWalletRepository) GetTransactionByIdempotencyKey(
	_ context.Context,
	key string,
) (*repository.Transaction, error) {
	t, ok := f.transactions[key]
	if !ok {
		return nil, nil
	}

	cp := *t

	return &cp, nil
}

func (f *mockWalletRepository) BeginTx(context.Context) (pgx.Tx, error) {
	return mockTx{}, nil
}

func (f *mockWalletRepository) CreateOutboxEvent(
	_ context.Context,
	_ pgx.Tx,
	eventType string,
	payload any,
) error {
	f.outboxEvents = append(f.outboxEvents, mockOutboxEvent{eventType: eventType, payload: payload})

	return nil
}

func (f *mockWalletRepository) GetPendingOutboxEvents(
	context.Context,
	pgx.Tx,
	int,
) ([]repository.OutboxEvent, error) {
	return nil, nil
}

func (f *mockWalletRepository) MarkOutboxEventSent(context.Context, pgx.Tx, int64) error {
	return nil
}

// mockCache — WalletCache для тестов: всегда промах на чтении, ничего не
// хранит. Считает только вызовы InvalidateBalance, чтобы тесты могли
// убедиться, что кэш инвалидируется после изменения баланса.
type mockCache struct {
	invalidateCalls int
}

func (*mockCache) GetBalance(context.Context, int64) (int64, error) { return -1, nil }
func (*mockCache) SetBalance(context.Context, int64, int64) error   { return nil }

func (f *mockCache) InvalidateBalance(context.Context, int64) error {
	f.invalidateCalls++

	return nil
}

func (*mockCache) AcquireLock(context.Context, int64) (bool, error) { return true, nil }
func (*mockCache) ReleaseLock(context.Context, int64) error         { return nil }
