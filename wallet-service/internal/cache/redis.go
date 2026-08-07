// Package cache содержит Redis-реализацию кэша баланса кошелька и
// распределённых блокировок поверх операций с балансом.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// WalletCache — кэш баланса кошелька и распределённая блокировка на
// уровне игрока поверх Redis. Используется, чтобы разгрузить БД на
// чтениях баланса и сериализовать конкурентные операции по одному
// userID между инстансами сервиса.
type WalletCache interface {
	// GetBalance возвращает закэшированный баланс игрока. Если баланс
	// отсутствует в кэше (промах или истёк TTL), возвращает -1, nil.
	GetBalance(ctx context.Context, userID int64) (int64, error)
	// SetBalance кладёт баланс игрока в кэш с TTL, заданным при создании кэша.
	SetBalance(ctx context.Context, userID, balance int64) error
	// InvalidateBalance удаляет закэшированный баланс игрока, например
	// после успешного пополнения/списания в БД.
	InvalidateBalance(ctx context.Context, userID int64) error
	// AcquireLock пытается взять распределённую блокировку по userID и
	// возвращает true, если блокировка была установлена вызывающим.
	//
	// TODO(concurrency): блокировка не использует fencing-токен — ReleaseLock
	// безусловно удаляет ключ, поэтому если операция держателя блокировки
	// выполняется дольше lockTTL, другой вызывающий может успеть взять
	// блокировку, а исходный держатель по завершении своей работы снимет
	// уже чужую блокировку. Для безопасного использования в проде нужно
	// возвращать из AcquireLock уникальный токен и снимать блокировку через
	// Lua-скрипт с проверкой владельца (compare-and-delete).
	AcquireLock(ctx context.Context, userID int64) (bool, error)
	// ReleaseLock снимает распределённую блокировку по userID.
	ReleaseLock(ctx context.Context, userID int64) error
}

// redisCache — реализация WalletCache поверх Redis.
type redisCache struct {
	client     *redis.Client
	balanceTTL time.Duration
	lockTTL    time.Duration
}

// NewRedisCache создаёт WalletCache поверх переданного Redis-клиента с
// TTL кэша баланса 30 секунд и TTL блокировки 5 секунд.
func NewRedisCache(client *redis.Client) WalletCache {
	return &redisCache{
		client:     client,
		balanceTTL: 30 * time.Second,
		lockTTL:    5 * time.Second,
	}
}

// balanceKey возвращает ключ Redis, под которым хранится закэшированный
// баланс игрока userID.
func balanceKey(userID int64) string {
	return fmt.Sprintf("balance:player:%d", userID)
}

// lockKey возвращает ключ Redis, под которым хранится распределённая
// блокировка игрока userID.
func lockKey(userID int64) string {
	return fmt.Sprintf("lock:player:%d", userID)
}

// cachedBalance — значение, сериализуемое в Redis для закэшированного баланса.
type cachedBalance struct {
	Balance  int64     `json:"balance"`
	CachedAt time.Time `json:"cached_at"`
}

// GetBalance реализует WalletCache.
func (c *redisCache) GetBalance(
	ctx context.Context,
	userID int64,
) (int64, error) {
	key := balanceKey(userID)
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return -1, nil
		}

		return -1, fmt.Errorf("redis get balance: %w", err)
	}

	var cached cachedBalance

	if err := json.Unmarshal([]byte(val), &cached); err != nil {
		return -1, fmt.Errorf("unmarshal balance: %w", err)
	}

	return cached.Balance, nil
}

// SetBalance реализует WalletCache.
func (c *redisCache) SetBalance(
	ctx context.Context,
	userID, balance int64,
) error {
	key := balanceKey(userID)

	data, err := json.Marshal(cachedBalance{
		Balance:  balance,
		CachedAt: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("marshal balance: %w", err)
	}

	err = c.client.Set(ctx, key, data, c.balanceTTL).Err()
	if err != nil {
		return fmt.Errorf("redis set balance: %w", err)
	}

	return nil
}

// InvalidateBalance реализует WalletCache.
func (c *redisCache) InvalidateBalance(
	ctx context.Context,
	userID int64,
) error {
	key := balanceKey(userID)

	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("redis invalidate balance: %w", err)
	}

	return nil
}

// AcquireLock реализует WalletCache.
func (c *redisCache) AcquireLock(
	ctx context.Context,
	userID int64,
) (bool, error) {
	key := lockKey(userID)

	acquired, err := c.client.SetNX(ctx, key, "locked", c.lockTTL).Result()
	if err != nil {
		return false, fmt.Errorf("redis acquire lock: %w", err)
	}

	return acquired, nil
}

// ReleaseLock реализует WalletCache.
func (c *redisCache) ReleaseLock(
	ctx context.Context,
	userID int64,
) error {
	key := lockKey(userID)

	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("redis release lock: %w", err)
	}

	return nil
}
