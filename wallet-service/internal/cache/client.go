package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// pingTimeout ограничивает время ожидания ответа Redis на Ping при
// создании клиента, чтобы старт сервиса не завис навсегда при
// недоступном Redis.
const pingTimeout = 5 * time.Second

// NewRedisClient создаёт клиент Redis, подключённый к addr, и проверяет
// соединение через Ping. Если Redis недоступен, закрывает созданный
// клиент и возвращает ошибку.
func NewRedisClient(ctx context.Context, addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     10,
		MinIdleConns: 2,
	})

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()

		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return client, nil
}
