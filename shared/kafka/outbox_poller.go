package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// OutboxEvent — событие паттерна transactional outbox, ожидающее
// публикации. Общий тип для всех сервисов: репозиторий сервиса должен
// возвращать именно его из GetPendingOutboxEvents, а не свою локальную
// структуру с такими же полями.
type OutboxEvent struct {
	ID        int64
	EventType string
	Payload   []byte
}

// OutboxRepository — минимальный набор методов, нужных OutboxPoller для
// вычитывания и подтверждения outbox-событий. Реализуется репозиторием
// каждого сервиса (BetRepository, WalletRepository и т.д.).
type OutboxRepository interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	GetPendingOutboxEvents(ctx context.Context, tx pgx.Tx, limit int) ([]OutboxEvent, error)
	MarkOutboxEventSent(ctx context.Context, tx pgx.Tx, id int64) error
}

// OutboxPoller периодически вычитывает неотправленные события из
// transactional outbox и публикует их в Kafka через Producer.
type OutboxPoller struct {
	repo      OutboxRepository
	producer  Producer
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

// NewOutboxPoller создаёт OutboxPoller с интервалом опроса 100мс и
// размером пачки 100 событий.
func NewOutboxPoller(repo OutboxRepository, producer Producer, logger *slog.Logger) *OutboxPoller {
	return &OutboxPoller{
		repo:      repo,
		producer:  producer,
		logger:    logger,
		interval:  100 * time.Millisecond,
		batchSize: 100,
	}
}

// Start блокирующе опрашивает outbox с интервалом p.interval, пока ctx
// не будет отменён. Предназначен для запуска в отдельной горутине.
func (p *OutboxPoller) Start(ctx context.Context) {
	p.logger.Info("outbox poller started")

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("outbox poller stopped")
			return
		case <-ticker.C:
			p.process(ctx)
		}
	}
}

func (p *OutboxPoller) process(ctx context.Context) {
	tx, err := p.repo.BeginTx(ctx)
	if err != nil {
		p.logger.Error("failed to begin tx", "error", err.Error())
		return
	}

	defer tx.Rollback(ctx)

	events, err := p.repo.GetPendingOutboxEvents(ctx, tx, p.batchSize)
	if err != nil {
		p.logger.Error("failed to get outbox events", "error", err.Error())
		return
	}

	if len(events) == 0 {
		return
	}

	p.logger.Info("processing outbox events", "count", len(events))

	for _, event := range events {
		key := extractKey(event.Payload)

		err := p.producer.Publish(event.EventType, key, json.RawMessage(event.Payload))
		if err != nil {
			p.logger.Error("failed to publish outbox event",
				"event_id", event.ID,
				"event_type", event.EventType,
				"error", err.Error(),
			)

			continue
		}

		err = p.repo.MarkOutboxEventSent(ctx, tx, event.ID)
		if err != nil {
			p.logger.Error("failed to mark outbox event sent",
				"event_id", event.ID,
				"error", err.Error(),
			)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		p.logger.Error("failed to commit outbox tx", "error", err.Error())
	}
}

// extractKey извлекает user_id из JSON-пейлоада события и формирует по
// нему ключ Kafka-сообщения, чтобы события одного игрока попадали в одну
// партицию и сохраняли порядок относительно друг друга.
func extractKey(payload []byte) string {
	var data struct {
		UserID int64 `json:"user_id"`
	}

	if err := json.Unmarshal(payload, &data); err != nil {
		return "unknown"
	}

	return fmt.Sprintf("user:%d", data.UserID)
}
