package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/casino/wallet-service/internal/repository"
)

type OutboxPoller struct {
	repo      repository.WalletRepository
	producer  Producer
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

func NewOutboxPoller(
	repo repository.WalletRepository,
	producer Producer,
	logger *slog.Logger,
) *OutboxPoller {
	return &OutboxPoller{
		repo:      repo,
		producer:  producer,
		logger:    logger,
		interval:  100 * time.Millisecond,
		batchSize: 100,
	}
}

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
		p.logger.Error("failed to begin tx",
			"error", err.Error(),
		)

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

func extractKey(payload []byte) string {
	var data struct {
		UserID int64 `json:"user_id"`
	}

	if err := json.Unmarshal(payload, &data); err != nil {
		return "unknown"
	}

	return fmt.Sprintf("user:%d", data.UserID)
}
