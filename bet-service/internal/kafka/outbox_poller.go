package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/casino/bet-service/internal/repository"
)

// OutboxPoller периодически вычитывает неотправленные события из
// transactional outbox (repository.BetRepository) и публикует их в
// Kafka через Producer.
type OutboxPoller struct {
	repo      repository.BetRepository
	producer  Producer
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

// NewOutboxPoller создаёт OutboxPoller с интервалом опроса 100мс и
// размером пачки 100 событий.
func NewOutboxPoller(
	repo repository.BetRepository,
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

// Start блокирующе опрашивает outbox с интервалом p.interval, пока ctx
// не будет отменён. Предназначен для запуска в отдельной горутине
// (go poller.Start(ctx)).
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

// process выбирает до p.batchSize неотправленных outbox-событий в
// рамках одной транзакции БД, публикует их в Kafka и помечает
// опубликованные события отправленными, после чего фиксирует транзакцию.
//
// Семантика доставки — at-least-once: если процесс упадёт после Publish,
// но до Commit, либо MarkOutboxEventSent завершится ошибкой (что в
// Postgres переводит всю транзакцию в aborted-состояние и откатывает
// весь батч при Commit), уже опубликованные события останутся
// неотмеченными и будут отправлены повторно при следующем опросе.
// Идемпотентность Kafka-продюсера защищает только от дублей на уровне
// одной сессии продюсера, поэтому потребители событий должны сами быть
// идемпотентны к повторной доставке.
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
		p.logger.Error("failed to get outbox events",
			"error", err.Error(),
		)

		return
	}

	if len(events) == 0 {
		return
	}

	p.logger.Info("processing outbox events",
		"count", len(events),
	)

	for _, event := range events {
		key := extractKey(event.Payload)

		// event.Payload уже содержит сериализованный в БД JSON; оборачиваем
		// в json.RawMessage, чтобы Publish пере-сериализовал его как есть —
		// без этого []byte замаршалился бы в base64-строку.
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
		p.logger.Error("failed to commit outbox tx",
			"error", err.Error(),
		)
	}
}

// extractKey извлекает user_id из JSON-пейлоада события и формирует по
// нему ключ Kafka-сообщения, чтобы события одного игрока попадали в
// одну партицию и сохраняли порядок относительно друг друга. Если
// payload не удалось разобрать (нет поля user_id или невалидный JSON),
// возвращает "unknown".
func extractKey(payload []byte) string {
	var data struct {
		UserID int64 `json:"user_id"`
	}

	if err := json.Unmarshal(payload, &data); err != nil {
		return "unknown"
	}

	return fmt.Sprintf("user:%d", data.UserID)
}
