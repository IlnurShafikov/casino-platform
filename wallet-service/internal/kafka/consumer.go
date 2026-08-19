package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/casino/shared/events"
)

// EventHandler описывает обработку событий, на которые подписан Consumer.
// Интерфейс объявлен здесь, а не в internal/service, чтобы не получить
// цикл импортов (service импортирует kafka ради Producer). Реализуется
// service.WalletService — Go сопоставляет их структурно, без явной ссылки.
type EventHandler interface {
	HandleBetPlaced(ctx context.Context, event events.BetPlaced) error
	HandleBetSettled(ctx context.Context, event events.BetSettled) error
	HandleUserRegistered(ctx context.Context, event events.UserRegistered) error
}

type Consumer struct {
	group   sarama.ConsumerGroup
	handler EventHandler
	logger  *slog.Logger
	topics  []string
}

func NewConsumer(
	brokers []string,
	groupID string,
	handler EventHandler,
	logger *slog.Logger,
) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	return &Consumer{
		group:   group,
		handler: handler,
		logger:  logger,
		topics: []string{
			events.TopicBetPlaced, events.TopicBetSettled, events.TopicUserRegistered},
	}, nil
}

func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("kafka consumer started", "topics", c.topics)

	for {
		if err := c.group.Consume(ctx, c.topics, c); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}

			c.logger.Error("consumer group error", "error", err.Error())
		}

		if ctx.Err() != nil {
			c.logger.Info("kafka consumer stopped")

			return
		}
	}
}

// Close закрывает consumer group и освобождает её ресурсы.
func (c *Consumer) Close() error {
	return c.group.Close()
}

// Setup реализует sarama.ConsumerGroupHandler — вызывается перед началом
// цикла обработки на каждой ребалансировке.
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup реализует sarama.ConsumerGroupHandler — вызывается после
// завершения цикла обработки на каждой ребалансировке.
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim реализует sarama.ConsumerGroupHandler — читает сообщения
// одной партиции и передаёт их в handler.
func (c *Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			c.handleMessage(session, msg)
		}
	}
}

// handleMessage разбирает и обрабатывает одно сообщение. Offset коммитится
// (session.MarkMessage) только после успешной обработки — если она
// завершилась ошибкой, сообщение не отмечается прочитанным и будет
// прочитано повторно (например, после перезапуска сервиса).
func (c *Consumer) handleMessage(
	session sarama.ConsumerGroupSession,
	msg *sarama.ConsumerMessage,
) {
	ctx := session.Context()

	var err error

	switch msg.Topic {
	case events.TopicBetPlaced:
		var event events.BetPlaced

		if err = json.Unmarshal(msg.Value, &event); err == nil {
			err = c.handler.HandleBetPlaced(ctx, event)
		}
	case events.TopicBetSettled:
		var event events.BetSettled

		if err = json.Unmarshal(msg.Value, &event); err == nil {
			err = c.handler.HandleBetSettled(ctx, event)
		}
	case events.TopicUserRegistered:
		var event events.UserRegistered

		if err = json.Unmarshal(msg.Value, &event); err == nil {
			err = c.handler.HandleUserRegistered(ctx, event)
		}
	default:
		c.logger.Warn("unexpected topic", "topic", msg.Topic)
		session.MarkMessage(msg, "")

		return
	}

	if err != nil {
		c.logger.Error("failed to process message",
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"error", err.Error(),
		)

		return
	}

	session.MarkMessage(msg, "")
}
