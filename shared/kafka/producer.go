// Package kafka содержит Kafka-продюсер bet-сервиса для публикации
// событий (например, из transactional outbox) с гарантией "ровно один
// раз на партицию" за счёт идемпотентного producer'а.
package kafka

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

// Producer публикует сообщения в Kafka.
type Producer interface {
	// Publish сериализует payload в JSON и синхронно публикует его в topic
	// с ключом key, дожидаясь подтверждения от брокера согласно
	// настройкам продюсера (acks=all).
	Publish(topic, key string, payload any) error
	// Close закрывает соединение с брокерами и освобождает ресурсы продюсера.
	Close() error
}

// producer — реализация Producer поверх sarama.SyncProducer.
type producer struct {
	client sarama.SyncProducer
	logger *slog.Logger
}

// NewProducer создаёт Producer, подключённый к brokers. Продюсер
// настроен как идемпотентный с acks=all — гарантирует, что событие не
// потеряется и не будет продублировано при повторной отправке из-за
// сетевого сбоя, что важно для финансовых событий (ставки, выплаты).
func NewProducer(brokers []string, logger *slog.Logger) (Producer, error) {
	config := sarama.NewConfig()

	// acks=all — ждём подтверждения от всех реплик
	// Самая надёжная настройка для финансовых событий
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	// Идемпотентный producer — защита от дублей
	// Даже если сеть моргнула и запрос повторился —
	// Kafka запишет событие только один раз
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	// Нужно для получения подтверждения
	config.Producer.Return.Successes = true

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	logger.Info("kafka producer connected",
		"brokers", brokers,
	)

	return &producer{
		client: client,
		logger: logger,
	}, nil
}

// Publish реализует Producer.
func (p *producer) Publish(topic, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	partition, offset, err := p.client.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("send kafka message: %w", err)
	}

	p.logger.Info("kafka message sent",
		"topic", topic,
		"key", key,
		"partition", partition,
		"offset", offset,
	)

	return nil
}

// Close реализует Producer.
func (p *producer) Close() error {
	return p.client.Close()
}
