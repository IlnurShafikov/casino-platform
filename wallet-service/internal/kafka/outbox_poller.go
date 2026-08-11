package kafka

import "github.com/casino/wallet-service/internal/repository"

type OutboxPoller struct {
	repo repository.WalletRepository
	producer Producer
}
