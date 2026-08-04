package events

import "time"

// BetPlaced — событие когда игрок сделал ставку
// Публикует Bet Service
// Читает Wallet Service
type BetPlaced struct {
	BetID     string    `json:"bet_id"`
	UserID    int64     `json:"user_id"`
	Amount    int64     `json:"amount"` // в центах
	GameType  string    `json:"game_type"`
	CreatedAt time.Time `json:"created_at"`
}

// BetSettled — событие когда ставка закрыта (выиграл/проиграл)
// Публикует Bet Service
// Читает Wallet Service
type BetSettled struct {
	BetID     string    `json:"bet_id"`
	UserID    int64     `json:"user_id"`
	Won       bool      `json:"won"`
	WinAmount int64     `json:"win_amount"` // в центах, 0 если проиграл
	SettledAt time.Time `json:"settled_at"`
}

// MoneyDebited — событие когда деньги списаны
// Публикует Wallet Service
// Читает Bet Service
type MoneyDebited struct {
	BetID     string    `json:"bet_id"`
	UserID    int64     `json:"user_id"`
	Amount    int64     `json:"amount"`
	DebitedAt time.Time `json:"debited_at"`
}

// MoneyDebitFailed — событие когда списание не удалось
// Публикует Wallet Service
// Читает Bet Service
type MoneyDebitFailed struct {
	BetID    string    `json:"bet_id"`
	UserID   int64     `json:"user_id"`
	Amount   int64     `json:"amount"`
	Reason   string    `json:"reason"`
	FailedAt time.Time `json:"failed_at"`
}

// Названия топиков в Kafka
const (
	TopicBetPlaced        = "bet.placed"
	TopicBetSettled       = "bet.settled"
	TopicMoneyDebited     = "money.debited"
	TopicMoneyDebitFailed = "money.debit.failed"
)
