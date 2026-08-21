package service

import "errors"

// ErrBetSettleTimeout возвращается, если ставка не разыгралась за
// pollTimeout — например, если wallet-service или Kafka недоступны.
var ErrBetSettleTimeout = errors.New("timed out waiting for bet to settle")
