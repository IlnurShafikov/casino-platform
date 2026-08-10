package service

import "errors"

var (
	ErrAmountMustBePositive = errors.New("amount must be positive")
	ErrGameTypeIsRequired   = errors.New("game_type is required")
)
