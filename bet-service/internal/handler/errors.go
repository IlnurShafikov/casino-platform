package handler

import "errors"

var (
	ErrInvalidBetID        = errors.New("invalid bet ID")
	ErrBetIDMustBePositive = errors.New("bet ID must be positive")
)
