package repository

import "errors"

var (
	// ErrWalletNotFound возвращается, когда кошелёк с указанным user_id отсутствует в БД.
	ErrWalletNotFound = errors.New("wallet not found")
	// ErrInsufficientFunds возвращается при попытке списать больше, чем есть на балансе.
	ErrInsufficientFunds = errors.New("insufficient funds")
	// ErrDuplicateKey возвращается при конфликте уникального ограничения
	// (например, при гонке на optimistic locking по полю version).
	ErrDuplicateKey = errors.New("duplicate key")
	// ErrInvalidAmount возвращается, когда сумма операции не положительна.
	ErrInvalidAmount = errors.New("amount must be positive")
)
