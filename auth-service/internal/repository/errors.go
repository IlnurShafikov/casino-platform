package repository

import "errors"

var (
	// ErrUserNotFound возвращается, когда пользователь с указанным email
	// отсутствует в БД.
	ErrUserNotFound = errors.New("user not found")
	// ErrEmailAlreadyExists возвращается при попытке зарегистрировать
	// пользователя с уже занятым email.
	ErrEmailAlreadyExists = errors.New("email already exists")
)
