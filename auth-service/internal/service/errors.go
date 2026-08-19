package service

import "errors"

var (
	ErrInvalidEmail     = errors.New("invalid email")
	ErrPasswordTooShort = errors.New("password must be at least 8 characters")
	// ErrInvalidCredentials — единая ошибка и для "email не найден", и для
	// "пароль не совпал". Так специально: если возвращать разные ошибки,
	// злоумышленник по ответу API сможет узнавать, какие email вообще
	// зарегистрированы в системе (email enumeration).
	ErrInvalidCredentials = errors.New("invalid email or password")
)
