package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/casino/auth-service/internal/repository"
	"github.com/casino/shared/events"
	"github.com/casino/shared/middleware"
	"golang.org/x/crypto/bcrypt"
)

const minPasswordLength = 8

type (
	RegisterRequest struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	LoginRequest struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	// AuthResponse — результат успешной регистрации или логина: данные
	// пользователя и сразу готовый JWT, чтобы не заставлять клиента делать
	// отдельный запрос за токеном после регистрации.
	AuthResponse struct {
		UserID int64  `json:"user_id"`
		Email  string `json:"email"`
		Token  string `json:"token"`
	}
)

type AuthService interface {
	Register(ctx context.Context, req RegisterRequest) (*AuthResponse, error)
	Login(ctx context.Context, req LoginRequest) (*AuthResponse, error)
}

type authService struct {
	repo      repository.UserRepository
	jwtSecret []byte
	logger    *slog.Logger
}

func NewAuthService(
	repo repository.UserRepository,
	jwtSecret []byte,
	logger *slog.Logger,
) AuthService {
	return &authService{
		repo:      repo,
		jwtSecret: jwtSecret,
		logger:    logger,
	}
}

func (a *authService) Register(ctx context.Context, req RegisterRequest) (*AuthResponse, error) {
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))

	if req.Email == "" || !strings.Contains(req.Email, "@") {
		return nil, ErrInvalidEmail
	}

	if len(req.Password) < minPasswordLength {
		return nil, ErrPasswordTooShort
	}

	a.logger.InfoContext(ctx, "registering user", "email", req.Email)

	existing, err := a.repo.GetByEmail(ctx, req.Email)
	if err != nil && !errors.Is(err, repository.ErrUserNotFound) {
		return nil, fmt.Errorf("check existing user: %w", err)
	}

	if existing != nil {
		return nil, repository.ErrEmailAlreadyExists
	}

	passwordHash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	tx, err := a.repo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback(ctx)

	user, err := a.repo.Create(ctx, tx, req.Email, string(passwordHash))
	if err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	event := events.UserRegistered{
		UserID:       user.ID,
		Email:        user.Email,
		RegisteredAt: user.CreatedAt,
	}

	err = a.repo.CreateOutboxEvent(ctx, tx, events.TopicUserRegistered, event)
	if err != nil {
		return nil, fmt.Errorf("create outbox event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	token, err := middleware.GenerateToken(user.ID, event.Email, a.jwtSecret)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	a.logger.InfoContext(ctx, "user registered", "user_id", user.ID, "email", user.Email)

	return &AuthResponse{
		UserID: user.ID,
		Email:  user.Email,
		Token:  token,
	}, nil
}

func (a *authService) Login(ctx context.Context, req LoginRequest) (*AuthResponse, error) {
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))

	user, err := a.repo.GetByEmail(ctx, req.Email)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return nil, ErrInvalidCredentials
		}

		return nil, fmt.Errorf("get user: %w", err)
	}

	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password))
	if err != nil {
		return nil, ErrInvalidCredentials
	}

	token, err := middleware.GenerateToken(user.ID, user.Email, a.jwtSecret)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	a.logger.InfoContext(ctx, "user logged in", "user_id", user.ID)

	return &AuthResponse{
		UserID: user.ID,
		Email:  user.Email,
		Token:  token,
	}, nil
}
