package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/casino/auth-service/internal/repository"
	"github.com/casino/auth-service/internal/service"
	"github.com/go-chi/chi"
)

const maxRequestBodyBytes = 1 << 20

type AuthHandler struct {
	service service.AuthService
	logger  *slog.Logger
}

func NewAuthHandler(
	service service.AuthService,
	logger *slog.Logger,
) *AuthHandler {
	return &AuthHandler{
		service: service,
		logger:  logger,
	}
}

func (h *AuthHandler) RegisterRoutes(r chi.Router) {
	r.Post("/auth/register", h.Register)
	r.Post("/auth/login", h.Login)
}

func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	var req service.RegisterRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
	}

	resp, err := h.service.Register(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, r, err, "failed to register", "email", req.Email)
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	var req service.LoginRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid reuest body")
		return
	}

	resp, err := h.service.Login(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, r, err, "failed to login", "email", req.Email)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// serviceErrorStatus сопоставляет sentinel-ошибки бизнес-логики с
// HTTP-статусами и сообщениями, безопасными для отдачи клиенту.
var serviceErrorStatus = []struct {
	err     error
	status  int
	message string
}{
	{service.ErrInvalidEmail, http.StatusBadRequest, "invalid email"},
	{service.ErrPasswordTooShort, http.StatusBadRequest, "password must be at least 8 characters"},
	{repository.ErrEmailAlreadyExists, http.StatusConflict, "email already exists"},
	{service.ErrInvalidCredentials, http.StatusUnauthorized, "invalid email or password"},
}

func (h *AuthHandler) handleServiceError(
	w http.ResponseWriter,
	r *http.Request,
	err error,
	logMessage string,
	logFields ...any,
) {
	for _, m := range serviceErrorStatus {
		if errors.Is(err, m.err) {
			writeError(w, m.status, m.message)
			return
		}
	}

	h.logger.ErrorContext(r.Context(), logMessage,
		append(logFields, "error", err.Error())...)

	writeError(w, http.StatusInternalServerError, logMessage)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}
