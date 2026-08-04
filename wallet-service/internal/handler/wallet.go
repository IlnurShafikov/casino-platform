package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/casino/wallet-service/internal/repository"
	"github.com/casino/wallet-service/internal/service"
	"github.com/go-chi/chi"
)

// maxRequestBodyBytes ограничивает размер тела запроса, чтобы клиент не мог
// прислать произвольно большой JSON и исчерпать память сервиса.
const maxRequestBodyBytes = 1 << 20 // 1 MiB

// WalletHandler обрабатывает HTTP-запросы к кошелькам игроков и транслирует
// их в вызовы service.WalletService, преобразуя ошибки сервиса в HTTP-статусы.
type WalletHandler struct {
	service service.WalletService
	logger  *slog.Logger
}

// NewWalletHandler создаёт WalletHandler поверх переданного WalletService.
func NewWalletHandler(
	service service.WalletService,
	logger *slog.Logger,
) *WalletHandler {
	return &WalletHandler{
		service: service,
		logger:  logger,
	}
}

// RegisterRoutes регистрирует маршруты кошелька в переданном роутере chi.
//
// TODO: маршруты не защищены аутентификацией/авторизацией — любой клиент
// может обратиться к кошельку произвольного userID. Перед выходом в прод
// нужен middleware, который проверяет, что вызывающий действительно
// является владельцем userID из пути (или обладает сервисным доступом).
func (h *WalletHandler) RegisterRoutes(r chi.Router) {
	r.Post("/wallets", h.CreateWallet)
	r.Get("/wallets/{userID}/balance", h.GetBalance)
	r.Post("/wallets/{userID}/deposit", h.Deposit)
	r.Post("/wallets/{userID}/debit", h.Debit)
}

// CreateWallet обрабатывает POST /wallets — создаёт новый кошелёк для игрока.
func (h *WalletHandler) CreateWallet(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	var req service.CreateWalletRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.UserID <= 0 {
		writeError(w, http.StatusBadRequest, "user_id must be positive")
		return
	}

	if err := h.service.CreateWallet(r.Context(), req); err != nil {
		h.handleServiceError(w, r, err, "failed to create wallet", "user_id", req.UserID)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"message": "wallet created",
	})
}

// GetBalance обрабатывает GET /wallets/{userID}/balance — возвращает текущий баланс кошелька.
func (h *WalletHandler) GetBalance(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	balance, err := h.service.GetBalance(r.Context(), userID)
	if err != nil {
		h.handleServiceError(w, r, err, "failed to get balance", "user_id", userID)
		return
	}

	writeJSON(w, http.StatusOK, balance)
}

// Deposit обрабатывает POST /wallets/{userID}/deposit — пополняет баланс кошелька.
// Идемпотентность обеспечивается заголовком Idempotency-Key: повторный запрос
// с тем же ключом не приводит к повторному начислению.
func (h *WalletHandler) Deposit(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	idempotencyKey := r.Header.Get("Idempotency-Key")

	var body struct {
		Amount int64 `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req := service.DepositRequest{
		UserID:         userID,
		Amount:         body.Amount,
		IdempotencyKey: idempotencyKey,
	}

	result, err := h.service.Deposit(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, r, err, "failed to deposit", "user_id", userID, "amount", body.Amount)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// Debit обрабатывает POST /wallets/{userID}/debit — списывает средства с баланса кошелька
// (например, при размещении ставки). Идемпотентность обеспечивается заголовком
// Idempotency-Key: повторный запрос с тем же ключом не приводит к повторному списанию.
func (h *WalletHandler) Debit(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	idempotencyKey := r.Header.Get("Idempotency-Key")

	var body struct {
		Amount        int64  `json:"amount"`
		ReferenceID   string `json:"reference_id"`
		ReferenceType string `json:"reference_type"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req := service.DebitRequest{
		UserID:         userID,
		Amount:         body.Amount,
		IdempotencyKey: idempotencyKey,
		ReferenceID:    body.ReferenceID,
		ReferenceType:  body.ReferenceType,
	}

	result, err := h.service.Debit(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, r, err, "failed to debit", "user_id", userID, "amount", body.Amount)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// serviceErrorStatus сопоставляет sentinel-ошибки бизнес-логики с HTTP-статусами
// и сообщениями, безопасными для отдачи клиенту.
var serviceErrorStatus = []struct {
	err     error
	status  int
	message string
}{
	{repository.ErrInvalidAmount, http.StatusBadRequest, "amount must be positive"},
	{repository.ErrWalletNotFound, http.StatusNotFound, "wallet not found"},
	{repository.ErrInsufficientFunds, http.StatusUnprocessableEntity, "insufficient funds"},
	{repository.ErrDuplicateKey, http.StatusConflict, "concurrent update, please retry"},
}

// handleServiceError сопоставляет ошибку, вернувшуюся из WalletService, с HTTP-ответом:
// известные sentinel-ошибки маппятся на конкретные статусы, всё остальное логируется
// как внутренняя ошибка и отдаётся клиенту как 500 без деталей реализации.
func (h *WalletHandler) handleServiceError(
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
		append(logFields, "error", err.Error())...,
	)

	writeError(w, http.StatusInternalServerError, logMessage)
}

// writeJSON сериализует data в JSON и пишет его в ответ с указанным HTTP-статусом.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeError пишет в ответ JSON вида {"error": message} с указанным HTTP-статусом.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}

// parseUserID извлекает и валидирует userID из пути запроса (параметр chi "userID").
func parseUserID(r *http.Request) (int64, error) {
	userIDStr := chi.URLParam(r, "userID")

	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		return 0, errors.New("invalid user ID")
	}

	if userID <= 0 {
		return 0, errors.New("user ID must be positive")
	}

	return userID, nil
}
