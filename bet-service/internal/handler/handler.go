package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/casino/bet-service/internal/repository"
	"github.com/casino/bet-service/internal/service"
	"github.com/casino/shared/middleware"
	"github.com/go-chi/chi"
)

// maxRequestBodyBytes ограничивает размер тела запроса, чтобы клиент не мог
// прислать произвольно большой JSON и исчерпать память сервиса.
const maxRequestBodyBytes = 1 << 20 // 1 MiB

type BetHandler struct {
	service service.BetService
	logger  *slog.Logger
}

func NewBetHandler(
	service service.BetService,
	logger *slog.Logger,
) *BetHandler {
	return &BetHandler{
		service: service,
		logger:  logger,
	}
}

// RegisterRoutes регистрирует маршруты ставок в переданном роутере chi.
// Предполагается, что r уже обёрнут middleware.JWT — обработчики берут
// user_id из контекста запроса, а не из тела или пути, поэтому разместить
// ставку от чужого имени нельзя, даже зная чужой user_id.
func (h *BetHandler) RegisterRoutes(r chi.Router) {
	r.Post("/bets", h.PlaceBet)
	r.Get("/bets/{id}", h.GetBet)
}

// PlaceBet обрабатывает POST /bets — размещает ставку от имени
// аутентифицированного пользователя. Идемпотентность обеспечивается
// заголовком Idempotency-Key: повторный запрос с тем же ключом не создаёт
// вторую ставку.
func (h *BetHandler) PlaceBet(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	var body struct {
		Amount   int64  `json:"amount"`
		GameType string `json:"game_type"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")

		return
	}

	req := service.PlaceBetRequest{
		UserID:         middleware.GetUserID(r.Context()),
		Amount:         body.Amount,
		GameType:       body.GameType,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	}

	bet, err := h.service.PlaceBet(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, r, err, "failed to place bet", "user_id", req.UserID)

		return
	}

	writeJSON(w, http.StatusCreated, bet)
}

// GetBet обрабатывает GET /bets/{id} — возвращает ставку по её ID.
func (h *BetHandler) GetBet(w http.ResponseWriter, r *http.Request) {
	betID, err := parseBetID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	bet, err := h.service.GetBet(r.Context(), betID)
	if err != nil {
		h.handleServiceError(w, r, err, "failed to get bet", "bet_id", betID)

		return
	}

	writeJSON(w, http.StatusOK, bet)
}

// serviceErrorStatus сопоставляет sentinel-ошибки бизнес-логики с
// HTTP-статусами и сообщениями, безопасными для отдачи клиенту.
var serviceErrorStatus = []struct {
	err     error
	status  int
	message string
}{
	{service.ErrAmountMustBePositive, http.StatusBadRequest, "amount must be positive"},
	{service.ErrGameTypeIsRequired, http.StatusBadRequest, "game_type is required"},
	{repository.ErrBetNotFound, http.StatusNotFound, "bet not found"},
	{service.ErrUnknownGameType, http.StatusBadRequest, "unknown game type"},
}

// handleServiceError сопоставляет ошибку, вернувшуюся из BetService, с
// HTTP-ответом: известные sentinel-ошибки маппятся на конкретные статусы,
// всё остальное логируется как внутренняя ошибка и отдаётся клиенту как
// 500 без деталей реализации.
func (h *BetHandler) handleServiceError(
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

// parseBetID извлекает и валидирует ID ставки из пути запроса (параметр chi "id").
func parseBetID(r *http.Request) (int64, error) {
	idStr := chi.URLParam(r, "id")

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0, ErrInvalidBetID
	}

	if id <= 0 {
		return 0, ErrBetIDMustBePositive
	}

	return id, nil
}
