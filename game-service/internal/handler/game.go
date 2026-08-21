package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/casino/game-service/internal/betclient"
	"github.com/casino/game-service/internal/service"
	"github.com/go-chi/chi"
)

// maxRequestBodyBytes ограничивает размер тела запроса, чтобы клиент не мог
// прислать произвольно большой JSON и исчерпать память сервиса.
const maxRequestBodyBytes = 1 << 20 // 1 MiB

type GameHandler struct {
	service service.GameService
	logger  *slog.Logger
}

func NewGameHandler(service service.GameService, logger *slog.Logger) *GameHandler {
	return &GameHandler{
		service: service,
		logger:  logger,
	}
}

func (h *GameHandler) RegisterRoutes(r chi.Router) {
	r.Post("/games/{gameType}/play", h.Play)
}

func (h *GameHandler) Play(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	gameType := chi.URLParam(r, "gameType")

	var body struct {
		Amount int64 `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")

		return
	}

	authHeader := r.Header.Get("Authorization")

	bet, err := h.service.Play(r.Context(), authHeader, service.PlayRequest{
		GameType: gameType,
		Amount:   body.Amount,
	})
	if err != nil {
		h.handleServiceError(w, r, err, gameType)

		return
	}

	writeJSON(w, http.StatusOK, bet)
}

// handleServiceError сопоставляет ошибку от GameService с HTTP-ответом:
// таймаут ожидания — 504, ошибка, пришедшая от bet-service — тот же
// статус и сообщение, что вернул сам bet-service, всё остальное
// (сеть недоступна и т.п.) — 502.
func (h *GameHandler) handleServiceError(
	w http.ResponseWriter,
	r *http.Request,
	err error,
	gameType string,
) {
	if errors.Is(err, service.ErrBetSettleTimeout) {
		writeError(w, http.StatusGatewayTimeout,
			"bet is taking longer than expected to settle, check its status separately")

		return
	}

	var upstreamErr *betclient.UpstreamError
	if errors.As(err, &upstreamErr) {
		writeError(w, upstreamErr.StatusCode, upstreamErr.Message)

		return
	}

	h.logger.ErrorContext(r.Context(), "failed to play",
		"game_type", gameType,
		"error", err.Error(),
	)

	writeError(w, http.StatusBadGateway, "failed to reach bet-service")
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
