package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/casino/shared/middleware"
	"github.com/go-chi/chi"
)

// AuthHandler выдаёт JWT-токены для последующего доступа к защищённым маршрутам.
type AuthHandler struct {
	jwtSecret []byte
}

// NewAuthHandler создаёт AuthHandler, подписывающий выданные токены jwtSecret.
func NewAuthHandler(jwtSecret []byte) *AuthHandler {
	return &AuthHandler{
		jwtSecret: jwtSecret,
	}
}

// RegisterRoutes регистрирует маршруты аутентификации в переданном роутере chi.
//
// TODO(security): эндпоинт не проверяет личность вызывающего — он подписывает
// токен для любого user_id/email, переданного в теле запроса, без пароля или
// иного доказательства владения аккаунтом. Это позволяет получить валидный
// доступ к кошельку произвольного игрока. Нужна реальная проверка учётных
// данных (например, пароль/OTP или проверка через сервис аутентификации)
// перед выдачей токена, либо эндпоинт должен быть доступен только доверенным
// внутренним вызывающим (сервис-to-сервис), но не публично.
func (h *AuthHandler) RegisterRoutes(r chi.Router) {
	r.Post("/auth/token", h.GenerateToken)
}

// GenerateToken обрабатывает POST /auth/token — выдаёт подписанный JWT для
// переданных user_id и email.
func (h *AuthHandler) GenerateToken(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	var body struct {
		UserID int64  `json:"user_id"`
		Email  string `json:"email"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if body.UserID <= 0 {
		writeError(w, http.StatusBadRequest, "user_id must be positive")
		return
	}

	body.Email = strings.TrimSpace(body.Email)
	if body.Email == "" {
		writeError(w, http.StatusBadRequest, "email is required")
		return
	}

	token, err := middleware.GenerateToken(body.UserID, body.Email, h.jwtSecret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate token")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"token": token,
	})
}
