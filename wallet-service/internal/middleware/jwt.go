package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// JWTConfig содержит конфигурацию, необходимую для проверки JWT access-токенов.
type JWTConfig struct {
	// SecretKey — HMAC-секрет для проверки подписи токена.
	SecretKey []byte
}

// Claims — набор JWT-claims, ожидаемых в access-токене, выданном
// платформой: расширяет стандартные registered claims ID и email
// аутентифицированного пользователя.
type Claims struct {
	UserID int64  `json:"user_id"`
	Email  string `json:"email"`

	jwt.RegisteredClaims
}

// bearerPrefix — обязательный префикс значения заголовка Authorization
// согласно схеме аутентификации "Bearer" (RFC 6750).
const bearerPrefix = "Bearer "

// JWT возвращает мидлварь, аутентифицирующую запросы по bearer-токену из
// заголовка Authorization. Токен должен быть подписан HMAC-алгоритмом с
// использованием cfg.SecretKey. При успехе ID аутентифицированного
// пользователя сохраняется в контексте запроса под ключом KeyUserID
// (доступен через GetUserID). При неудаче отвечает 401 с JSON-ошибкой,
// не вызывая next.
func JWT(cfg JWTConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeJSONError(w, http.StatusUnauthorized, "authorization header required")
				return
			}

			tokenString, ok := strings.CutPrefix(authHeader, bearerPrefix)
			if !ok || tokenString == "" {
				writeJSONError(w, http.StatusUnauthorized, "invalid authorization header format")
				return
			}

			claims := &Claims{}

			_, err := jwt.ParseWithClaims(
				tokenString,
				claims,
				func(token *jwt.Token) (any, error) {
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, errors.New("unexpected signing method")
					}

					return cfg.SecretKey, nil
				},
			)
			if err != nil {
				writeJSONError(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}

			ctx := context.WithValue(r.Context(), KeyUserID, claims.UserID)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GenerateToken создаёт и подписывает JWT access-токен для указанных
// userID и email со сроком действия 24 часа, используя HMAC-алгоритм
// HS256 и secretKey.
func GenerateToken(userID int64, email string, secretKey []byte) (string, error) {
	claims := &Claims{
		UserID: userID,
		Email:  email,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Subject:   "player",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	tokenStr, err := token.SignedString(secretKey)
	if err != nil {
		return "", err
	}

	return tokenStr, nil
}

// writeJSONError пишет в ответ JSON вида {"error": message} с указанным
// HTTP-статусом и корректным заголовком Content-Type.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write([]byte(`{"error": "` + message + `"}`))
}
