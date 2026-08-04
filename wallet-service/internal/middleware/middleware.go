// Package middleware содержит HTTP-мидлвари wallet-сервиса: проброс
// request ID, структурированное логирование запросов и восстановление
// после паник.
package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// contextKey — приватный тип для значений контекста, задаваемых этим
// пакетом, чтобы избежать коллизий с ключами из других пакетов.
type contextKey string

const (
	// KeyRequestID — ключ контекста, под которым мидлварь RequestID
	// сохраняет текущий request ID.
	KeyRequestID contextKey = "request_id"
	// KeyUserID — ключ контекста, под которым ожидается ID
	// аутентифицированного пользователя (устанавливается мидлварью JWT).
	KeyUserID contextKey = "user_id"
)

// GetRequestID возвращает request ID, сохранённый в ctx мидлварью
// RequestID. Если request ID отсутствует, возвращает пустую строку.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(KeyRequestID).(string); ok {
		return id
	}

	return ""
}

// GetUserID возвращает ID аутентифицированного пользователя, сохранённый
// в ctx. Если ID отсутствует, возвращает 0.
func GetUserID(ctx context.Context) int64 {
	if userID, ok := ctx.Value(KeyUserID).(int64); ok {
		return userID
	}

	return 0
}

// RequestID возвращает мидлварь, гарантирующую наличие request ID у
// каждого запроса. Переиспользует значение входящего заголовка
// X-Request-ID, если он есть, иначе генерирует новый UUID. Итоговый ID
// сохраняется в контексте запроса (доступен через GetRequestID) и
// возвращается клиенту в заголовке ответа X-Request-ID.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}

		ctx := context.WithValue(r.Context(), KeyRequestID, requestID)

		w.Header().Set("X-Request-ID", requestID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// responseWriter оборачивает http.ResponseWriter, чтобы перехватить код
// статуса и размер ответа, записанные последующими хендлерами — это
// нужно Logger, чтобы включить их в итоговую запись лога.
type responseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

// WriteHeader запоминает код статуса перед делегированием вызова
// вложенному ResponseWriter.
func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// Write запоминает количество записанных байт перед делегированием
// вызова вложенному ResponseWriter.
func (rw *responseWriter) Write(b []byte) (int, error) {
	size, err := rw.ResponseWriter.Write(b)
	rw.size += size

	return size, err
}

// Logger возвращает мидлварь, логирующую через logger начало и завершение
// каждого запроса: метод, путь, код статуса, размер ответа и длительность.
// Читает request ID из контекста, поэтому обычно должна подключаться
// после RequestID.
func Logger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			requestID := GetRequestID(r.Context())

			wrapped := &responseWriter{
				ResponseWriter: w,
				status:         http.StatusOK,
			}

			logger.InfoContext(r.Context(), "request started",
				"request_id", requestID,
				"method", r.Method,
				"path", r.URL.Path,
				"remote_addr", r.RemoteAddr,
			)

			next.ServeHTTP(wrapped, r)

			logger.InfoContext(r.Context(), "request completed",
				"request_id", requestID,
				"method", r.Method,
				"path", r.URL.Path,
				"status", wrapped.status,
				"size_bytes", wrapped.size,
				"duration", time.Since(start),
			)
		})
	}
}

// Recovery возвращает мидлварь, восстанавливающуюся после паник в
// последующих хендлерах, логирующую панику через logger и отдающую
// клиенту типовой JSON-ответ 500 вместо падения сервера. Обычно должна
// быть самой внешней мидлварью в цепочке, чтобы перехватывать паники и
// из других мидлварей тоже.
func Recovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					requestID := GetRequestID(r.Context())

					logger.ErrorContext(r.Context(), "panic recovered",
						"request_id", requestID,
						"error", err,
						"method", r.Method,
						"path", r.URL.Path,
					)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"error": "internal server error"}`))
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}
