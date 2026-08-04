// Package middleware provides HTTP middleware for the wallet service,
// including request ID propagation, structured request logging, and
// panic recovery.
package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// contextKey is a private type used for context values set by this
// package, preventing collisions with keys defined in other packages.
type contextKey string

const (
	// KeyRequestID is the context key under which the current request ID
	// is stored by the RequestID middleware.
	KeyRequestID contextKey = "request_id"
	// KeyUserID is the context key under which the authenticated user ID
	// is expected to be stored (set by an auth middleware upstream).
	KeyUserID contextKey = "user_id"
)

// GetRequestID returns the request ID stored in ctx by the RequestID
// middleware. It returns an empty string if no request ID is present.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(KeyRequestID).(string); ok {
		return id
	}

	return ""
}

// GetUserID returns the authenticated user ID stored in ctx. It returns 0
// if no user ID is present.
func GetUserID(ctx context.Context) int64 {
	if userID, ok := ctx.Value(KeyUserID).(int64); ok {
		return userID
	}

	return 0
}

// RequestID returns middleware that ensures every request carries a
// request ID. It reuses the value of the incoming X-Request-ID header
// when present, otherwise generates a new UUID. The resulting ID is
// stored in the request context (retrievable via GetRequestID) and
// echoed back to the client via the X-Request-ID response header.
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

// responseWriter wraps http.ResponseWriter to capture the status code and
// response size written by downstream handlers, so that Logger can
// include them in its completion log entry.
type responseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

// WriteHeader records the status code before delegating to the
// underlying ResponseWriter.
func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// Write records the number of bytes written before delegating to the
// underlying ResponseWriter.
func (rw *responseWriter) Write(b []byte) (int, error) {
	size, err := rw.ResponseWriter.Write(b)
	rw.size += size

	return size, err
}

// Logger returns middleware that logs the start and completion of every
// request via logger, including method, path, status code, response
// size, and duration. It reads the request ID from context, so it
// should typically be chained after RequestID.
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

// Recovery returns middleware that recovers from panics raised by
// downstream handlers, logs the panic via logger, and writes a generic
// 500 JSON error response instead of letting the panic crash the
// server. It should typically be the outermost middleware in the chain
// so it can catch panics from other middleware as well.
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
