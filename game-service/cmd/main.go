package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/casino/game-service/internal/betclient"
	"github.com/casino/game-service/internal/config"
	"github.com/casino/game-service/internal/handler"
	"github.com/casino/game-service/internal/service"
	ourMiddleware "github.com/casino/shared/middleware"
	"github.com/go-chi/chi"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	slog.SetDefault(logger)

	logger.Info("starting game service")

	cfg := config.Load()

	logger.Info("config loaded",
		"http_port", cfg.HTTPPort,
		"bet_service_url", cfg.BetServiceURL,
	)

	betClient := betclient.NewClient(cfg.BetServiceURL)
	gameSvc := service.NewGameService(betClient)
	gameHandler := handler.NewGameHandler(gameSvc, logger)

	r := chi.NewRouter()

	r.Use(ourMiddleware.Recovery(logger))
	r.Use(ourMiddleware.RequestID)
	r.Use(ourMiddleware.Logger(logger))

	r.Get("/heath", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	})

	r.Group(func(r chi.Router) {
		r.Use(ourMiddleware.JWT(ourMiddleware.JWTConfig{
			SecretKey: []byte(cfg.JWTSecret),
		}))

		gameHandler.RegisterRoutes(r)
	})

	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  10 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("game server started", "port", cfg.HTTPPort)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err.Error())
			os.Exit(1)
		}
	}()

	<-quit
	logger.Info("shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)

	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown", "error", err.Error())
	}

	logger.Info("server stopped")
}
