package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/casino/wallet-service/internal/config"
	"github.com/casino/wallet-service/internal/handler"
	"github.com/casino/wallet-service/internal/repository"
	"github.com/casino/wallet-service/internal/service"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	slog.SetDefault(logger)

	logger.Info("starting wallet service")

	cfg := config.Load()

	logger.Info("config loaded",
		"http_port", cfg.HTTPPort,
		"database_url", cfg.DatabaseURL,
	)

	ctx := context.Background()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to ping database",
			"error", err.Error(),
		)

		os.Exit(1)
	}

	logger.Info("connected database")

	walletRepo := repository.NewWalletRepository(db)
	walletSvc := service.NewWalletService(walletRepo, logger)
	walletHandler := handler.NewWalletHandler(walletSvc, logger)
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	})

	walletHandler.RegisterRoutes(r)

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
		logger.Info("wallet service startes",
			"port", cfg.HTTPPort,
		)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error",
				"error", err.Error(),
			)
			os.Exit(1)
		}
	}()

	<-quit
	logger.Info("shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)

	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown",
			"error", err.Error(),
		)
	}

	logger.Info("server stopped")

}
