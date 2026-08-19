package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/casino/auth-service/internal/config"
	"github.com/casino/auth-service/internal/handler"
	"github.com/casino/auth-service/internal/repository"
	"github.com/casino/auth-service/internal/service"
	sharedKafka "github.com/casino/shared/kafka"
	ourMiddleware "github.com/casino/shared/middleware"
	"github.com/go-chi/chi"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	slog.SetDefault(logger)

	logger.Info("starting auth service")

	cfg := config.Load()

	logger.Info("config loaded",
		"http_port", cfg.HTTPPort,
		"database_url", cfg.DatabaseURL,
	)

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err.Error())
		os.Exit(1)
	}

	defer db.Close()

	logger.Info("connected database")

	producer, err := sharedKafka.NewProducer(cfg.KafkaBrokers, logger)
	if err != nil {
		logger.Error("failed to connect to kafka producer", "error", err.Error())
		os.Exit(1)
	}

	defer producer.Close()

	userRepo := repository.NewUserRepository(db)
	authSvc := service.NewAuthService(userRepo, []byte(cfg.JWTSecret), logger)

	poller := sharedKafka.NewOutboxPoller(userRepo, producer, logger)

	go poller.Start(ctx)

	authHandler := handler.NewAuthHandler(authSvc, logger)

	r := chi.NewRouter()

	r.Use(ourMiddleware.Recovery(logger))
	r.Use(ourMiddleware.RequestID)
	r.Use(ourMiddleware.Logger(logger))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	})

	authHandler.RegisterRoutes(r)

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
		logger.Info("auth service started", "port", cfg.HTTPPort)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err.Error())
			os.Exit(1)
		}
	}()

	<-quit
	logger.Info("shutting down server...")

	cancel()

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
