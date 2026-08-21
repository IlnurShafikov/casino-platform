package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/casino/bet-service/internal/config"
	"github.com/casino/bet-service/internal/handler"
	betKafka "github.com/casino/bet-service/internal/kafka"
	"github.com/casino/bet-service/internal/repository"
	"github.com/casino/bet-service/internal/service"
	sharedKafka "github.com/casino/shared/kafka"
	ourMiddleware "github.com/casino/shared/middleware"
	"github.com/go-chi/chi"
	"github.com/jackc/pgx/v5/pgxpool"
)

const consumerGroupID = "bet-service"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	slog.SetDefault(logger)

	logger.Info("starting bet service")

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
		logger.Error("failed to connected to kafka producer", "error", err.Error())
		os.Exit(1)
	}

	defer producer.Close()

	betRepo := repository.NewBetRepository(db)

	games := map[string]service.Game{
		service.GameTypeSlot: service.SlotGame{},
	}

	betSvc := service.NewBetService(betRepo, producer, games, logger)

	consumer, err := betKafka.NewConsumer(cfg.KafkaBrokers, consumerGroupID, betSvc, logger)
	if err != nil {
		logger.Error("failed to connected to kafka consumer", "error", err.Error())
		os.Exit(1)
	}

	defer consumer.Close()

	poller := sharedKafka.NewOutboxPoller(betRepo, producer, logger)

	go poller.Start(ctx)
	go consumer.Start(ctx)

	betHandler := handler.NewBetHandler(betSvc, logger)
	r := chi.NewRouter()

	r.Use(ourMiddleware.Recovery(logger))
	r.Use(ourMiddleware.RequestID)
	r.Use(ourMiddleware.Logger(logger))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	})

	r.Group(func(r chi.Router) {
		r.Use(ourMiddleware.JWT(ourMiddleware.JWTConfig{
			SecretKey: []byte(cfg.JWTSecret),
		}))

		betHandler.RegisterRoutes(r)
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
		logger.Info("bet service started", "port", cfg.HTTPPort)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err.Error())
			os.Exit(1)
		}
	}()

	<-quit
	logger.Info("shutting down server...")

	// Останавливаем фоновые горутины (poller, consumer) до того, как
	// начнём закрывать их зависимости через defer.
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
