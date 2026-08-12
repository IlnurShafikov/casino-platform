package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ourMiddleware "github.com/casino/shared/middleware"
	walletCache "github.com/casino/wallet-service/internal/cache"
	"github.com/casino/wallet-service/internal/config"
	"github.com/casino/wallet-service/internal/handler"
	walletKafka "github.com/casino/wallet-service/internal/kafka"
	"github.com/casino/wallet-service/internal/repository"
	"github.com/casino/wallet-service/internal/service"
	"github.com/go-chi/chi"
	"github.com/jackc/pgx/v5/pgxpool"
)

// consumerGroupID — ID consumer group wallet-сервиса в Kafka. Все запущенные
// инстансы wallet-service должны использовать одно и то же значение, чтобы
// делить партиции между собой, а не читать одни и те же события каждый
// сам по себе.
const consumerGroupID = "wallet-service"

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to ping database",
			"error", err.Error(),
		)

		os.Exit(1)
	}

	logger.Info("connected database")

	redisClient, err := walletCache.NewRedisClient(ctx, cfg.RedisURL)
	if err != nil {
		logger.Error("failed to connect to redis",
			"error", err.Error(),
		)
		os.Exit(1)
	}

	defer redisClient.Close()

	logger.Info("connected tp redis")

	producer, err := walletKafka.NewProducer(cfg.KafkaBrokers, logger)
	if err != nil {
		logger.Error("failed to connect to kafka producer", "error", err.Error())
		os.Exit(1)
	}

	defer producer.Close()

	walletCacheImpl := walletCache.NewRedisCache(redisClient)
	walletRepo := repository.NewWalletRepository(db)
	walletSvc := service.NewWalletService(walletRepo, walletCacheImpl, logger)

	consumer, err := walletKafka.NewConsumer(cfg.KafkaBrokers, consumerGroupID, walletSvc, logger)
	if err != nil {
		logger.Error("failed to connect to kafka consumer", "error", err.Error())
		os.Exit(1)
	}

	defer consumer.Close()

	poller := walletKafka.NewOutboxPoller(walletRepo, producer, logger)

	go poller.Start(ctx)
	go consumer.Start(ctx)

	walletHandler := handler.NewWalletHandler(walletSvc, logger)
	r := chi.NewRouter()

	r.Use(ourMiddleware.Recovery(logger))
	r.Use(ourMiddleware.RequestID)
	r.Use(ourMiddleware.Logger(logger))
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	})

	authHandler := handler.NewAuthHandler([]byte(cfg.JWTSecret))
	authHandler.RegisterRoutes(r)

	r.Group(func(r chi.Router) {
		r.Use(ourMiddleware.JWT(ourMiddleware.JWTConfig{
			SecretKey: []byte(cfg.JWTSecret),
		}))

		walletHandler.RegisterRoutes(r)
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
		logger.Info("wallet service started",
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

	// Останавливаем фоновые горутины (poller, consumer) до того, как
	// начнём закрывать их зависимости через defer.
	cancel()

	shutdownCtx, shutdowCancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)

	defer shutdowCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown",
			"error", err.Error(),
		)
	}

	logger.Info("server stopped")
}
