package config

import "os"

type Config struct {
	HTTPPort     string
	DatabaseURL  string
	RedisURL     string
	KafkaBrokers []string
	JWTSecret    string
}

func Load() *Config {
	return &Config{
		HTTPPort:    getEnv("YTTP_PORT", "8081"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://casino:casino123@localhost:5432/casino"),
		RedisURL:    getEnv("REDIS_URL", "localhost:6379"),
		KafkaBrokers: []string{
			getEnv("KAFKA_BROKERS", "localhost:9092"),
		},
		JWTSecret: getEnv("JWT_SECRET", "local-dev-secret-change-in-production"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}
