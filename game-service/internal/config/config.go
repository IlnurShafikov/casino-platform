package config

import "os"

type Config struct {
	HTTPPort      string
	BetServiceURL string
	JWTSecret     string
}

func Load() *Config {
	return &Config{
		HTTPPort:      getEnv("HTTP_PORT", "8084"),
		BetServiceURL: getEnv("BET_SERVICE_URL", "http://localhost:8082"),
		JWTSecret:     getEnv("JWT_SECRET", "local-dev-secret-change-in-production"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}
