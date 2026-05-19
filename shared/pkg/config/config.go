package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName       string
	HTTPAddr          string
	PostgresDSN       string
	RedisAddr         string
	RoutingServiceURL string
	ConsumerGroup     string
	ConsumerName      string
	ShutdownTimeout   time.Duration
}

func Load(serviceName, defaultAddr string) Config {
	return Config{
		ServiceName:       serviceName,
		HTTPAddr:          getenv(serviceEnv(serviceName, "ADDR"), defaultAddr),
		PostgresDSN:       getenv("POSTGRES_DSN", "postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable"),
		RedisAddr:         getenv("REDIS_ADDR", "localhost:6379"),
		RoutingServiceURL: getenv("ROUTING_SERVICE_URL", "http://localhost:8083"),
		ConsumerGroup:     getenv("CONSUMER_GROUP", serviceName),
		ConsumerName:      getenv("CONSUMER_NAME", serviceName+"-1"),
		ShutdownTimeout:   time.Duration(getenvInt("SHUTDOWN_TIMEOUT_SECONDS", 10)) * time.Second,
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func serviceEnv(serviceName, suffix string) string {
	out := make([]byte, 0, len(serviceName)+len(suffix)+1)
	for i := 0; i < len(serviceName); i++ {
		ch := serviceName[i]
		if ch == '-' {
			ch = '_'
		}
		if ch >= 'a' && ch <= 'z' {
			ch -= 32
		}
		out = append(out, ch)
	}
	out = append(out, '_')
	out = append(out, suffix...)
	return string(out)
}
