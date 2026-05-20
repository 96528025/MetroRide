package kafka

import (
	"os"
	"strings"
	"time"
)

const DriverLocationTopic = "metroride.driver.location.v1"

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
	Enabled bool
}

func LoadConfig(defaultGroupID string) Config {
	return Config{
		Brokers: splitCSV(getenv("KAFKA_BROKERS", "localhost:9092")),
		Topic:   getenv("KAFKA_DRIVER_LOCATION_TOPIC", DriverLocationTopic),
		GroupID: getenv("KAFKA_CONSUMER_GROUP", defaultGroupID),
		Enabled: strings.EqualFold(getenv("ENABLE_KAFKA_LOCATION_STREAM", "false"), "true"),
	}
}

func WriteTimeout() time.Duration {
	return 2 * time.Second
}

func ReadTimeout() time.Duration {
	return 2 * time.Second
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
