package config

import (
	"reflect"
	"testing"
	"time"
)

func TestLoadUsesDefaults(t *testing.T) {
	clearConfigEnv(t, "routing-service")

	got := Load("routing-service", ":8083")
	want := Config{
		ServiceName:       "routing-service",
		HTTPAddr:          ":8083",
		PostgresDSN:       "postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable",
		RedisAddr:         "localhost:6379",
		RoutingServiceURL: "http://localhost:8083",
		ConsumerGroup:     "routing-service",
		ConsumerName:      "routing-service-1",
		ShutdownTimeout:   10 * time.Second,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() = %+v, want %+v", got, want)
	}
}

func TestLoadUsesEnvironmentOverrides(t *testing.T) {
	clearConfigEnv(t, "routing-service")
	t.Setenv("ROUTING_SERVICE_ADDR", "127.0.0.1:18083")
	t.Setenv("POSTGRES_DSN", "postgres://test:test@db:5432/test")
	t.Setenv("REDIS_ADDR", "redis.internal:6380")
	t.Setenv("ROUTING_SERVICE_URL", "http://routing.internal:9083")
	t.Setenv("CONSUMER_GROUP", "routing-workers")
	t.Setenv("CONSUMER_NAME", "routing-worker-7")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "27")

	got := Load("routing-service", ":8083")
	want := Config{
		ServiceName:       "routing-service",
		HTTPAddr:          "127.0.0.1:18083",
		PostgresDSN:       "postgres://test:test@db:5432/test",
		RedisAddr:         "redis.internal:6380",
		RoutingServiceURL: "http://routing.internal:9083",
		ConsumerGroup:     "routing-workers",
		ConsumerName:      "routing-worker-7",
		ShutdownTimeout:   27 * time.Second,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() = %+v, want %+v", got, want)
	}
}

func TestLoadFallsBackForInvalidShutdownTimeout(t *testing.T) {
	clearConfigEnv(t, "dispatch-service")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "not-a-number")

	got := Load("dispatch-service", ":8082")
	if got.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want %v", got.ShutdownTimeout, 10*time.Second)
	}
}

func TestServiceEnvNormalizesServiceName(t *testing.T) {
	tests := []struct {
		name    string
		service string
		suffix  string
		want    string
	}{
		{name: "hyphenated service", service: "rider-service", suffix: "ADDR", want: "RIDER_SERVICE_ADDR"},
		{name: "digits", service: "api-v2", suffix: "PORT", want: "API_V2_PORT"},
		{name: "mixed case and underscore", service: "Route_Worker", suffix: "URL", want: "ROUTE_WORKER_URL"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := serviceEnv(test.service, test.suffix); got != test.want {
				t.Errorf("serviceEnv(%q, %q) = %q, want %q", test.service, test.suffix, got, test.want)
			}
		})
	}
}

func clearConfigEnv(t *testing.T, serviceName string) {
	t.Helper()
	for _, key := range []string{
		serviceEnv(serviceName, "ADDR"),
		"POSTGRES_DSN",
		"REDIS_ADDR",
		"ROUTING_SERVICE_URL",
		"CONSUMER_GROUP",
		"CONSUMER_NAME",
		"SHUTDOWN_TIMEOUT_SECONDS",
	} {
		t.Setenv(key, "")
	}
}
