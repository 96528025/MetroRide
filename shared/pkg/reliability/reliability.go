package reliability

import (
	"context"
	"fmt"
	"time"
)

const (
	RedisTimeout      = 2 * time.Second
	PostgresTimeout   = 2 * time.Second
	RoutingTimeout    = 2 * time.Second
	ReadinessTimeout  = 1500 * time.Millisecond
	MaxRetryAttempts  = 3
	InitialRetryDelay = 150 * time.Millisecond
)

func WithRedisTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, RedisTimeout)
}

func WithPostgresTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, PostgresTimeout)
}

func WithReadinessTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, ReadinessTimeout)
}

func Retry(ctx context.Context, attempts int, delay time.Duration, fn func(context.Context) error) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := fn(ctx); err != nil {
			lastErr = err
		} else {
			return nil
		}
		if attempt == attempts {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
	}
	return fmt.Errorf("operation failed after %d attempts: %w", attempts, lastErr)
}
