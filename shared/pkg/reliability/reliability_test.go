package reliability

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTimeoutHelpersApplyConfiguredDeadlineAndCancel(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		with    func(context.Context) (context.Context, context.CancelFunc)
	}{
		{name: "redis", timeout: RedisTimeout, with: WithRedisTimeout},
		{name: "postgres", timeout: PostgresTimeout, with: WithPostgresTimeout},
		{name: "readiness", timeout: ReadinessTimeout, with: WithReadinessTimeout},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := time.Now()
			ctx, cancel := test.with(context.Background())
			after := time.Now()

			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("context has no deadline")
			}
			if deadline.Before(before.Add(test.timeout)) || deadline.After(after.Add(test.timeout)) {
				t.Fatalf("deadline %s is outside the configured %s timeout window", deadline, test.timeout)
			}

			cancel()
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("context error = %v, want context.Canceled", ctx.Err())
			}
		})
	}
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	transient := errors.New("transient")
	calls := 0
	err := Retry(context.Background(), 4, 0, func(context.Context) error {
		calls++
		if calls < 3 {
			return transient
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestRetryReturnsWrappedLastErrorAfterAllAttempts(t *testing.T) {
	wantErr := errors.New("still unavailable")
	calls := 0
	err := Retry(context.Background(), 3, 0, func(context.Context) error {
		calls++
		return wantErr
	})

	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want wrapped sentinel", err)
	}
	if !strings.Contains(err.Error(), "operation failed after 3 attempts") {
		t.Fatalf("error = %q, want attempt count", err)
	}
}

func TestRetryNormalizesNonPositiveAttemptsToOne(t *testing.T) {
	wantErr := errors.New("failed once")
	calls := 0
	err := Retry(context.Background(), 0, time.Hour, func(context.Context) error {
		calls++
		return wantErr
	})

	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want wrapped sentinel", err)
	}
	if !strings.Contains(err.Error(), "operation failed after 1 attempts") {
		t.Fatalf("error = %q, want normalized attempt count", err)
	}
}

func TestRetryStopsPromptlyWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := Retry(ctx, 5, time.Hour, func(context.Context) error {
		calls++
		cancel()
		return errors.New("retryable failure")
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want cancellation before a second attempt", calls)
	}
}
