package outbox

import (
	"testing"
	"time"
)

func TestRetryBackoff(t *testing.T) {
	tests := []struct {
		name             string
		previousAttempts int
		want             time.Duration
	}{
		{name: "first failure", previousAttempts: 0, want: 250 * time.Millisecond},
		{name: "second failure", previousAttempts: 1, want: 500 * time.Millisecond},
		{name: "seventh failure", previousAttempts: 6, want: 16 * time.Second},
		{name: "cap", previousAttempts: 7, want: 30 * time.Second},
		{name: "remains capped", previousAttempts: 100, want: 30 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryBackoff(tt.previousAttempts); got != tt.want {
				t.Fatalf("retryBackoff(%d) = %s, want %s", tt.previousAttempts, got, tt.want)
			}
		})
	}
}
