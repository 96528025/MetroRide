package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type discardLogs struct{}

func (discardLogs) Info(string, ...any)  {}
func (discardLogs) Error(string, ...any) {}

func TestConsumeReturnsWhenContextIsCancelled(t *testing.T) {
	// Nothing listens on port 1; with the context already cancelled the client
	// fails every call with context.Canceled before it dials.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer func() { _ = rdb.Close() }()
	svc := &notificationService{rdb: rdb}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		svc.consume(ctx, discardLogs{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consume kept looping after its context was cancelled")
	}
}
