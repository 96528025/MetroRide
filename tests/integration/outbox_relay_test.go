//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/outbox"
)

// TestOutboxRelayKeepsBatchProgressWhenOnePublishFails pins the relay's most
// important property: progress is monotonic. A batch that fails partway through
// has already handed the earlier events to Redis, so those rows must stay marked
// as published. If the relay discarded them, every poll would deliver them again
// and a permanently failing record — which sorts ahead of nothing and therefore
// never clears — would repeat the batch forever.
//
// The failure is real, not mocked: the third event targets a Redis key that
// already holds a string, so XADD fails with WRONGTYPE on every attempt while
// the first two succeed.
func TestOutboxRelayKeepsBatchProgressWhenOnePublishFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run := uuid.NewString()
	sourceService := "outbox-relay-test-" + run
	deliverableStream := "test.outbox.deliverable." + run
	poisonedStream := "test.outbox.poisoned." + run

	db := openDB(t, ctx)
	defer db.Close()
	rdb := openRedis(t)
	defer func() { _ = rdb.Close() }()

	if err := outbox.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("ensure outbox schema: %v", err)
	}

	// Registered after the Close deferrals so it runs before them; t.Cleanup would
	// run after the pool and client are already shut down.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := db.Exec(cleanupCtx, `delete from event_outbox where source_service = $1`, sourceService); err != nil {
			t.Logf("clean up outbox rows: %v", err)
		}
		if err := rdb.Del(cleanupCtx, deliverableStream, poisonedStream).Err(); err != nil {
			t.Logf("clean up test streams: %v", err)
		}
	}()

	// Make the third event permanently undeliverable.
	if err := rdb.Set(ctx, poisonedStream, "not-a-stream", 0).Err(); err != nil {
		t.Fatalf("poison target stream: %v", err)
	}

	first := enqueueTestEvent(t, ctx, db, sourceService, deliverableStream)
	second := enqueueTestEvent(t, ctx, db, sourceService, deliverableStream)
	poisoned := enqueueTestEvent(t, ctx, db, sourceService, poisonedStream)

	relayCtx, stopRelay := context.WithCancel(ctx)
	defer stopRelay()
	relay := outbox.NewRelay(sourceService, slog.New(slog.NewTextHandler(io.Discard, nil)), db, rdb)
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay.Run(relayCtx)
	}()

	// Two recorded attempts on the poisoned row prove at least two full relay
	// cycles ran, so a relay that replayed delivered events would already have
	// duplicated the first two by the time this returns.
	waitForPublishAttempts(t, ctx, db, poisoned, poisonedStream, 2)

	stopRelay()
	select {
	case <-relayDone:
	case <-time.After(10 * time.Second):
		t.Fatal("relay did not stop after its context was cancelled")
	}

	length, err := rdb.XLen(ctx, deliverableStream).Result()
	if err != nil {
		t.Fatalf("read deliverable stream length: %v", err)
	}
	if length != 2 {
		t.Fatalf("expected exactly 2 delivered events, got %d: the relay replayed events it had already published", length)
	}

	for _, id := range []string{first, second} {
		if !isPublished(t, ctx, db, id, deliverableStream) {
			t.Fatalf("event %s was delivered to Redis but is not marked published", id)
		}
	}
	if isPublished(t, ctx, db, poisoned, poisonedStream) {
		t.Fatalf("event %s was never delivered but is marked published", poisoned)
	}
}

func enqueueTestEvent(t *testing.T, ctx context.Context, db *pgxpool.Pool, source, stream string) string {
	t.Helper()

	envelope, err := events.NewEnvelope(uuid.NewString(), events.TypeRideRequested, source, uuid.NewString(), events.RideRequested{
		RideID:      uuid.NewString(),
		RiderID:     source,
		RequestedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("build test envelope: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin outbox transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := outbox.Enqueue(ctx, tx, stream, envelope); err != nil {
		t.Fatalf("enqueue outbox event: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit outbox event: %v", err)
	}

	// The relay orders by created_at, which Enqueue stamps from the application
	// clock. Separate the rows so the batch order under test is deterministic.
	time.Sleep(5 * time.Millisecond)
	return envelope.ID
}

func waitForPublishAttempts(t *testing.T, ctx context.Context, db *pgxpool.Pool, id, stream string, want int) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		var attempts int
		err := db.QueryRow(ctx, `
			select publish_attempts from event_outbox where id = $1 and stream = $2
		`, id, stream).Scan(&attempts)
		if err != nil {
			t.Fatalf("read publish attempts for %s: %v", id, err)
		}
		if attempts >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("event %s reached only %d publish attempts, wanted %d", id, attempts, want)
		}
		time.Sleep(pollInterval)
	}
}

func isPublished(t *testing.T, ctx context.Context, db *pgxpool.Pool, id, stream string) bool {
	t.Helper()

	var published *time.Time
	err := db.QueryRow(ctx, `
		select published_at from event_outbox where id = $1 and stream = $2
	`, id, stream).Scan(&published)
	if err != nil {
		t.Fatalf("read published_at for %s: %v", id, err)
	}
	return published != nil
}
