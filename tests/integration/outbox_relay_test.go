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

// TestOutboxRelayKeepsProgressPastPublishFailures pins two relay properties:
// successful rows are not replayed after a partial failure, and permanently bad
// rows cannot starve healthy work queued behind a full batch of failures.
//
// The failure is real, not mocked: poisoned events target a Redis key that
// already holds a string, so XADD fails with WRONGTYPE on every attempt.
func TestOutboxRelayKeepsProgressPastPublishFailures(t *testing.T) {
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

	// Make the poison events permanently undeliverable.
	if err := rdb.Set(ctx, poisonedStream, "not-a-stream", 0).Err(); err != nil {
		t.Fatalf("poison target stream: %v", err)
	}

	first := enqueueTestEvent(t, ctx, db, sourceService, deliverableStream)
	// NewRelay's production batch size is 25. The first poll takes the healthy
	// event plus 24 poison events, leaving one poison event and the later healthy
	// event outside the batch. Retry scheduling makes failed rows briefly
	// ineligible, so that remaining healthy work enters the next batch instead of
	// sitting forever behind the same 25 poison rows.
	poisoned := make([]string, 0, 25)
	for i := 0; i < 25; i++ {
		poisoned = append(poisoned, enqueueTestEvent(t, ctx, db, sourceService, poisonedStream))
	}
	second := enqueueTestEvent(t, ctx, db, sourceService, deliverableStream)
	// Give one due row prior failures so this batch also verifies the production
	// UPDATE applies the capped retry delay using PostgreSQL's clock.
	if _, err := db.Exec(ctx, `
		update event_outbox set publish_attempts = 7
		where id = $1 and stream = $2
	`, poisoned[1], poisonedStream); err != nil {
		t.Fatalf("seed poison retry history: %v", err)
	}
	databaseBeforeBatch := databaseClock(t, ctx, db)

	relay := outbox.NewRelay(sourceService, slog.New(slog.NewTextHandler(io.Discard, nil)), db, rdb)
	if err := relay.PublishPending(ctx); err == nil {
		t.Fatal("first relay batch unexpectedly had no poison-event failures")
	}
	if attempts := publishAttempts(t, ctx, db, poisoned[0], poisonedStream); attempts != 1 {
		t.Fatalf("first poison event attempts = %d after one batch, want 1", attempts)
	}
	if attempts := publishAttempts(t, ctx, db, poisoned[23], poisonedStream); attempts != 1 {
		t.Fatalf("last in-batch poison event attempts = %d after one batch, want 1; relay stopped at an earlier failure", attempts)
	}
	attempts, nextAttempt := retryState(t, ctx, db, poisoned[1], poisonedStream)
	if attempts != 8 {
		t.Fatalf("seeded poison event attempts = %d after one batch, want 8", attempts)
	}
	if nextAttempt.Before(databaseBeforeBatch.Add(30 * time.Second)) {
		t.Fatalf("seeded poison retry scheduled at %s from database time %s; want the 30-second capped backoff", nextAttempt, databaseBeforeBatch)
	}
	// Keep the assertion independent of machine speed: even if the first batch
	// took longer than the production backoff, attempted rows remain ineligible
	// while the second call exercises the rows left outside that batch.
	if _, err := db.Exec(ctx, `
		update event_outbox set next_attempt_at = now() + interval '1 minute'
		where source_service = $1 and published_at is null and publish_attempts > 0
	`, sourceService); err != nil {
		t.Fatalf("hold attempted poison events: %v", err)
	}
	if err := relay.PublishPending(ctx); err == nil {
		t.Fatal("second relay batch unexpectedly had no poison-event failures")
	}

	// The first call fills its 25-row limit before reaching the later healthy
	// event. The second call must skip the delayed failures, continue past the one
	// unattempted poison row, and publish that healthy event in the same batch.
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
	for _, id := range poisoned {
		if isPublished(t, ctx, db, id, poisonedStream) {
			t.Fatalf("event %s was never delivered but is marked published", id)
		}
	}

	// Make all failures eligible again and prove they remain retryable after
	// healthy work has passed them.
	if _, err := db.Exec(ctx, `
		update event_outbox set next_attempt_at = now()
		where source_service = $1 and published_at is null
	`, sourceService); err != nil {
		t.Fatalf("make poison events retryable: %v", err)
	}
	if err := relay.PublishPending(ctx); err == nil {
		t.Fatal("retry relay batch unexpectedly had no poison-event failures")
	}
	if attempts := publishAttempts(t, ctx, db, poisoned[0], poisonedStream); attempts != 2 {
		t.Fatalf("oldest poison event attempts = %d, want 2", attempts)
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

	// Enqueue stamps created_at from the application clock and PostgreSQL supplies
	// the initial retry time. Separate rows so their test order is deterministic.
	time.Sleep(5 * time.Millisecond)
	return envelope.ID
}

func publishAttempts(t *testing.T, ctx context.Context, db *pgxpool.Pool, id, stream string) int {
	t.Helper()

	var attempts int
	if err := db.QueryRow(ctx, `
		select publish_attempts from event_outbox where id = $1 and stream = $2
	`, id, stream).Scan(&attempts); err != nil {
		t.Fatalf("read publish attempts for %s: %v", id, err)
	}
	return attempts
}

func retryState(t *testing.T, ctx context.Context, db *pgxpool.Pool, id, stream string) (int, time.Time) {
	t.Helper()

	var attempts int
	var nextAttempt time.Time
	if err := db.QueryRow(ctx, `
		select publish_attempts, next_attempt_at
		from event_outbox where id = $1 and stream = $2
	`, id, stream).Scan(&attempts, &nextAttempt); err != nil {
		t.Fatalf("read retry state for %s: %v", id, err)
	}
	return attempts, nextAttempt
}

func databaseClock(t *testing.T, ctx context.Context, db *pgxpool.Pool) time.Time {
	t.Helper()

	var now time.Time
	if err := db.QueryRow(ctx, `select clock_timestamp()`).Scan(&now); err != nil {
		t.Fatalf("read database clock: %v", err)
	}
	return now
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
