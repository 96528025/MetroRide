//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/redis/go-redis/v9"
)

type completeRideResponse struct {
	RideID  string `json:"ride_id"`
	Status  string `json:"status"`
	EventID string `json:"event_id"`
	Error   string `json:"error"`
}

func TestCompleteRideRejectsUnknownAndUnassignedRides(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	code, body := completeRide(t, ctx, uuid.NewString())
	if code != http.StatusNotFound || body.Error != "ride not found" {
		t.Fatalf("unknown ride: status = %d, body = %+v; want 404 ride not found", code, body)
	}

	code, body = completeRide(t, ctx, "not-a-uuid")
	if code != http.StatusNotFound || body.Error != "ride not found" {
		t.Fatalf("malformed ride id: status = %d, body = %+v; want 404 ride not found", code, body)
	}

	// A ride that dispatch has not assigned yet, inserted directly so the test does not
	// depend on stopping dispatch-service.
	db := openDB(t, ctx)
	defer db.Close()
	rideID := uuid.NewString()
	if _, err := db.Exec(ctx, `
		insert into rides (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, status, created_at, updated_at)
		values ($1, $2, 37.775, -122.419, 37.789, -122.401, 'requested', now(), now())
	`, rideID, "integration-complete-requested-"+rideID); err != nil {
		t.Fatalf("insert requested ride: %v", err)
	}

	code, body = completeRide(t, ctx, rideID)
	if code != http.StatusConflict || body.Error != "ride is requested" {
		t.Fatalf("requested ride: status = %d, body = %+v; want 409 ride is requested", code, body)
	}
	if status := rideStatus(t, ctx, db, rideID); status != "requested" {
		t.Fatalf("ride status after rejected completion = %q, want requested", status)
	}
	if n := outboxRows(t, ctx, db, rideID, events.TypeRideCompleted); n != 0 {
		t.Fatalf("outbox rows for a rejected completion = %d, want 0", n)
	}
}

func TestCompleteRideMovesAnAssignedRideOnceAndPublishesOneEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	created := createRide(t, ctx, createRideRequest{
		RiderID:    "integration-complete-" + uuid.NewString(),
		PickupLat:  37.775,
		PickupLng:  -122.419,
		DropoffLat: 37.789,
		DropoffLng: -122.401,
	})
	waitForAssignedRide(t, ctx, created.RideID)

	code, body := completeRide(t, ctx, created.RideID)
	if code != http.StatusAccepted {
		t.Fatalf("complete: status = %d, body = %+v; want 202", code, body)
	}
	if body.RideID != created.RideID || body.Status != "completed" || body.EventID == "" {
		t.Fatalf("complete: body = %+v; want ride_id, status completed and an event_id", body)
	}
	if ride := getRide(t, ctx, created.RideID); ride.Status != "completed" {
		t.Fatalf("ride status after completion = %q, want completed", ride.Status)
	}

	code, again := completeRide(t, ctx, created.RideID)
	if code != http.StatusConflict || again.Error != "ride is completed" {
		t.Fatalf("second completion: status = %d, body = %+v; want 409 ride is completed", code, again)
	}

	db := openDB(t, ctx)
	defer db.Close()
	if n := outboxRows(t, ctx, db, created.RideID, events.TypeRideCompleted); n != 1 {
		t.Fatalf("ride_completed outbox rows = %d, want 1", n)
	}
	rdb := openRedis(t)
	defer func() { _ = rdb.Close() }()
	envelope := waitForCompletion(t, ctx, rdb, created.RideID, body.EventID)
	payload, err := events.DecodePayload[events.RideCompleted](envelope)
	if err != nil {
		t.Fatalf("decode ride_completed payload: %v", err)
	}
	if payload.RideID != created.RideID || payload.RiderID == "" || payload.DriverID == "" || payload.AssignmentID == "" {
		t.Fatalf("ride_completed payload = %+v; want ride, rider, driver and assignment ids", payload)
	}
	if _, err := time.Parse(time.RFC3339Nano, payload.CompletedAt); err != nil {
		t.Fatalf("completed_at %q is not RFC 3339: %v", payload.CompletedAt, err)
	}
	if envelope.Source != "rider-service" || envelope.CorrelationID != created.RideID {
		t.Fatalf("envelope = %+v; want source rider-service and the ride as correlation id", envelope)
	}
}

// TestConcurrentCompletionsAcceptExactlyOne pins the concurrency control of the
// endpoint: the conditional update on rides (status = 'assigned') lets exactly one
// of ten simultaneous requests through and answers the rest 409. The outbox row and
// the stream entry are checked for the one accepted request; the "exactly one entry
// on Redis" part is an observation of the fault-free path, not a guarantee, because
// the relay is at-least-once (see shared/pkg/outbox).
func TestConcurrentCompletionsAcceptExactlyOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	created := createRide(t, ctx, createRideRequest{
		RiderID:    "integration-complete-concurrent-" + uuid.NewString(),
		PickupLat:  37.775,
		PickupLng:  -122.419,
		DropoffLat: 37.789,
		DropoffLng: -122.401,
	})
	waitForAssignedRide(t, ctx, created.RideID)

	const attempts = 10
	type outcome struct {
		code int
		body completeRideResponse
	}
	outcomes := make([]outcome, attempts)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(attempts)
	done.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			code, body := completeRide(t, ctx, created.RideID)
			outcomes[i] = outcome{code: code, body: body}
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()

	accepted := 0
	var eventID string
	for i, o := range outcomes {
		switch o.code {
		case http.StatusAccepted:
			accepted++
			eventID = o.body.EventID
		case http.StatusConflict:
			if o.body.Error != "ride is completed" {
				t.Fatalf("attempt %d: 409 body = %+v, want ride is completed", i, o.body)
			}
		default:
			t.Fatalf("attempt %d: status = %d, body = %+v; want 202 or 409", i, o.code, o.body)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted completions = %d of %d, want exactly 1", accepted, attempts)
	}

	db := openDB(t, ctx)
	defer db.Close()
	var outboxIDs []string
	rows, err := db.Query(ctx, `select id from event_outbox where aggregate_id = $1 and event_type = $2`, created.RideID, events.TypeRideCompleted)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan outbox id: %v", err)
		}
		outboxIDs = append(outboxIDs, id)
	}
	rows.Close()
	if len(outboxIDs) != 1 || outboxIDs[0] != eventID {
		t.Fatalf("ride_completed outbox rows = %v, want exactly the accepted event %s", outboxIDs, eventID)
	}

	rdb := openRedis(t)
	defer func() { _ = rdb.Close() }()
	waitForCompletion(t, ctx, rdb, created.RideID, eventID)
	ids := completionEnvelopeIDs(t, ctx, rdb, created.RideID)
	if len(ids) != 1 || ids[0] != eventID {
		t.Fatalf("distinct ride_completed envelope ids on %s for the ride = %v, want only %s", events.StreamRideCompletions, ids, eventID)
	}
}

// TestCompleteRideRollsBackWhenTheTransactionCannotFinish covers the three ways
// the completion transaction can fail after the conditional update: the ride has
// two assignment rows, it has none, and the outbox insert is blocked past the
// PostgreSQL timeout. In every case the response is 500, the ride is still
// assigned, and no ride_completed outbox row exists; the endpoint then succeeds
// once the obstacle is gone.
func TestCompleteRideRollsBackWhenTheTransactionCannotFinish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	created := createRide(t, ctx, createRideRequest{
		RiderID:    "integration-complete-rollback-" + uuid.NewString(),
		PickupLat:  37.775,
		PickupLng:  -122.419,
		DropoffLat: 37.789,
		DropoffLng: -122.401,
	})
	waitForAssignedRide(t, ctx, created.RideID)

	db := openDB(t, ctx)
	defer db.Close()

	var assignmentID, driverID string
	var distanceKM float64
	var etaSeconds int
	var assignedAt time.Time
	if err := db.QueryRow(ctx, `
		select id, driver_id, distance_km, eta_seconds, created_at from ride_assignments where ride_id = $1
	`, created.RideID).Scan(&assignmentID, &driverID, &distanceKM, &etaSeconds, &assignedAt); err != nil {
		t.Fatalf("read assignment: %v", err)
	}

	assertNotCompleted := func(step string) {
		t.Helper()
		if status := rideStatus(t, ctx, db, created.RideID); status != "assigned" {
			t.Fatalf("%s: ride status = %q, want assigned", step, status)
		}
		if n := outboxRows(t, ctx, db, created.RideID, events.TypeRideCompleted); n != 0 {
			t.Fatalf("%s: ride_completed outbox rows = %d, want 0", step, n)
		}
	}

	// Two assignment rows.
	extraID := uuid.NewString()
	if _, err := db.Exec(ctx, `
		insert into ride_assignments (id, ride_id, driver_id, distance_km, eta_seconds, created_at)
		values ($1, $2, $3, $4, $5, now())
	`, extraID, created.RideID, driverID, distanceKM, etaSeconds); err != nil {
		t.Fatalf("insert second assignment: %v", err)
	}
	code, body := completeRide(t, ctx, created.RideID)
	if code != http.StatusInternalServerError || body.Error != "ride assignment state inconsistent" {
		t.Fatalf("two assignments: status = %d, body = %+v; want 500 ride assignment state inconsistent", code, body)
	}
	assertNotCompleted("two assignments")

	// No assignment row.
	if _, err := db.Exec(ctx, `delete from ride_assignments where ride_id = $1`, created.RideID); err != nil {
		t.Fatalf("delete assignments: %v", err)
	}
	code, body = completeRide(t, ctx, created.RideID)
	if code != http.StatusInternalServerError || body.Error != "ride assignment state inconsistent" {
		t.Fatalf("no assignment: status = %d, body = %+v; want 500 ride assignment state inconsistent", code, body)
	}
	assertNotCompleted("no assignment")

	// Restore the original assignment row.
	if _, err := db.Exec(ctx, `
		insert into ride_assignments (id, ride_id, driver_id, distance_km, eta_seconds, created_at)
		values ($1, $2, $3, $4, $5, $6)
	`, assignmentID, created.RideID, driverID, distanceKM, etaSeconds, assignedAt); err != nil {
		t.Fatalf("restore assignment: %v", err)
	}

	// The outbox insert cannot finish: another session holds event_outbox exclusively, so
	// Enqueue waits until the 2s PostgreSQL deadline cancels it and the transaction rolls back.
	holder, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock holder: %v", err)
	}
	if _, err := holder.Exec(ctx, `lock table event_outbox in access exclusive mode`); err != nil {
		t.Fatalf("lock event_outbox: %v", err)
	}
	code, body = completeRide(t, ctx, created.RideID)
	if rollbackErr := holder.Rollback(ctx); rollbackErr != nil {
		t.Fatalf("release lock: %v", rollbackErr)
	}
	if code != http.StatusInternalServerError || body.Error != "failed to complete ride" {
		t.Fatalf("locked outbox: status = %d, body = %+v; want 500 failed to complete ride", code, body)
	}
	assertNotCompleted("locked outbox")

	// With the obstacles gone the same request succeeds.
	code, body = completeRide(t, ctx, created.RideID)
	if code != http.StatusAccepted || body.Status != "completed" {
		t.Fatalf("after recovery: status = %d, body = %+v; want 202 completed", code, body)
	}
	if n := outboxRows(t, ctx, db, created.RideID, events.TypeRideCompleted); n != 1 {
		t.Fatalf("after recovery: ride_completed outbox rows = %d, want 1", n)
	}
}

func completeRide(t *testing.T, ctx context.Context, rideID string) (int, completeRideResponse) {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL("8080")+"/v1/rides/"+rideID+"/complete", nil)
	if err != nil {
		t.Fatalf("build complete ride request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("complete ride request failed: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read complete ride response: %v", err)
	}
	var out completeRideResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode complete ride response %q: %v", strings.TrimSpace(string(raw)), err)
	}
	return resp.StatusCode, out
}

func rideStatus(t *testing.T, ctx context.Context, db *pgxpool.Pool, rideID string) string {
	t.Helper()

	var status string
	if err := db.QueryRow(ctx, `select status from rides where id = $1`, rideID).Scan(&status); err != nil {
		t.Fatalf("read ride status: %v", err)
	}
	return status
}

func outboxRows(t *testing.T, ctx context.Context, db *pgxpool.Pool, rideID, eventType string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(ctx, `
		select count(*) from event_outbox where aggregate_id = $1 and event_type = $2
	`, rideID, eventType).Scan(&count); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	return count
}

// completionEnvelopes returns every ride_completed envelope for the ride on
// events.ride.completions, in stream order.
func completionEnvelopes(t *testing.T, ctx context.Context, rdb *redis.Client, rideID string) []events.Envelope {
	t.Helper()

	messages, err := rdb.XRange(ctx, events.StreamRideCompletions, "-", "+").Result()
	if err != nil {
		t.Fatalf("read %s: %v", events.StreamRideCompletions, err)
	}
	var found []events.Envelope
	for _, message := range messages {
		envelope, err := events.DecodeEnvelope(message)
		if err != nil {
			t.Fatalf("decode entry %s: %v", message.ID, err)
		}
		if envelope.Type == events.TypeRideCompleted && envelope.CorrelationID == rideID {
			found = append(found, envelope)
		}
	}
	return found
}

func completionEnvelopeIDs(t *testing.T, ctx context.Context, rdb *redis.Client, rideID string) []string {
	t.Helper()

	seen := map[string]bool{}
	var ids []string
	for _, envelope := range completionEnvelopes(t, ctx, rdb, rideID) {
		if !seen[envelope.ID] {
			seen[envelope.ID] = true
			ids = append(ids, envelope.ID)
		}
	}
	return ids
}

func waitForCompletion(t *testing.T, ctx context.Context, rdb *redis.Client, rideID, eventID string) events.Envelope {
	t.Helper()

	for {
		for _, envelope := range completionEnvelopes(t, ctx, rdb, rideID) {
			if envelope.ID == eventID {
				return envelope
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for ride_completed %s on %s", eventID, events.StreamRideCompletions)
		case <-time.After(pollInterval):
		}
	}
}
