//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/redis/go-redis/v9"
)

const pollInterval = 500 * time.Millisecond

type createRideRequest struct {
	RiderID    string  `json:"rider_id"`
	PickupLat  float64 `json:"pickup_lat"`
	PickupLng  float64 `json:"pickup_lng"`
	DropoffLat float64 `json:"dropoff_lat"`
	DropoffLng float64 `json:"dropoff_lng"`
}

type createRideResponse struct {
	RideID string `json:"ride_id"`
	Status string `json:"status"`
}

type rideResponse struct {
	ID        string  `json:"id"`
	RiderID   string  `json:"rider_id"`
	DriverID  *string `json:"driver_id"`
	Status    string  `json:"status"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
}

func TestRideAssignmentHappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := createRideRequest{
		RiderID:    "integration-happy-" + uuid.NewString(),
		PickupLat:  37.775,
		PickupLng:  -122.419,
		DropoffLat: 37.789,
		DropoffLng: -122.401,
	}

	created := createRide(t, ctx, req)
	ride := waitForAssignedRide(t, ctx, created.RideID)
	if ride.DriverID == nil || *ride.DriverID == "" {
		t.Fatalf("expected assigned ride to include driver_id: %+v", ride)
	}
}

func TestDuplicateRideRequestedEventIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := createRideRequest{
		RiderID:    "integration-idempotency-" + uuid.NewString(),
		PickupLat:  37.775,
		PickupLng:  -122.419,
		DropoffLat: 37.789,
		DropoffLng: -122.401,
	}

	created := createRide(t, ctx, req)
	ride := waitForAssignedRide(t, ctx, created.RideID)
	if ride.DriverID == nil {
		t.Fatalf("expected first assignment to set driver_id: %+v", ride)
	}
	firstDriver := *ride.DriverID

	db := openDB(t, ctx)
	defer db.Close()
	before := assignmentCount(t, ctx, db, created.RideID)
	if before != 1 {
		t.Fatalf("expected exactly one assignment before duplicate event, got %d", before)
	}

	rdb := openRedis(t)
	defer func() { _ = rdb.Close() }()

	payload := events.RideRequested{
		RideID:      created.RideID,
		RiderID:     req.RiderID,
		PickupLat:   req.PickupLat,
		PickupLng:   req.PickupLng,
		DropoffLat:  req.DropoffLat,
		DropoffLng:  req.DropoffLng,
		RequestedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	envelope, err := events.NewEnvelope(uuid.NewString(), events.TypeRideRequested, "integration-test", created.RideID, payload)
	if err != nil {
		t.Fatalf("create duplicate event envelope: %v", err)
	}
	if _, err := events.Publish(ctx, rdb, events.StreamRideRequests, envelope); err != nil {
		t.Fatalf("publish duplicate ride_requested event: %v", err)
	}

	time.Sleep(3 * time.Second)

	after := assignmentCount(t, ctx, db, created.RideID)
	if after != 1 {
		t.Fatalf("expected duplicate event to preserve one assignment, got %d", after)
	}
	rideAfter := getRide(t, ctx, created.RideID)
	if rideAfter.DriverID == nil || *rideAfter.DriverID != firstDriver {
		t.Fatalf("expected duplicate event to preserve driver %q, got %+v", firstDriver, rideAfter)
	}
}

func createRide(t *testing.T, ctx context.Context, req createRideRequest) createRideResponse {
	t.Helper()

	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal create ride request: %v", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL("8080")+"/v1/rides", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build create ride request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("create ride request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create ride returned status %s", resp.Status)
	}
	var out createRideResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode create ride response: %v", err)
	}
	if out.RideID == "" {
		t.Fatal("create ride response missing ride_id")
	}
	return out
}

func waitForAssignedRide(t *testing.T, ctx context.Context, rideID string) rideResponse {
	t.Helper()

	var last rideResponse
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for ride assignment; last state: %+v", last)
		default:
		}
		last = getRide(t, ctx, rideID)
		if last.Status == "assigned" {
			return last
		}
		time.Sleep(pollInterval)
	}
}

func getRide(t *testing.T, ctx context.Context, rideID string) rideResponse {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL("8080")+"/v1/rides/"+rideID, nil)
	if err != nil {
		t.Fatalf("build get ride request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get ride request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get ride returned status %s", resp.Status)
	}
	var out rideResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode get ride response: %v", err)
	}
	return out
}

func assignmentCount(t *testing.T, ctx context.Context, db *pgxpool.Pool, rideID string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(ctx, `select count(*) from ride_assignments where ride_id = $1`, rideID).Scan(&count); err != nil {
		t.Fatalf("query assignment count: %v", err)
	}
	return count
}

func openDB(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	dsn := getenv("INTEGRATION_POSTGRES_DSN", "postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable")
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	return db
}

func openRedis(t *testing.T) *redis.Client {
	t.Helper()

	return redis.NewClient(&redis.Options{Addr: getenv("INTEGRATION_REDIS_ADDR", "localhost:6379")})
}

func baseURL(port string) string {
	return fmt.Sprintf("http://%s:%s", getenv("INTEGRATION_HOST", "localhost"), port)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
