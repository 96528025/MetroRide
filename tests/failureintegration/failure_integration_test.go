//go:build failureintegration

package failureintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/redis/go-redis/v9"
)

const (
	failureTestTimeout = 30 * time.Second
	dlqPollInterval    = 500 * time.Millisecond
)

type createRideRequest struct {
	RiderID    string  `json:"rider_id"`
	PickupLat  float64 `json:"pickup_lat"`
	PickupLng  float64 `json:"pickup_lng"`
	DropoffLat float64 `json:"dropoff_lat"`
	DropoffLng float64 `json:"dropoff_lng"`
}

type createRideResponse struct {
	RideID  string `json:"ride_id"`
	Status  string `json:"status"`
	EventID string `json:"event_id"`
}

type rideResponse struct {
	ID       string  `json:"id"`
	DriverID *string `json:"driver_id"`
	Status   string  `json:"status"`
}

func TestRoutingOutageDeadLettersRideRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), failureTestTimeout)
	defer cancel()

	assertRoutingUnavailable(t, ctx)

	rdb := redis.NewClient(&redis.Options{
		Addr: getenv("INTEGRATION_REDIS_ADDR", "localhost:6379"),
	})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}

	baseline := deadLetterBaseline(t, ctx, rdb)
	created := createRide(t, ctx, createRideRequest{
		RiderID:    "failure-integration-" + uuid.NewString(),
		PickupLat:  37.775,
		PickupLng:  -122.419,
		DropoffLat: 37.789,
		DropoffLng: -122.401,
	})

	deadLetter := waitForDeadLetter(t, ctx, rdb, baseline, created.RideID)
	assertDeadLetter(t, deadLetter, created)
	assertRideWasNotAssigned(t, ctx, created.RideID)
}

func assertRoutingUnavailable(t *testing.T, ctx context.Context) {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL("8083")+"/healthz", nil)
	if err != nil {
		t.Fatalf("build routing outage check: %v", err)
	}
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	t.Fatalf("routing-service must be unavailable before the failure test; received %s", resp.Status)
}

func deadLetterBaseline(t *testing.T, ctx context.Context, rdb *redis.Client) string {
	t.Helper()

	messages, err := rdb.XRevRangeN(ctx, events.StreamDeadLetter, "+", "-", 1).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("read dead-letter baseline: %v", err)
	}
	if len(messages) == 0 {
		return "0-0"
	}
	return messages[0].ID
}

func createRide(t *testing.T, ctx context.Context, input createRideRequest) createRideResponse {
	t.Helper()

	body, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal create ride request: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL("8080")+"/v1/rides", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build create ride request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create ride request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create ride returned status %s", resp.Status)
	}
	var created createRideResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create ride response: %v", err)
	}
	if created.RideID == "" || created.EventID == "" {
		t.Fatalf("create ride response must include ride_id and event_id: %+v", created)
	}
	if created.Status != "requested" {
		t.Fatalf("expected newly created ride to be requested, got %+v", created)
	}
	return created
}

func waitForDeadLetter(t *testing.T, ctx context.Context, rdb *redis.Client, baseline, rideID string) events.DeadLetter {
	t.Helper()

	lastID := baseline
	var lastSeen events.DeadLetter
	for {
		result, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{events.StreamDeadLetter, lastID},
			Count:   100,
			Block:   dlqPollInterval,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for dead-letter entry for ride %s after %s; last payload: %+v", rideID, baseline, lastSeen)
			}
			t.Fatalf("read dead-letter stream: %v", err)
		}
		for _, stream := range result {
			for _, message := range stream.Messages {
				lastID = message.ID
				envelope, err := events.DecodeEnvelope(message)
				if err != nil {
					continue
				}
				payload, err := events.DecodePayload[events.DeadLetter](envelope)
				if err != nil {
					if envelope.CorrelationID == rideID {
						t.Fatalf("decode matching dead-letter payload: %v", err)
					}
					continue
				}
				lastSeen = payload
				if payload.RideID == rideID {
					if envelope.Type != "dead_lettered" || envelope.Source != "dispatch-service" || envelope.CorrelationID != rideID {
						t.Fatalf("unexpected matching dead-letter envelope: %+v", envelope)
					}
					return payload
				}
			}
		}
	}
}

func assertDeadLetter(t *testing.T, deadLetter events.DeadLetter, created createRideResponse) {
	t.Helper()

	if deadLetter.OriginalEventID != created.EventID {
		t.Fatalf("expected original event %q, got %+v", created.EventID, deadLetter)
	}
	if deadLetter.OriginalEventType != events.TypeRideRequested {
		t.Fatalf("expected original event type %q, got %+v", events.TypeRideRequested, deadLetter)
	}
	if deadLetter.RideID != created.RideID {
		t.Fatalf("expected ride %q, got %+v", created.RideID, deadLetter)
	}
	if deadLetter.Service != "dispatch-service" {
		t.Fatalf("expected dispatch-service dead-letter source, got %+v", deadLetter)
	}
	if strings.TrimSpace(deadLetter.Error) == "" || !strings.Contains(deadLetter.Error, "routing-service") {
		t.Fatalf("expected routing failure context, got %+v", deadLetter)
	}
	if _, err := time.Parse(time.RFC3339Nano, deadLetter.FailedAt); err != nil {
		t.Fatalf("dead-letter failed_at must be RFC3339Nano: %q: %v", deadLetter.FailedAt, err)
	}
}

func assertRideWasNotAssigned(t *testing.T, ctx context.Context, rideID string) {
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
	var ride rideResponse
	if err := json.NewDecoder(resp.Body).Decode(&ride); err != nil {
		t.Fatalf("decode ride response: %v", err)
	}
	if ride.Status != "requested" || ride.DriverID != nil {
		t.Fatalf("routing failure must leave ride requested and unassigned: %+v", ride)
	}

	db, err := pgxpool.New(ctx, getenv("INTEGRATION_POSTGRES_DSN", "postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable"))
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	defer db.Close()
	var assignments int
	if err := db.QueryRow(ctx, `select count(*) from ride_assignments where ride_id = $1`, rideID).Scan(&assignments); err != nil {
		t.Fatalf("count ride assignments: %v", err)
	}
	if assignments != 0 {
		t.Fatalf("expected no assignment rows for failed ride %s, got %d", rideID, assignments)
	}
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
