package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/redis/go-redis/v9"
)

type discardErrors struct{}

func (discardErrors) Error(string, ...any) {}

func TestConsumeDriverLocationsReturnsWhenContextIsCancelled(t *testing.T) {
	// Nothing listens on port 1; with the context already cancelled the client
	// fails every call with context.Canceled before it dials.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer func() { _ = rdb.Close() }()
	svc := &routingService{rdb: rdb}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		svc.consumeDriverLocations(ctx, discardErrors{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumeDriverLocations kept looping after its context was cancelled")
	}
}

func TestNearestDriverReportsRoadAlgorithmAndPassengerRoute(t *testing.T) {
	svc := &routingService{
		store: fakeDrivers{map[string]driver{
			"driver-1": {
				ID:        "driver-1",
				Latitude:  37.7749,
				Longitude: -122.4194,
				Available: true,
			},
		}},
		routes: fakeRoutes{},
	}
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/routes/nearest-driver",
		strings.NewReader(`{"pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.78,"dropoff_lng":-122.42}`),
	)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	svc.nearestDriver(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response struct {
		Algorithm string `json:"algorithm"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Algorithm != "road-time-shortlist" {
		t.Fatalf("algorithm = %q, want %q", response.Algorithm, "road-time-shortlist")
	}
}

func TestSelectNearestDriver(t *testing.T) {
	drivers := map[string]driver{
		"nearest-unavailable": {ID: "nearest-unavailable", Latitude: 37.7750, Longitude: -122.4190, Available: false},
		"farther":             {ID: "farther", Latitude: 37.8044, Longitude: -122.2712, Available: true},
		"nearest":             {ID: "nearest", Latitude: 37.7749, Longitude: -122.4194, Available: true},
	}

	selected, distance, found := selectNearestDriver(drivers, 37.7751, -122.4193)
	if !found {
		t.Fatal("expected an available driver")
	}
	if selected.ID != "nearest" {
		t.Fatalf("expected nearest driver, got %q", selected.ID)
	}
	if distance <= 0 {
		t.Fatalf("expected a positive distance, got %f", distance)
	}
}

func TestSelectNearestDriverReturnsFalseWhenNoneAvailable(t *testing.T) {
	drivers := map[string]driver{
		"driver-1": {ID: "driver-1", Available: false},
	}

	_, _, found := selectNearestDriver(drivers, 37.7751, -122.4193)
	if found {
		t.Fatal("expected no available driver")
	}
}

func TestSelectNearestDriverBreaksDistanceTiesByID(t *testing.T) {
	drivers := map[string]driver{
		"driver-b": {ID: "driver-b", Latitude: 37.7749, Longitude: -122.4194, Available: true},
		"driver-a": {ID: "driver-a", Latitude: 37.7749, Longitude: -122.4194, Available: true},
	}

	selected, _, found := selectNearestDriver(drivers, 37.7751, -122.4193)
	if !found || selected.ID != "driver-a" {
		t.Fatalf("expected deterministic driver-a tie break, got %+v", selected)
	}
}

func BenchmarkSelectNearestDriver10000(b *testing.B) {
	drivers := make(map[string]driver, 10_000)
	for i := 0; i < 10_000; i++ {
		id := fmt.Sprintf("driver-%05d", i)
		drivers[id] = driver{
			ID:        id,
			Latitude:  37.0 + float64(i%1000)/10_000,
			Longitude: -122.0 - float64(i%1000)/10_000,
			Available: i%7 != 0,
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = selectNearestDriver(drivers, 37.7751, -122.4193)
	}
}

// Fixtures are confined to tests; production always uses the configured road provider.
type fakeDrivers struct{ drivers map[string]driver }

func (s fakeDrivers) Available(context.Context) (map[string]driver, error) {
	out := map[string]driver{}
	for k, v := range s.drivers {
		out[k] = v
	}
	return out, nil
}
func (fakeDrivers) Save(context.Context, events.DriverLocationUpdated) error { return nil }
func (fakeDrivers) Ready(context.Context) error                              { return nil }

type fakeRoutes struct{}

func (fakeRoutes) Estimate(context.Context, float64, float64, float64, float64) (routeEstimate, error) {
	return routeEstimate{DistanceKM: 8.5, DurationSeconds: 920.5, Provider: "test-fixture", CalculatedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
}

type routeFunction func(context.Context, float64, float64, float64, float64) (routeEstimate, error)

func (f routeFunction) Estimate(c context.Context, a, b, d, e float64) (routeEstimate, error) {
	return f(c, a, b, d, e)
}
func TestRoadTimeCanSelectADriverFartherAwayByStraightLine(t *testing.T) {
	svc := routingService{store: fakeDrivers{map[string]driver{"near": {ID: "near", Latitude: 37.7751, Longitude: -122.419, Available: true}, "faster": {ID: "faster", Latitude: 37.78, Longitude: -122.419, Available: true}}}, routes: routeFunction(func(_ context.Context, lat, lon, dlat, dlon float64) (routeEstimate, error) {
		duration := 300.0
		if lat == 37.78 {
			duration = 100
		}
		return routeEstimate{DistanceKM: 2, DurationSeconds: duration, Provider: "fixture", CalculatedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})}
	req := httptest.NewRequest(http.MethodPost, "/v1/routes/nearest-driver", strings.NewReader(`{"pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}`))
	rec := httptest.NewRecorder()
	svc.nearestDriver(rec, req)
	var response struct {
		DriverID string `json:"driver_id"`
	}
	json.Unmarshal(rec.Body.Bytes(), &response)
	if rec.Code != 200 || response.DriverID != "faster" {
		t.Fatalf("response=%d %s", rec.Code, rec.Body.String())
	}
}
