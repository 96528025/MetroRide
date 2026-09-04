package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNearestDriverReportsHaversineAlgorithm(t *testing.T) {
	svc := &routingService{
		drivers: map[string]driver{
			"driver-1": {
				ID:        "driver-1",
				Latitude:  37.7749,
				Longitude: -122.4194,
				Available: true,
			},
		},
	}
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/routes/nearest-driver",
		strings.NewReader(`{"pickup_lat":37.775,"pickup_lng":-122.419}`),
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
	if response.Algorithm != "haversine-nearest" {
		t.Fatalf("algorithm = %q, want %q", response.Algorithm, "haversine-nearest")
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
