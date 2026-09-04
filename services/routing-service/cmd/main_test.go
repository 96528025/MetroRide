package main

import (
	"encoding/json"
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
