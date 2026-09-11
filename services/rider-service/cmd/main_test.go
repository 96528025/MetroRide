package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/httpx"
)

func TestRiderReadinessDependsOnlyOnPostgres(t *testing.T) {
	postgresChecks := 0
	checks := riderReadinessChecks(func(context.Context) error {
		postgresChecks++
		return nil
	})

	if len(checks) != 1 {
		t.Fatalf("readiness checks = %d, want only postgres", len(checks))
	}
	if _, ok := checks["postgres"]; !ok {
		t.Fatal("postgres readiness check is missing")
	}
	if _, ok := checks["redis"]; ok {
		t.Fatal("redis must not be a rider-service readiness dependency")
	}

	recorder := serveReadiness(t, checks)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if postgresChecks != 1 {
		t.Fatalf("postgres readiness checks = %d, want 1", postgresChecks)
	}
	var response struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "ready" {
		t.Fatalf("readiness status = %q, want %q", response.Status, "ready")
	}
}

func TestRiderReadinessFailsWhenPostgresIsUnavailable(t *testing.T) {
	checks := riderReadinessChecks(func(context.Context) error {
		return errors.New("postgres unavailable")
	})

	recorder := serveReadiness(t, checks)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
}

func serveReadiness(t *testing.T, checks map[string]httpx.ReadinessCheck) *httptest.ResponseRecorder {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := httpx.CommonMuxWithReadiness(log, checks)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func TestCompletionRejectionMapsTheLookupOutcomeToTheResponse(t *testing.T) {
	cases := []struct {
		name      string
		status    string
		lookupErr error
		wantCode  int
		wantError string
	}{
		{name: "ride still requested", status: "requested", lookupErr: nil, wantCode: http.StatusConflict, wantError: "ride is requested"},
		{name: "ride already completed", status: "completed", lookupErr: nil, wantCode: http.StatusConflict, wantError: "ride is completed"},
		{name: "ride cancelled", status: "cancelled", lookupErr: nil, wantCode: http.StatusConflict, wantError: "ride is cancelled"},
		{name: "no such ride", status: "", lookupErr: pgx.ErrNoRows, wantCode: http.StatusNotFound, wantError: "ride not found"},
		{name: "wrapped no rows", status: "", lookupErr: fmt.Errorf("scan: %w", pgx.ErrNoRows), wantCode: http.StatusNotFound, wantError: "ride not found"},
		{name: "lookup failed is not a missing ride", status: "", lookupErr: errors.New("connection reset"), wantCode: http.StatusInternalServerError, wantError: "failed to complete ride"},
		{name: "cancelled lookup is not a missing ride", status: "", lookupErr: context.DeadlineExceeded, wantCode: http.StatusInternalServerError, wantError: "failed to complete ride"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := completionRejection(tc.status, tc.lookupErr)
			if code != tc.wantCode {
				t.Fatalf("status = %d, want %d", code, tc.wantCode)
			}
			if body["error"] != tc.wantError {
				t.Fatalf("error = %q, want %q", body["error"], tc.wantError)
			}
			if len(body) != 1 {
				t.Fatalf("body = %v, want only the error key", body)
			}
		})
	}
}

func TestRideCompletedUsesTheSnakeCaseFieldNamesOfTheContract(t *testing.T) {
	body, err := json.Marshal(events.RideCompleted{
		RideID:       "ride-1",
		RiderID:      "rider-42",
		DriverID:     "driver-2",
		AssignmentID: "assignment-1",
		CompletedAt:  "2026-09-10T12:00:00.123456789Z",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]string
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := map[string]string{
		"ride_id":       "ride-1",
		"rider_id":      "rider-42",
		"driver_id":     "driver-2",
		"assignment_id": "assignment-1",
		"completed_at":  "2026-09-10T12:00:00.123456789Z",
	}
	if len(fields) != len(want) {
		t.Fatalf("fields = %v, want exactly %v", fields, want)
	}
	for name, value := range want {
		if fields[name] != value {
			t.Fatalf("field %q = %q, want %q (got %s)", name, fields[name], value, body)
		}
	}
	if events.StreamRideCompletions != "events.ride.completions" {
		t.Fatalf("StreamRideCompletions = %q", events.StreamRideCompletions)
	}
}
