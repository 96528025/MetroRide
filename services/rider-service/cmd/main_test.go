package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

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
