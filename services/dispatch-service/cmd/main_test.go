package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/reliability"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
)

func TestFindNearestDriverReturnsRoutingResult(t *testing.T) {
	type observedRequest struct {
		method      string
		path        string
		contentType string
		body        routingRequest
		decodeErr   error
	}
	var observed observedRequest
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		observed = observedRequest{
			method:      r.Method,
			path:        r.URL.Path,
			contentType: r.Header.Get("Content-Type"),
		}
		observed.decodeErr = json.NewDecoder(r.Body).Decode(&observed.body)
		return routingTestHTTPResponse(r, http.StatusOK, `{"driver_id":"driver-7","distance_km":1.25,"eta_seconds":180,"trip_distance_km":8.5,"trip_duration_seconds":920.5,"route_provider":"test-fixture","route_calculated_at":"2026-09-15T00:00:00Z"}`), nil
	})}

	d := newRoutingTestDispatcher("http://routing.test", client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := d.findNearestDriver(ctx, events.RideRequested{
		RideID:     "ride-1",
		PickupLat:  37.775,
		PickupLng:  -122.419,
		DropoffLat: 37.78,
		DropoffLng: -122.42,
	})
	if err != nil {
		t.Fatalf("find nearest driver: %v", err)
	}
	if got.DriverID != "driver-7" || got.DistanceKM != 1.25 || got.ETASeconds != 180 || got.TripDistanceKM == nil || *got.TripDistanceKM != 8.5 || *got.TripDurationSeconds != 920.5 {
		t.Fatalf("routing response = %+v, want driver-7 at 1.25 km with 180-second ETA", got)
	}

	if observed.decodeErr != nil {
		t.Fatalf("decode routing request: %v", observed.decodeErr)
	}
	if observed.method != http.MethodPost {
		t.Fatalf("method = %q, want %q", observed.method, http.MethodPost)
	}
	if observed.path != "/v1/routes/nearest-driver" {
		t.Fatalf("path = %q, want %q", observed.path, "/v1/routes/nearest-driver")
	}
	if observed.contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", observed.contentType)
	}
	if observed.body != (routingRequest{PickupLat: 37.775, PickupLng: -122.419, DropoffLat: 37.78, DropoffLng: -122.42}) {
		t.Fatalf("routing request = %+v, want pickup coordinates", observed.body)
	}
}

func TestFindNearestDriverRejectsEmptyDriverID(t *testing.T) {
	var attempts atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return routingTestHTTPResponse(r, http.StatusOK, `{"distance_km":0,"eta_seconds":0}`), nil
	})}

	d := newRoutingTestDispatcher("http://routing.test", client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := d.findNearestDriver(ctx, events.RideRequested{})
	if err == nil || !strings.Contains(err.Error(), "empty driver_id") {
		t.Fatalf("error = %v, want empty driver_id error", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("routing attempts = %d, want 1 for a valid no-driver response", got)
	}
}

func TestFindNearestDriverRetriesUpstreamErrors(t *testing.T) {
	var attempts atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return routingTestHTTPResponse(r, http.StatusServiceUnavailable, "temporarily unavailable"), nil
	})}

	d := newRoutingTestDispatcher("http://routing.test", client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := d.findNearestDriver(ctx, events.RideRequested{RideID: "ride-503"})
	if err == nil || !strings.Contains(err.Error(), "routing-service returned non-200 response") {
		t.Fatalf("error = %v, want non-200 response error", err)
	}
	if got := attempts.Load(); got != int32(reliability.MaxRetryAttempts) {
		t.Fatalf("routing attempts = %d, want %d", got, reliability.MaxRetryAttempts)
	}
}

func TestFindNearestDriverRetriesUnavailableService(t *testing.T) {
	var attempts atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, errors.New("routing unavailable")
	})}
	d := newRoutingTestDispatcher("http://routing.invalid", client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := d.findNearestDriver(ctx, events.RideRequested{RideID: "ride-unavailable"})
	if err == nil || !strings.Contains(err.Error(), "routing unavailable") {
		t.Fatalf("error = %v, want routing unavailable error", err)
	}
	if got := attempts.Load(); got != int32(reliability.MaxRetryAttempts) {
		t.Fatalf("routing attempts = %d, want %d", got, reliability.MaxRetryAttempts)
	}
}

func TestFindNearestDriverRetriesInvalidJSON(t *testing.T) {
	var attempts atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return routingTestHTTPResponse(r, http.StatusOK, `{`), nil
	})}

	d := newRoutingTestDispatcher("http://routing.test", client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := d.findNearestDriver(ctx, events.RideRequested{RideID: "ride-invalid-json"})
	if err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("error = %v, want JSON decoding error", err)
	}
	if got := attempts.Load(); got != int32(reliability.MaxRetryAttempts) {
		t.Fatalf("routing attempts = %d, want %d", got, reliability.MaxRetryAttempts)
	}
}

func newRoutingTestDispatcher(url string, client *http.Client) *dispatcher {
	return &dispatcher{
		cfg:    config.Config{RoutingServiceURL: url},
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		client: client,
	}
}

func routingTestHTTPResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestRoutingReadinessUsesShortDeadline(t *testing.T) {
	var observed time.Duration
	d := newRoutingTestDispatcher("http://routing.test", &http.Client{
		Timeout: reliability.RoutingTimeout,
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			deadline, ok := r.Context().Deadline()
			if !ok {
				t.Fatal("readiness request has no deadline")
			}
			observed = time.Until(deadline)
			return routingTestHTTPResponse(r, http.StatusOK, `{}`), nil
		}),
	})
	if err := d.checkRoutingService(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observed <= 0 || observed > reliability.ReadinessTimeout {
		t.Fatalf("readiness deadline = %s, want at most %s", observed, reliability.ReadinessTimeout)
	}
}

func TestDispatchDeadLetterFailureIsCounted(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	d := &dispatcher{rdb: rdb}
	before := counterValue(assignmentFailures)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := d.publishDeadLetter(ctx, redis.XMessage{ID: "1-0", Values: map[string]any{"event": "bad JSON"}}, errors.New("invalid event"))
	if err == nil {
		t.Fatal("closed Redis client unexpectedly published a dead letter")
	}
	if got := counterValue(assignmentFailures); got != before+1 {
		t.Fatalf("terminal failure attempts = %g, want %g", got, before+1)
	}
}

func counterValue(counter prometheus.Counter) float64 {
	value := &dto.Metric{}
	if err := counter.Write(value); err != nil {
		panic(err)
	}
	return value.GetCounter().GetValue()
}
