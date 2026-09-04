package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewServerAppliesOperationalTimeouts(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server := NewServer(":9090", handler)

	if server.Addr != ":9090" {
		t.Fatalf("address = %q, want %q", server.Addr, ":9090")
	}
	if server.Handler == nil {
		t.Fatal("handler is nil")
	}
	if server.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("read header timeout = %s, want %s", server.ReadHeaderTimeout, 5*time.Second)
	}
	if server.ReadTimeout != 10*time.Second {
		t.Fatalf("read timeout = %s, want %s", server.ReadTimeout, 10*time.Second)
	}
	if server.WriteTimeout != 15*time.Second {
		t.Fatalf("write timeout = %s, want %s", server.WriteTimeout, 15*time.Second)
	}
	if server.IdleTimeout != 60*time.Second {
		t.Fatalf("idle timeout = %s, want %s", server.IdleTimeout, 60*time.Second)
	}
}

func TestCommonMuxHealthAndDefaultReadiness(t *testing.T) {
	mux := CommonMux(testLogger())

	for _, test := range []struct {
		path       string
		wantStatus string
	}{
		{path: "/healthz", wantStatus: "ok"},
		{path: "/readyz", wantStatus: "ready"},
	} {
		t.Run(test.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			mux.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
			}
			if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
				t.Fatalf("content type = %q, want application/json", contentType)
			}
			var response struct {
				Status string `json:"status"`
			}
			if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if response.Status != test.wantStatus {
				t.Fatalf("response status = %q, want %q", response.Status, test.wantStatus)
			}
		})
	}
}

func TestCommonMuxReadinessReportsEveryFailure(t *testing.T) {
	checksRun := 0
	checks := map[string]ReadinessCheck{
		"healthy": func(context.Context) error {
			checksRun++
			return nil
		},
		"postgres": func(context.Context) error {
			checksRun++
			return errors.New("database unavailable")
		},
		"redis": func(context.Context) error {
			checksRun++
			return errors.New("stream unavailable")
		},
		"skipped": nil,
	}
	mux := CommonMuxWithReadiness(testLogger(), checks)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
	if checksRun != 3 {
		t.Fatalf("checks run = %d, want 3", checksRun)
	}
	var response struct {
		Status   string            `json:"status"`
		Failures map[string]string `json:"failures"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "not_ready" {
		t.Fatalf("response status = %q, want not_ready", response.Status)
	}
	if len(response.Failures) != 2 {
		t.Fatalf("failures = %#v, want postgres and redis", response.Failures)
	}
	if response.Failures["postgres"] != "database unavailable" {
		t.Fatalf("postgres failure = %q", response.Failures["postgres"])
	}
	if response.Failures["redis"] != "stream unavailable" {
		t.Fatalf("redis failure = %q", response.Failures["redis"])
	}
}

func TestCheckHTTPUsesGETAndValidatesStatus(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path == "/available" {
			return testHTTPResponse(r, http.StatusNoContent, ""), nil
		}
		return testHTTPResponse(r, http.StatusServiceUnavailable, "unavailable"), nil
	})}

	if err := CheckHTTP("http://dependency.test/available", client)(context.Background()); err != nil {
		t.Fatalf("successful check returned error: %v", err)
	}
	err := CheckHTTP("http://dependency.test/unavailable", client)(context.Background())
	if err == nil || err.Error() != "503 Service Unavailable" {
		t.Fatalf("failed check error = %v, want 503 Service Unavailable", err)
	}
}

func TestCheckHTTPHonorsCanceledContext(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, r.Context().Err()
	})}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := CheckHTTP("http://dependency.test/readyz", client)(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
}

func TestDecodeJSONAcceptsKnownFieldsAndRejectsUnknownFields(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}

	var decoded payload
	valid := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"metro"}`))
	if err := DecodeJSON(valid, &decoded); err != nil {
		t.Fatalf("decode valid JSON: %v", err)
	}
	if decoded.Name != "metro" {
		t.Fatalf("decoded name = %q, want metro", decoded.Name)
	}

	unknown := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"metro","extra":true}`))
	err := DecodeJSON(unknown, &decoded)
	if err == nil || !strings.Contains(err.Error(), `unknown field "extra"`) {
		t.Fatalf("unknown-field error = %v", err)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testHTTPResponse(request *http.Request, status int, body string) *http.Response {
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
