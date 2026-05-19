package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ReadinessCheck func(context.Context) error

func NewServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func CommonMux(l *slog.Logger) *http.ServeMux {
	return CommonMuxWithReadiness(l, nil)
}

func CommonMuxWithReadiness(l *slog.Logger, checks map[string]ReadinessCheck) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		RespondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		failures := map[string]string{}
		for name, check := range checks {
			if check == nil {
				continue
			}
			if err := check(r.Context()); err != nil {
				failures[name] = err.Error()
			}
		}
		if len(failures) > 0 {
			l.Error("readiness check failed", "failures", failures)
			RespondJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "failures": failures})
			return
		}
		RespondJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	return mux
}

func CheckHTTP(url string, client *http.Client) ReadinessCheck {
	return func(ctx context.Context) error {
		if client == nil {
			client = http.DefaultClient
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			return errors.New(resp.Status)
		}
		return nil
	}
}

func RespondJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func DecodeJSON(r *http.Request, out any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(out)
}

func Shutdown(ctx context.Context, srv *http.Server, l *slog.Logger) {
	if err := srv.Shutdown(ctx); err != nil {
		l.Error("http server shutdown failed", "error", err)
	}
}
