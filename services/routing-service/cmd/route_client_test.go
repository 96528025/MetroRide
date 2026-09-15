package main

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func routeRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR enables route cache integration checks")
	}
	r := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { r.Close() })
	return r
}
func TestValhallaRouteValidatesContractAndCoalescesConcurrentRequests(t *testing.T) {
	r := routeRedis(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		var body struct {
			Locations      []struct{ Lat, Lon float64 }
			Costing, Units string
		}
		if json.NewDecoder(req.Body).Decode(&body) != nil || len(body.Locations) != 2 || body.Locations[1].Lat != 37.8 || body.Units != "kilometers" || body.Costing != "auto" {
			t.Error("wrong passenger route request")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"trip":{"status":0,"units":"kilometers","summary":{"length":8.5,"time":920.5}}}`))
	}))
	defer server.Close()
	client := valhallaClient{server.URL, server.Client(), r}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, c := context.WithTimeout(context.Background(), 4*time.Second)
			defer c()
			v, e := client.Estimate(ctx, 37.7, -122.4, 37.8, -122.5)
			if e != nil || v.DistanceKM != 8.5 || v.DurationSeconds != 920.5 || v.CalculatedAt == "" {
				t.Errorf("estimate=%+v err=%v", v, e)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("upstream calls=%d", calls.Load())
	}
}
func TestValhallaNeverReplacesInvalidResponsesWithSimulatedRoutes(t *testing.T) {
	r := routeRedis(t)
	for _, body := range []string{`{"trip":{"status":0,"units":"kilometers","summary":{"length":1}}}`, `{"trip":{"status":0,"units":"miles","summary":{"length":1,"time":2}}}`, `{"trip":{"status":0,"units":"kilometers","summary":{"length":-1,"time":2}}}`, `{"error":"no path"}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { w.Write([]byte(body)) }))
		client := valhallaClient{server.URL, server.Client(), r}
		_, e := client.Estimate(context.Background(), 37, -122, 38, -123)
		server.Close()
		if e == nil {
			t.Fatalf("accepted invalid response %s", body)
		}
	}
}
