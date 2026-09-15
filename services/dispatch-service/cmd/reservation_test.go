package main

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/redis/go-redis/v9"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentAssignmentsReserveOneDriverAndEndedReplayDoesNotReserveAgain(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN enables concurrent reservation checks")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	schema := "dispatch_test_" + uuid.NewString()[:8]
	if _, e = admin.Exec(ctx, "create schema "+schema); e != nil {
		t.Fatal(e)
	}
	defer admin.Exec(ctx, "drop schema "+schema+" cascade")
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	db, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	sql, e := os.ReadFile("../../../infrastructure/docker/postgres/init.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(ctx, string(sql)); e != nil {
		t.Fatal(e)
	}
	ids := []string{uuid.NewString(), uuid.NewString()}
	messages := make([]redis.XMessage, 2)
	for i, id := range ids {
		_, e = db.Exec(ctx, `insert into rides(id,rider_id,pickup_lat,pickup_lng,dropoff_lat,dropoff_lng,status,created_at,updated_at) values($1,'rider',37.775,-122.419,37.789,-122.401,'requested',now(),now())`, id)
		if e != nil {
			t.Fatal(e)
		}
		env, e := events.NewEnvelope(uuid.NewString(), events.TypeRideRequested, "test", id, events.RideRequested{RideID: id, RiderID: "rider", PickupLat: 37.775, PickupLng: -122.419, DropoffLat: 37.789, DropoffLng: -122.401})
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := json.Marshal(env)
		messages[i] = redis.XMessage{ID: "1-0", Values: map[string]any{"event": string(raw)}}
	}
	db.Exec(ctx, `insert into driver_positions values('driver',37.77,-122.41,true,now())`)
	var calls atomic.Int32
	ready := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 2 {
			close(ready)
		}
		select {
		case <-ready:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"driver_id":"driver","distance_km":1.25,"eta_seconds":180,"trip_distance_km":8.5,"trip_duration_seconds":920.5,"route_provider":"test-fixture","route_calculated_at":"2026-09-15T00:00:00Z"}`))
	}))
	defer server.Close()
	d := dispatcher{cfg: config.Config{RoutingServiceURL: server.URL}, db: db, client: server.Client(), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	results := make(chan error, 2)
	for _, m := range messages {
		go func(m redis.XMessage) {
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			results <- d.handleMessage(c, m)
		}(m)
	}
	successes := 0
	for i := 0; i < 2; i++ {
		if <-results == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful competitors=%d", successes)
	}
	var reserved, assigned, outgoing int
	db.QueryRow(ctx, `select (select count(*) from driver_reservations),(select count(*) from ride_assignments),(select count(*) from event_outbox)`).Scan(&reserved, &assigned, &outgoing)
	if reserved != 1 || assigned != 1 || outgoing != 2 {
		t.Fatalf("reservation/assignment/outbox=%d/%d/%d", reserved, assigned, outgoing)
	}
	var winner string
	db.QueryRow(ctx, `select ride_id from driver_reservations`).Scan(&winner)
	// Emulate the committed end transition, then replay the original request.
	db.Exec(ctx, `update rides set status='completed' where id=$1`, winner)
	db.Exec(ctx, `delete from driver_reservations where ride_id=$1`, winner)
	for i, id := range ids {
		if id == winner {
			if e = d.handleMessage(ctx, messages[i]); e != nil {
				t.Fatal(e)
			}
		}
	}
	db.QueryRow(ctx, `select count(*) from driver_reservations`).Scan(&reserved)
	if reserved != 0 {
		t.Fatal("ended request reserved driver again")
	}
}
