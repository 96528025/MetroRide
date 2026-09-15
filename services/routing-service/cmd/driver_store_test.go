package main

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/events"
	"os"
	"testing"
	"time"
)

func TestDriverStateIsSharedPersistentAndReservationsSurviveLocationUpdates(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN enables persistent driver tests")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	schema := "driver_test_" + uuid.NewString()[:8]
	if _, e = admin.Exec(ctx, "create schema "+schema); e != nil {
		t.Fatal(e)
	}
	defer admin.Exec(ctx, "drop schema "+schema+" cascade")
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	_, e = pool.Exec(ctx, `create table driver_positions(driver_id text primary key,latitude double precision,longitude double precision,available boolean,updated_at timestamptz);create table driver_reservations(driver_id text primary key,ride_id text unique)`)
	if e != nil {
		t.Fatal(e)
	}
	a, b := postgresDrivers{pool, 15 * time.Second}, postgresDrivers{pool, 15 * time.Second}
	now := time.Now().UTC()
	p := events.DriverLocationUpdated{DriverID: "driver", Latitude: 37, Longitude: -122, Available: true, UpdatedAt: now.Format(time.RFC3339Nano)}
	if e = a.Save(ctx, p); e != nil {
		t.Fatal(e)
	}
	got, e := b.Available(ctx)
	if e != nil || len(got) != 1 {
		t.Fatalf("shared read %v %v", got, e)
	}
	_, e = pool.Exec(ctx, `insert into driver_reservations values ('driver','ride')`)
	if e != nil {
		t.Fatal(e)
	}
	p.UpdatedAt = now.Add(time.Second).Format(time.RFC3339Nano)
	if e = b.Save(ctx, p); e != nil {
		t.Fatal(e)
	}
	got, e = a.Available(ctx)
	if e != nil || len(got) != 0 {
		t.Fatal("location update made a reserved driver available")
	}
	// A new store instance reads the same reservation; stale deliveries cannot roll back position.
	c := postgresDrivers{pool, 15 * time.Second}
	p.Latitude = 38
	p.UpdatedAt = now.Add(-time.Minute).Format(time.RFC3339Nano)
	if e = c.Save(ctx, p); e != nil {
		t.Fatal(e)
	}
	pool.Exec(ctx, `delete from driver_reservations where driver_id='driver'`)
	got, e = c.Available(ctx)
	if e != nil || got["driver"].Latitude != 37 {
		t.Fatalf("stale delivery replaced position: %v %v", got, e)
	}
	pool.Exec(ctx, `update driver_positions set updated_at=now()-interval '16 seconds'`)
	got, e = c.Available(ctx)
	if e != nil || len(got) != 0 {
		t.Fatal("stale driver was eligible")
	}
}
