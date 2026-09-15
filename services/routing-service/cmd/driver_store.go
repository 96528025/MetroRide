package main

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/reliability"
	"math"
	"time"
)

type driverStore interface {
	Available(context.Context) (map[string]driver, error)
	Save(context.Context, events.DriverLocationUpdated) error
	Ready(context.Context) error
}
type postgresDrivers struct {
	db     *pgxpool.Pool
	maxAge time.Duration
}

func (s *postgresDrivers) Ready(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	var version int
	return s.db.QueryRow(checkCtx, "select version from core_schema_migrations where version=1").Scan(&version)
}
func validPoint(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
}
func (s *postgresDrivers) Save(ctx context.Context, p events.DriverLocationUpdated) error {
	at, e := time.Parse(time.RFC3339Nano, p.UpdatedAt)
	if e != nil || p.DriverID == "" || !validPoint(p.Latitude, p.Longitude) || at.After(time.Now().Add(5*time.Second)) {
		return reliability.Permanent(errors.New("invalid driver location"))
	}
	c, cancel := reliability.WithPostgresTimeout(ctx)
	defer cancel()
	_, e = s.db.Exec(c, `insert into driver_positions(driver_id,latitude,longitude,available,updated_at) values($1,$2,$3,$4,$5)
 on conflict(driver_id) do update set latitude=excluded.latitude,longitude=excluded.longitude,available=excluded.available,updated_at=excluded.updated_at
 where driver_positions.updated_at<excluded.updated_at`, p.DriverID, p.Latitude, p.Longitude, p.Available, at)
	return e
}
func (s *postgresDrivers) Available(ctx context.Context) (map[string]driver, error) {
	c, cancel := reliability.WithPostgresTimeout(ctx)
	defer cancel()
	rows, e := s.db.Query(c, `select p.driver_id,p.latitude,p.longitude,p.available,p.updated_at from driver_positions p
 where p.available and p.updated_at>=clock_timestamp()-($1*interval '1 second')
 and not exists(select 1 from driver_reservations r where r.driver_id=p.driver_id)`, s.maxAge.Seconds())
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[string]driver{}
	for rows.Next() {
		var d driver
		if e = rows.Scan(&d.ID, &d.Latitude, &d.Longitude, &d.Available, &d.UpdatedAt); e != nil {
			return nil, e
		}
		out[d.ID] = d
	}
	return out, rows.Err()
}
