package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/httpx"
	"github.com/metroride/metroride/shared/pkg/logging"
	"github.com/metroride/metroride/shared/pkg/metrics"
	"github.com/metroride/metroride/shared/pkg/outbox"
	"github.com/metroride/metroride/shared/pkg/reliability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var (
	rideRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "metroride_ride_requests_total",
		Help: "Total number of ride requests accepted by rider-service.",
	})
	ridesCompleted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "metroride_rides_completed_total",
		Help: "Total number of rides moved to completed by rider-service.",
	})
)

type createRideRequest struct {
	RiderID    string   `json:"rider_id"`
	PickupLat  *float64 `json:"pickup_lat"`
	PickupLng  *float64 `json:"pickup_lng"`
	DropoffLat *float64 `json:"dropoff_lat"`
	DropoffLng *float64 `json:"dropoff_lng"`
}

type service struct {
	log *slog.Logger
	db  *pgxpool.Pool
}

func riderReadinessChecks(checkPostgres httpx.ReadinessCheck) map[string]httpx.ReadinessCheck {
	return map[string]httpx.ReadinessCheck{
		"postgres": checkPostgres,
	}
}

func main() {
	metrics.RegisterCommon()
	outbox.RegisterMetrics()
	prometheus.MustRegister(rideRequests, ridesCompleted)
	cfg := config.Load("rider-service", ":8080")
	log := logging.New(cfg.ServiceName)
	ctx, cancel := context.WithCancel(context.Background())

	db, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  reliability.RedisTimeout,
		ReadTimeout:  reliability.RedisTimeout,
		WriteTimeout: reliability.RedisTimeout,
	})
	schemaCtx, schemaCancel := reliability.WithPostgresTimeout(ctx)
	err = outbox.EnsureSchema(schemaCtx, db)
	schemaCancel()
	if err != nil {
		log.Error("ensure outbox schema failed", "error", err)
		os.Exit(1)
	}
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		outbox.NewRelay(cfg.ServiceName, log, db, rdb).Run(ctx)
	}()

	svc := &service{log: log, db: db}
	mux := httpx.CommonMuxWithReadiness(log, riderReadinessChecks(svc.checkPostgres))
	mux.HandleFunc("POST /v1/rides", svc.createRide)
	mux.HandleFunc("GET /v1/rides/{ride_id}", svc.getRide)
	mux.HandleFunc("POST /v1/rides/{ride_id}/complete", svc.completeRide)
	mux.HandleFunc("POST /v1/rides/{ride_id}/cancel", svc.cancelRide)

	server := httpx.NewServer(cfg.HTTPAddr, mux)
	go func() {
		log.Info("rider-service listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	waitForShutdown(cfg, log, server)
	cancel()
	<-relayDone
	if err := rdb.Close(); err != nil {
		log.Error("close redis client failed", "error", err)
	}
	db.Close()
}

func (s *service) createRide(w http.ResponseWriter, r *http.Request) {
	var req createRideRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.RespondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.RiderID == "" {
		httpx.RespondJSON(w, http.StatusBadRequest, map[string]string{"error": "rider_id is required"})
		return
	}

	valid := func(lat, lon *float64) bool {
		return lat != nil && lon != nil && !math.IsNaN(*lat) && !math.IsNaN(*lon) && *lat >= -90 && *lat <= 90 && *lon >= -180 && *lon <= 180
	}
	if !valid(req.PickupLat, req.PickupLng) || !valid(req.DropoffLat, req.DropoffLng) {
		httpx.RespondJSON(w, http.StatusBadRequest, map[string]string{"error": "valid pickup and dropoff coordinates are required"})
		return
	}

	now := time.Now().UTC()
	rideID := uuid.NewString()
	payload := events.RideRequested{
		RideID:      rideID,
		RiderID:     req.RiderID,
		PickupLat:   *req.PickupLat,
		PickupLng:   *req.PickupLng,
		DropoffLat:  *req.DropoffLat,
		DropoffLng:  *req.DropoffLng,
		RequestedAt: now.Format(time.RFC3339Nano),
	}
	envelope, err := events.NewEnvelope(uuid.NewString(), events.TypeRideRequested, "rider-service", rideID, payload)
	if err != nil {
		httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to encode event"})
		return
	}
	dbCtx, dbCancel := reliability.WithPostgresTimeout(r.Context())
	defer dbCancel()
	tx, err := s.db.Begin(dbCtx)
	if err != nil {
		metrics.DependencyErrors.WithLabelValues("rider-service", "postgres").Inc()
		s.log.Error("begin ride request transaction failed", "error", err, "ride_id", rideID)
		httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create ride"})
		return
	}
	defer func() {
		c, cancel := reliability.WithPostgresTimeout(context.Background())
		defer cancel()
		_ = tx.Rollback(c)
	}()
	_, err = tx.Exec(dbCtx, `
		insert into rides (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, status, created_at, updated_at)
		values ($1, $2, $3, $4, $5, $6, 'requested', $7, $7)
	`, rideID, req.RiderID, req.PickupLat, req.PickupLng, req.DropoffLat, req.DropoffLng, now)
	if err != nil {
		metrics.DependencyErrors.WithLabelValues("rider-service", "postgres").Inc()
		s.log.Error("persist ride request failed", "error", err, "ride_id", rideID)
		httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create ride"})
		return
	}
	if err := outbox.Enqueue(dbCtx, tx, events.StreamRideRequests, envelope); err != nil {
		metrics.DependencyErrors.WithLabelValues("rider-service", "postgres").Inc()
		s.log.Error("enqueue ride request failed", "error", err, "ride_id", rideID)
		httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create ride"})
		return
	}
	if err := tx.Commit(dbCtx); err != nil {
		metrics.DependencyErrors.WithLabelValues("rider-service", "postgres").Inc()
		s.log.Error("commit ride request failed", "error", err, "ride_id", rideID)
		httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create ride"})
		return
	}

	rideRequests.Inc()
	s.log.Info("ride request accepted", "ride_id", rideID, "rider_id", req.RiderID, "event_id", envelope.ID)
	httpx.RespondJSON(w, http.StatusAccepted, map[string]any{"ride_id": rideID, "status": "requested", "event_id": envelope.ID})
}

func (s *service) getRide(w http.ResponseWriter, r *http.Request) {
	rideID := r.PathValue("ride_id")
	var response struct {
		ID        string     `json:"id"`
		RiderID   string     `json:"rider_id"`
		DriverID  *string    `json:"driver_id,omitempty"`
		Status    string     `json:"status"`
		CreatedAt time.Time  `json:"created_at"`
		UpdatedAt time.Time  `json:"updated_at"`
		Assigned  *time.Time `json:"assigned_at,omitempty"`
	}
	dbCtx, dbCancel := reliability.WithPostgresTimeout(r.Context())
	defer dbCancel()
	err := s.db.QueryRow(dbCtx, `
		select id, rider_id, driver_id, status, created_at, updated_at, assigned_at
		from rides where id = $1
	`, rideID).Scan(&response.ID, &response.RiderID, &response.DriverID, &response.Status, &response.CreatedAt, &response.UpdatedAt, &response.Assigned)
	if err != nil {
		httpx.RespondJSON(w, http.StatusNotFound, map[string]string{"error": "ride not found"})
		return
	}
	httpx.RespondJSON(w, http.StatusOK, response)
}

// completeRide moves an assigned ride to completed and enqueues the
// ride_completed event in the same transaction, the way createRide commits
// the ride with its ride_requested event. The conditional update
// (status = 'assigned') is the whole concurrency control: concurrent
// completions of one ride block on the row lock, re-evaluate the predicate
// after the first commits, touch no row, and are answered 409. The
// assignment lookup refuses anything but exactly one row so a completion is
// never published for a ride whose assignment state is inconsistent.
func (s *service) completeRide(w http.ResponseWriter, r *http.Request) {
	rideID := r.PathValue("ride_id")
	if _, err := uuid.Parse(rideID); err != nil {
		// rides.id is a uuid column; a value that is not one cannot name a ride.
		httpx.RespondJSON(w, http.StatusNotFound, map[string]string{"error": "ride not found"})
		return
	}
	now := time.Now().UTC()
	dbCtx, dbCancel := reliability.WithPostgresTimeout(r.Context())
	defer dbCancel()
	tx, err := s.db.Begin(dbCtx)
	if err != nil {
		s.completionFailed(w, "begin ride completion transaction failed", err, rideID)
		return
	}
	defer func() {
		c, cancel := reliability.WithPostgresTimeout(context.Background())
		defer cancel()
		_ = tx.Rollback(c)
	}()

	tag, err := tx.Exec(dbCtx, `
		update rides set status = 'completed', updated_at = $1
		where id = $2 and status = 'assigned'
	`, now, rideID)
	if err != nil {
		s.completionFailed(w, "complete ride failed", err, rideID)
		return
	}
	if tag.RowsAffected() == 0 {
		var status string
		lookupErr := tx.QueryRow(dbCtx, `select status from rides where id = $1`, rideID).Scan(&status)
		code, body := completionRejection(status, lookupErr)
		if code == http.StatusInternalServerError {
			metrics.DependencyErrors.WithLabelValues("rider-service", "postgres").Inc()
			s.log.Error("read ride status after rejected completion failed", "error", lookupErr, "ride_id", rideID)
		}
		httpx.RespondJSON(w, code, body)
		return
	}

	assignmentID, riderID, driverID, err := s.singleAssignment(dbCtx, tx, rideID)
	if err != nil {
		if errors.Is(err, errAssignmentStateInconsistent) {
			s.log.Error("ride assignment state inconsistent; completion rolled back", "error", err, "ride_id", rideID)
			httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "ride assignment state inconsistent"})
			return
		}
		s.completionFailed(w, "read ride assignment failed", err, rideID)
		return
	}

	payload := events.RideCompleted{
		RideID:       rideID,
		RiderID:      riderID,
		DriverID:     driverID,
		AssignmentID: assignmentID,
		CompletedAt:  now.Format(time.RFC3339Nano),
	}
	envelope, err := events.NewEnvelope(uuid.NewString(), events.TypeRideCompleted, "rider-service", rideID, payload)
	if err != nil {
		httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to encode event"})
		return
	}
	if _, err := tx.Exec(dbCtx, `delete from driver_reservations where ride_id=$1`, rideID); err != nil {
		s.completionFailed(w, "release reservation failed", err, rideID)
		return
	}
	if err := outbox.Enqueue(dbCtx, tx, events.StreamRideCompletions, envelope); err != nil {
		s.completionFailed(w, "enqueue ride completion failed", err, rideID)
		return
	}
	if err := tx.Commit(dbCtx); err != nil {
		s.completionFailed(w, "commit ride completion failed", err, rideID)
		return
	}

	ridesCompleted.Inc()
	s.log.Info("ride completed", "ride_id", rideID, "driver_id", driverID, "assignment_id", assignmentID, "event_id", envelope.ID)
	httpx.RespondJSON(w, http.StatusAccepted, map[string]any{"ride_id": rideID, "status": "completed", "event_id": envelope.ID})
}

// errAssignmentStateInconsistent is returned when a ride that the conditional
// update just moved to completed has zero or several ride_assignments rows.
// Neither can happen through dispatch-service, whose status guard allows one
// assignment per ride; either means the data is already wrong, so the
// completion is refused rather than published against a guessed assignment.
var errAssignmentStateInconsistent = errors.New("ride assignment state inconsistent")

// singleAssignment reads the ride's assignment inside the completion
// transaction and requires exactly one row. It deliberately has no LIMIT 1 and
// never picks the newest row: a ride with two assignments is a data problem
// to surface, not to paper over.
func (s *service) singleAssignment(ctx context.Context, tx pgx.Tx, rideID string) (assignmentID, riderID, driverID string, err error) {
	rows, err := tx.Query(ctx, `
		select a.id, r.rider_id, a.driver_id, r.driver_id
		from ride_assignments a
		join rides r on r.id = a.ride_id
		where a.ride_id = $1
	`, rideID)
	if err != nil {
		return "", "", "", err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		if count > 1 {
			continue
		}
		var rideDriver *string
		if err := rows.Scan(&assignmentID, &riderID, &driverID, &rideDriver); err != nil {
			return "", "", "", err
		}
		if rideDriver == nil || *rideDriver != driverID {
			return "", "", "", errAssignmentStateInconsistent
		}
	}
	if err := rows.Err(); err != nil {
		return "", "", "", err
	}
	if count != 1 {
		return "", "", "", fmt.Errorf("%w: ride %s has %d assignments, want 1", errAssignmentStateInconsistent, rideID, count)
	}
	return assignmentID, riderID, driverID, nil
}

// completionRejection maps what the completion transaction found after its
// conditional update touched no row to the HTTP response: the ride's current
// status (a 409 that names it), no such ride (404), or a failed lookup (500;
// a database error must not be reported as a missing ride).
func completionRejection(status string, lookupErr error) (int, map[string]string) {
	switch {
	case lookupErr == nil:
		return http.StatusConflict, map[string]string{"error": "ride is " + status}
	case errors.Is(lookupErr, pgx.ErrNoRows):
		return http.StatusNotFound, map[string]string{"error": "ride not found"}
	default:
		return http.StatusInternalServerError, map[string]string{"error": "failed to complete ride"}
	}
}

func (s *service) completionFailed(w http.ResponseWriter, message string, err error, rideID string) {
	metrics.DependencyErrors.WithLabelValues("rider-service", "postgres").Inc()
	s.log.Error(message, "error", err, "ride_id", rideID)
	httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to complete ride"})
}

func (s *service) checkPostgres(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return s.db.Ping(checkCtx)
}

func waitForShutdown(cfg config.Config, log *slog.Logger, server *http.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	httpx.Shutdown(ctx, server, log)
}

func (s *service) cancelRide(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("ride_id")
	if _, e := uuid.Parse(id); e != nil {
		httpx.RespondJSON(w, 404, map[string]string{"error": "ride not found"})
		return
	}
	ctx, cancel := reliability.WithPostgresTimeout(r.Context())
	defer cancel()
	tx, e := s.db.Begin(ctx)
	if e != nil {
		s.cancellationFailed(w, "begin cancellation failed", e, id)
		return
	}
	defer func() {
		rollback, c := reliability.WithPostgresTimeout(context.Background())
		defer c()
		_ = tx.Rollback(rollback)
	}()
	var status, rider string
	var driverID *string
	e = tx.QueryRow(ctx, `select status,rider_id,driver_id from rides where id=$1 for update`, id).Scan(&status, &rider, &driverID)
	if e != nil {
		code, body := completionRejection(status, e)
		httpx.RespondJSON(w, code, body)
		return
	}
	if status != "requested" && status != "assigned" {
		httpx.RespondJSON(w, 409, map[string]string{"error": "ride is " + status})
		return
	}
	payload := events.RideCancelled{RideID: id, RiderID: rider, CancelledAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if status == "assigned" {
		assignmentID, _, driver, e := s.singleAssignment(ctx, tx, id)
		if e != nil {
			s.cancellationFailed(w, "inconsistent assignment", e, id)
			return
		}
		payload.AssignmentID = assignmentID
		payload.DriverID = driver
	}
	if _, e = tx.Exec(ctx, `update rides set status='cancelled',updated_at=now() where id=$1`, id); e != nil {
		s.cancellationFailed(w, "cancel ride failed", e, id)
		return
	}
	if _, e = tx.Exec(ctx, `delete from driver_reservations where ride_id=$1`, id); e != nil {
		s.cancellationFailed(w, "release reservation failed", e, id)
		return
	}
	env, e := events.NewEnvelope(uuid.NewString(), events.TypeRideCancelled, "rider-service", id, payload)
	if e == nil {
		e = outbox.Enqueue(ctx, tx, events.StreamRideCancellations, env)
	}
	if e == nil {
		e = tx.Commit(ctx)
	}
	if e != nil {
		s.cancellationFailed(w, "commit cancellation failed", e, id)
		return
	}
	httpx.RespondJSON(w, 202, map[string]string{"ride_id": id, "status": "cancelled", "event_id": env.ID})
}

func (s *service) cancellationFailed(w http.ResponseWriter, message string, err error, rideID string) {
	metrics.DependencyErrors.WithLabelValues("rider-service", "postgres").Inc()
	s.log.Error(message, "error", err, "ride_id", rideID)
	httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to cancel ride"})
}
