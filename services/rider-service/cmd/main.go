package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/httpx"
	"github.com/metroride/metroride/shared/pkg/logging"
	"github.com/metroride/metroride/shared/pkg/metrics"
	"github.com/metroride/metroride/shared/pkg/reliability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var rideRequests = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "metroride_ride_requests_total",
	Help: "Total number of ride requests accepted by rider-service.",
})

type createRideRequest struct {
	RiderID    string  `json:"rider_id"`
	PickupLat  float64 `json:"pickup_lat"`
	PickupLng  float64 `json:"pickup_lng"`
	DropoffLat float64 `json:"dropoff_lat"`
	DropoffLng float64 `json:"dropoff_lng"`
}

type service struct {
	log *slog.Logger
	db  *pgxpool.Pool
	rdb *redis.Client
}

func main() {
	metrics.RegisterCommon()
	prometheus.MustRegister(rideRequests)
	cfg := config.Load("rider-service", ":8080")
	log := logging.New(cfg.ServiceName)
	ctx := context.Background()

	db, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  reliability.RedisTimeout,
		ReadTimeout:  reliability.RedisTimeout,
		WriteTimeout: reliability.RedisTimeout,
	})
	defer func() { _ = rdb.Close() }()

	svc := &service{log: log, db: db, rdb: rdb}
	mux := httpx.CommonMuxWithReadiness(log, map[string]httpx.ReadinessCheck{
		"postgres": svc.checkPostgres,
		"redis":    svc.checkRedis,
	})
	mux.HandleFunc("POST /v1/rides", svc.createRide)
	mux.HandleFunc("GET /v1/rides/{ride_id}", svc.getRide)

	server := httpx.NewServer(cfg.HTTPAddr, mux)
	go func() {
		log.Info("rider-service listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	waitForShutdown(cfg, log, server)
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

	now := time.Now().UTC()
	rideID := uuid.NewString()
	dbCtx, dbCancel := reliability.WithPostgresTimeout(r.Context())
	defer dbCancel()
	_, err := s.db.Exec(dbCtx, `
		insert into rides (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, status, created_at, updated_at)
		values ($1, $2, $3, $4, $5, $6, 'requested', $7, $7)
	`, rideID, req.RiderID, req.PickupLat, req.PickupLng, req.DropoffLat, req.DropoffLng, now)
	if err != nil {
		metrics.DependencyErrors.WithLabelValues("rider-service", "postgres").Inc()
		s.log.Error("persist ride request failed", "error", err, "ride_id", rideID)
		httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create ride"})
		return
	}

	payload := events.RideRequested{
		RideID:      rideID,
		RiderID:     req.RiderID,
		PickupLat:   req.PickupLat,
		PickupLng:   req.PickupLng,
		DropoffLat:  req.DropoffLat,
		DropoffLng:  req.DropoffLng,
		RequestedAt: now.Format(time.RFC3339Nano),
	}
	envelope, err := events.NewEnvelope(uuid.NewString(), events.TypeRideRequested, "rider-service", rideID, payload)
	if err != nil {
		httpx.RespondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to encode event"})
		return
	}
	redisCtx, redisCancel := reliability.WithRedisTimeout(r.Context())
	defer redisCancel()
	streamID, err := events.Publish(redisCtx, s.rdb, events.StreamRideRequests, envelope)
	if err != nil {
		metrics.DependencyErrors.WithLabelValues("rider-service", "redis").Inc()
		s.log.Error("publish ride request failed", "error", err, "event_type", events.TypeRideRequested, "ride_id", rideID)
		httpx.RespondJSON(w, http.StatusAccepted, map[string]any{"ride_id": rideID, "status": "requested", "warning": "event publish failed"})
		return
	}

	rideRequests.Inc()
	s.log.Info("ride request accepted", "ride_id", rideID, "rider_id", req.RiderID, "stream_id", streamID)
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

func (s *service) checkPostgres(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return s.db.Ping(checkCtx)
}

func (s *service) checkRedis(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return s.rdb.Ping(checkCtx).Err()
}

func waitForShutdown(cfg config.Config, log *slog.Logger, server *http.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	httpx.Shutdown(ctx, server, log)
}
