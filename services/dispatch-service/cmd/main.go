package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
	"github.com/metroride/metroride/shared/pkg/reliability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var (
	dispatchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "metroride_dispatch_latency_seconds",
		Help:    "Ride assignment latency from event receipt to assignment emission.",
		Buckets: prometheus.DefBuckets,
	})
	assignmentFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "metroride_assignment_failures_total",
		Help: "Total failed ride assignments.",
	})
	ridesAssigned = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "metroride_rides_assigned_total",
		Help: "Total rides successfully assigned by dispatch-service.",
	})
)

type routingRequest struct {
	PickupLat float64 `json:"pickup_lat"`
	PickupLng float64 `json:"pickup_lng"`
}

type routingResponse struct {
	DriverID   string  `json:"driver_id"`
	DistanceKM float64 `json:"distance_km"`
	ETASeconds int     `json:"eta_seconds"`
}

type dispatcher struct {
	cfg    config.Config
	log    *slog.Logger
	db     *pgxpool.Pool
	rdb    *redis.Client
	client *http.Client
}

func main() {
	metrics.RegisterCommon()
	prometheus.MustRegister(dispatchLatency, assignmentFailures, ridesAssigned)
	cfg := config.Load("dispatch-service", ":8082")
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

	d := &dispatcher{
		cfg:    cfg,
		log:    log,
		db:     db,
		rdb:    rdb,
		client: &http.Client{Timeout: 3 * time.Second},
	}

	if err := d.ensureConsumerGroup(ctx); err != nil {
		log.Error("ensure consumer group failed", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go d.consume(ctx)

	mux := httpx.CommonMuxWithReadiness(log, map[string]httpx.ReadinessCheck{
		"postgres":            d.checkPostgres,
		"redis":               d.checkRedis,
		"ride_request_stream": d.checkRideRequestStream,
		"routing_service":     httpx.CheckHTTP(cfg.RoutingServiceURL+"/readyz", d.client),
	})
	server := httpx.NewServer(cfg.HTTPAddr, mux)
	go func() {
		log.Info("dispatch-service listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	httpx.Shutdown(shutdownCtx, server, log)
}

func (d *dispatcher) ensureConsumerGroup(ctx context.Context) error {
	redisCtx, cancel := reliability.WithRedisTimeout(ctx)
	defer cancel()
	return ensureGroup(redisCtx, d.rdb, events.StreamRideRequests, d.cfg.ConsumerGroup)
}

func (d *dispatcher) consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		readCtx, readCancel := reliability.WithRedisTimeout(ctx)
		result, err := d.rdb.XReadGroup(readCtx, &redis.XReadGroupArgs{
			Group:    d.cfg.ConsumerGroup,
			Consumer: d.cfg.ConsumerName,
			Streams:  []string{events.StreamRideRequests, ">"},
			Count:    10,
			Block:    time.Second,
		}).Result()
		readCancel()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			metrics.StreamConsumeErrors.WithLabelValues("dispatch-service", events.StreamRideRequests).Inc()
			metrics.DependencyErrors.WithLabelValues("dispatch-service", "redis").Inc()
			d.log.Error("read ride request stream failed", "error", err)
			continue
		}
		for _, stream := range result {
			for _, message := range stream.Messages {
				if err := reliability.Retry(ctx, reliability.MaxRetryAttempts, reliability.InitialRetryDelay, func(attemptCtx context.Context) error {
					return d.handleMessage(attemptCtx, message)
				}); err != nil {
					assignmentFailures.Inc()
					d.log.Error("dispatch message failed after retries", "error", err, "message_id", message.ID)
					if dlqErr := d.publishDeadLetter(ctx, message, err); dlqErr != nil {
						d.log.Error("publish dead-letter event failed", "error", dlqErr, "message_id", message.ID)
						continue
					}
					ackCtx, ackCancel := reliability.WithRedisTimeout(ctx)
					if ackErr := d.rdb.XAck(ackCtx, events.StreamRideRequests, d.cfg.ConsumerGroup, message.ID).Err(); ackErr != nil {
						metrics.DependencyErrors.WithLabelValues("dispatch-service", "redis").Inc()
						d.log.Error("ack dead-lettered ride request failed", "error", ackErr, "message_id", message.ID)
					}
					ackCancel()
					continue
				}
				ackCtx, ackCancel := reliability.WithRedisTimeout(ctx)
				if err := d.rdb.XAck(ackCtx, events.StreamRideRequests, d.cfg.ConsumerGroup, message.ID).Err(); err != nil {
					metrics.DependencyErrors.WithLabelValues("dispatch-service", "redis").Inc()
					d.log.Error("ack ride request failed", "error", err, "message_id", message.ID)
				}
				ackCancel()
			}
		}
	}
}

func (d *dispatcher) handleMessage(ctx context.Context, message redis.XMessage) error {
	start := time.Now()
	envelope, err := events.DecodeEnvelope(message)
	if err != nil {
		return err
	}
	if envelope.Type != events.TypeRideRequested {
		return nil
	}
	payload, err := events.DecodePayload[events.RideRequested](envelope)
	if err != nil {
		return err
	}

	alreadyAssigned, err := d.rideAlreadyAssigned(ctx, payload.RideID)
	if err != nil {
		return err
	}
	if alreadyAssigned {
		d.log.Info("duplicate ride request skipped", "event_type", envelope.Type, "ride_id", payload.RideID)
		return nil
	}

	route, err := d.findNearestDriver(ctx, payload)
	if err != nil {
		return err
	}
	assignmentID := uuid.NewString()
	now := time.Now().UTC()
	dbCtx, dbCancel := reliability.WithPostgresTimeout(ctx)
	defer dbCancel()
	tx, err := d.db.Begin(dbCtx)
	if err != nil {
		metrics.DependencyErrors.WithLabelValues("dispatch-service", "postgres").Inc()
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	commandTag, err := tx.Exec(dbCtx, `
		update rides
		set driver_id = $1, status = 'assigned', assigned_at = $2, updated_at = $2
		where id = $3 and status = 'requested'
	`, route.DriverID, now, payload.RideID)
	if err != nil {
		metrics.DependencyErrors.WithLabelValues("dispatch-service", "postgres").Inc()
		return err
	}
	if commandTag.RowsAffected() == 0 {
		d.log.Info("ride already assigned before update", "event_type", envelope.Type, "ride_id", payload.RideID)
		return nil
	}
	_, err = tx.Exec(dbCtx, `
		insert into ride_assignments (id, ride_id, driver_id, distance_km, eta_seconds, created_at)
		values ($1, $2, $3, $4, $5, $6)
	`, assignmentID, payload.RideID, route.DriverID, route.DistanceKM, route.ETASeconds, now)
	if err != nil {
		metrics.DependencyErrors.WithLabelValues("dispatch-service", "postgres").Inc()
		return err
	}
	if err := tx.Commit(dbCtx); err != nil {
		metrics.DependencyErrors.WithLabelValues("dispatch-service", "postgres").Inc()
		return err
	}

	assignment := events.RideAssigned{
		RideID:       payload.RideID,
		RiderID:      payload.RiderID,
		DriverID:     route.DriverID,
		DistanceKM:   route.DistanceKM,
		ETASeconds:   route.ETASeconds,
		AssignmentID: assignmentID,
	}
	out, err := events.NewEnvelope(uuid.NewString(), events.TypeRideAssigned, "dispatch-service", payload.RideID, assignment)
	if err != nil {
		return err
	}
	if _, err := d.publishWithRetry(ctx, events.StreamRideAssignments, out); err != nil {
		return err
	}
	if _, err := d.publishWithRetry(ctx, events.StreamRideNotifications, out); err != nil {
		return err
	}
	ridesAssigned.Inc()
	dispatchLatency.Observe(time.Since(start).Seconds())
	d.log.Info("ride assigned", "event_type", events.TypeRideAssigned, "ride_id", payload.RideID, "driver_id", route.DriverID, "eta_seconds", route.ETASeconds)
	return nil
}

func (d *dispatcher) findNearestDriver(ctx context.Context, ride events.RideRequested) (routingResponse, error) {
	body, err := json.Marshal(routingRequest{PickupLat: ride.PickupLat, PickupLng: ride.PickupLng})
	if err != nil {
		return routingResponse{}, err
	}
	var out routingResponse
	err = reliability.Retry(ctx, reliability.MaxRetryAttempts, reliability.InitialRetryDelay, func(attemptCtx context.Context) error {
		routingCtx, cancel := context.WithTimeout(attemptCtx, reliability.RoutingTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(routingCtx, http.MethodPost, d.cfg.RoutingServiceURL+"/v1/routes/nearest-driver", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := d.client.Do(req)
		if err != nil {
			metrics.DependencyErrors.WithLabelValues("dispatch-service", "routing-service").Inc()
			d.log.Error("routing request failed", "error", err, "event_type", events.TypeRideRequested, "ride_id", ride.RideID)
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			metrics.DependencyErrors.WithLabelValues("dispatch-service", "routing-service").Inc()
			return errors.New("routing-service returned non-200 response")
		}
		return json.NewDecoder(resp.Body).Decode(&out)
	})
	if err != nil {
		return routingResponse{}, err
	}
	if out.DriverID == "" {
		return routingResponse{}, errors.New("routing-service returned empty driver_id")
	}
	return out, nil
}

func (d *dispatcher) rideAlreadyAssigned(ctx context.Context, rideID string) (bool, error) {
	dbCtx, cancel := reliability.WithPostgresTimeout(ctx)
	defer cancel()
	var status string
	var driverID *string
	err := d.db.QueryRow(dbCtx, `select status, driver_id from rides where id = $1`, rideID).Scan(&status, &driverID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		metrics.DependencyErrors.WithLabelValues("dispatch-service", "postgres").Inc()
		return false, err
	}
	return status != "requested" || driverID != nil, nil
}

func (d *dispatcher) publishWithRetry(ctx context.Context, stream string, envelope events.Envelope) (string, error) {
	var streamID string
	err := reliability.Retry(ctx, reliability.MaxRetryAttempts, reliability.InitialRetryDelay, func(attemptCtx context.Context) error {
		redisCtx, cancel := reliability.WithRedisTimeout(attemptCtx)
		defer cancel()
		id, err := events.Publish(redisCtx, d.rdb, stream, envelope)
		if err != nil {
			metrics.DependencyErrors.WithLabelValues("dispatch-service", "redis").Inc()
			return err
		}
		streamID = id
		return nil
	})
	return streamID, err
}

func (d *dispatcher) publishDeadLetter(ctx context.Context, message redis.XMessage, cause error) error {
	envelope, err := events.DecodeEnvelope(message)
	if err != nil {
		envelope = events.Envelope{ID: message.ID, Type: "decode_failed", Source: "unknown"}
	}
	rideID := envelope.CorrelationID
	if payload, decodeErr := events.DecodePayload[events.RideRequested](envelope); decodeErr == nil && payload.RideID != "" {
		rideID = payload.RideID
	}
	payload := events.DeadLetter{
		OriginalEventID:   envelope.ID,
		OriginalEventType: envelope.Type,
		RideID:            rideID,
		Error:             cause.Error(),
		Service:           "dispatch-service",
		FailedAt:          time.Now().UTC().Format(time.RFC3339Nano),
	}
	out, err := events.NewEnvelope(uuid.NewString(), "dead_lettered", "dispatch-service", rideID, payload)
	if err != nil {
		return err
	}
	_, err = d.publishWithRetry(ctx, events.StreamDeadLetter, out)
	return err
}

func (d *dispatcher) checkPostgres(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return d.db.Ping(checkCtx)
}

func (d *dispatcher) checkRedis(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return d.rdb.Ping(checkCtx).Err()
}

func (d *dispatcher) checkRideRequestStream(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return ensureGroup(checkCtx, d.rdb, events.StreamRideRequests, d.cfg.ConsumerGroup)
}

func ensureGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}
