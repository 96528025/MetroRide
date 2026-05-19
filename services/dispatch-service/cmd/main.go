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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/httpx"
	"github.com/metroride/metroride/shared/pkg/logging"
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
	prometheus.MustRegister(dispatchLatency, assignmentFailures)
	cfg := config.Load("dispatch-service", ":8082")
	log := logging.New(cfg.ServiceName)
	ctx := context.Background()

	db, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
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

	mux := httpx.CommonMux(log)
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
	err := d.rdb.XGroupCreateMkStream(ctx, events.StreamRideRequests, d.cfg.ConsumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (d *dispatcher) consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := d.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    d.cfg.ConsumerGroup,
			Consumer: d.cfg.ConsumerName,
			Streams:  []string{events.StreamRideRequests, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			d.log.Error("read ride request stream failed", "error", err)
			continue
		}
		for _, stream := range result {
			for _, message := range stream.Messages {
				if err := d.handleMessage(ctx, message); err != nil {
					assignmentFailures.Inc()
					d.log.Error("dispatch message failed", "error", err, "message_id", message.ID)
					continue
				}
				if err := d.rdb.XAck(ctx, events.StreamRideRequests, d.cfg.ConsumerGroup, message.ID).Err(); err != nil {
					d.log.Error("ack ride request failed", "error", err, "message_id", message.ID)
				}
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

	route, err := d.findNearestDriver(ctx, payload)
	if err != nil {
		return err
	}
	assignmentID := uuid.NewString()
	now := time.Now().UTC()
	_, err = d.db.Exec(ctx, `
		update rides
		set driver_id = $1, status = 'assigned', assigned_at = $2, updated_at = $2
		where id = $3
	`, route.DriverID, now, payload.RideID)
	if err != nil {
		return err
	}
	_, err = d.db.Exec(ctx, `
		insert into ride_assignments (id, ride_id, driver_id, distance_km, eta_seconds, created_at)
		values ($1, $2, $3, $4, $5, $6)
	`, assignmentID, payload.RideID, route.DriverID, route.DistanceKM, route.ETASeconds, now)
	if err != nil {
		return err
	}

	assigned := events.RideAssigned{
		RideID:       payload.RideID,
		RiderID:      payload.RiderID,
		DriverID:     route.DriverID,
		DistanceKM:   route.DistanceKM,
		ETASeconds:   route.ETASeconds,
		AssignmentID: assignmentID,
	}
	out, err := events.NewEnvelope(uuid.NewString(), events.TypeRideAssigned, "dispatch-service", payload.RideID, assigned)
	if err != nil {
		return err
	}
	if _, err := events.Publish(ctx, d.rdb, events.StreamRideAssignments, out); err != nil {
		return err
	}
	if _, err := events.Publish(ctx, d.rdb, events.StreamRideNotifications, out); err != nil {
		return err
	}
	dispatchLatency.Observe(time.Since(start).Seconds())
	d.log.Info("ride assigned", "ride_id", payload.RideID, "driver_id", route.DriverID, "eta_seconds", route.ETASeconds)
	return nil
}

func (d *dispatcher) findNearestDriver(ctx context.Context, ride events.RideRequested) (routingResponse, error) {
	body, err := json.Marshal(routingRequest{PickupLat: ride.PickupLat, PickupLng: ride.PickupLng})
	if err != nil {
		return routingResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.cfg.RoutingServiceURL+"/v1/routes/nearest-driver", bytes.NewReader(body))
	if err != nil {
		return routingResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return routingResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return routingResponse{}, errors.New("routing-service returned non-200 response")
	}
	var out routingResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return routingResponse{}, err
	}
	if out.DriverID == "" {
		return routingResponse{}, errors.New("routing-service returned empty driver_id")
	}
	return out, nil
}
