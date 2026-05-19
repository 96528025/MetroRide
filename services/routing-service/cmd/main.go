package main

import (
	"context"
	"errors"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/httpx"
	"github.com/metroride/metroride/shared/pkg/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var (
	routingDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "metroride_routing_computation_seconds",
		Help:    "Latency for nearest-driver route calculations.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	})
	activeDrivers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "metroride_active_drivers",
		Help: "Drivers currently available to routing-service.",
	})
)

type driver struct {
	ID        string
	Latitude  float64
	Longitude float64
	Available bool
	UpdatedAt time.Time
}

type routingService struct {
	logDrivers sync.RWMutex
	drivers    map[string]driver
	rdb        *redis.Client
}

type nearestDriverRequest struct {
	PickupLat float64 `json:"pickup_lat"`
	PickupLng float64 `json:"pickup_lng"`
}

func main() {
	prometheus.MustRegister(routingDuration, activeDrivers)
	cfg := config.Load("routing-service", ":8083")
	log := logging.New(cfg.ServiceName)
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	svc := &routingService{
		drivers: map[string]driver{
			"driver-seed-1": {ID: "driver-seed-1", Latitude: 37.7749, Longitude: -122.4194, Available: true, UpdatedAt: time.Now().UTC()},
			"driver-seed-2": {ID: "driver-seed-2", Latitude: 37.7849, Longitude: -122.4094, Available: true, UpdatedAt: time.Now().UTC()},
			"driver-seed-3": {ID: "driver-seed-3", Latitude: 37.7649, Longitude: -122.4294, Available: true, UpdatedAt: time.Now().UTC()},
		},
		rdb: rdb,
	}
	activeDrivers.Set(3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.consumeDriverLocations(ctx, log)

	mux := httpx.CommonMux(log)
	mux.HandleFunc("POST /v1/routes/nearest-driver", svc.nearestDriver)
	server := httpx.NewServer(cfg.HTTPAddr, mux)
	go func() {
		log.Info("routing-service listening", "addr", cfg.HTTPAddr)
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

func (s *routingService) nearestDriver(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() { routingDuration.Observe(time.Since(start).Seconds()) }()

	var req nearestDriverRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.RespondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	s.logDrivers.RLock()
	candidates := make([]driver, 0, len(s.drivers))
	for _, d := range s.drivers {
		if d.Available {
			candidates = append(candidates, d)
		}
	}
	s.logDrivers.RUnlock()

	if len(candidates) == 0 {
		httpx.RespondJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no available drivers"})
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		return haversine(req.PickupLat, req.PickupLng, candidates[i].Latitude, candidates[i].Longitude) <
			haversine(req.PickupLat, req.PickupLng, candidates[j].Latitude, candidates[j].Longitude)
	})

	selected := candidates[0]
	distanceKM := haversine(req.PickupLat, req.PickupLng, selected.Latitude, selected.Longitude)
	etaSeconds := int((distanceKM / 32.0) * 3600)
	if etaSeconds < 60 {
		etaSeconds = 60
	}
	httpx.RespondJSON(w, http.StatusOK, map[string]any{
		"driver_id":    selected.ID,
		"distance_km":  math.Round(distanceKM*100) / 100,
		"eta_seconds":  etaSeconds,
		"algorithm":    "haversine-nearest-with-dijkstra-ready-graph",
		"computed_at":  time.Now().UTC(),
	})
}

func (s *routingService) consumeDriverLocations(ctx context.Context, log anyLogger) {
	group := "routing-service"
	consumer := "routing-service-1"
	_ = s.rdb.XGroupCreateMkStream(ctx, events.StreamDriverLocations, group, "0").Err()
	for {
		result, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{events.StreamDriverLocations, ">"},
			Count:    25,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			log.Error("driver location stream read failed", "error", err)
			continue
		}
		for _, stream := range result {
			for _, message := range stream.Messages {
				envelope, err := events.DecodeEnvelope(message)
				if err != nil {
					log.Error("driver location decode failed", "error", err)
					continue
				}
				if envelope.Type != events.TypeDriverLocationUpdated {
					continue
				}
				payload, err := events.DecodePayload[events.DriverLocationUpdated](envelope)
				if err != nil {
					log.Error("driver location payload decode failed", "error", err)
					continue
				}
				updatedAt, _ := time.Parse(time.RFC3339Nano, payload.UpdatedAt)
				s.logDrivers.Lock()
				s.drivers[payload.DriverID] = driver{
					ID: payload.DriverID, Latitude: payload.Latitude, Longitude: payload.Longitude,
					Available: payload.Available, UpdatedAt: updatedAt,
				}
				available := 0
				for _, d := range s.drivers {
					if d.Available {
						available++
					}
				}
				activeDrivers.Set(float64(available))
				s.logDrivers.Unlock()
				_ = s.rdb.XAck(ctx, events.StreamDriverLocations, group, message.ID).Err()
			}
		}
	}
}

type anyLogger interface {
	Error(msg string, args ...any)
}

func haversine(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusKM = 6371.0
	dLat := degreesToRadians(lat2 - lat1)
	dLng := degreesToRadians(lng2 - lng1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(degreesToRadians(lat1))*math.Cos(degreesToRadians(lat2))*math.Sin(dLng/2)*math.Sin(dLng/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKM * c
}

func degreesToRadians(value float64) float64 {
	return value * math.Pi / 180
}
