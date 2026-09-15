package main

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	store   driverStore
	routes  routeEstimator
	rdb     *redis.Client
	cfg     config.Config
	options reliability.StreamOptions
}
type nearestDriverRequest struct {
	PickupLat  *float64 `json:"pickup_lat"`
	PickupLng  *float64 `json:"pickup_lng"`
	DropoffLat *float64 `json:"dropoff_lat"`
	DropoffLng *float64 `json:"dropoff_lng"`
}

const nearestDriverAlgorithm = "road-time-shortlist"

func main() {
	metrics.RegisterCommon()
	prometheus.MustRegister(routingDuration, activeDrivers)
	cfg := config.Load("routing-service", ":8083")
	log := logging.New(cfg.ServiceName)
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  reliability.RedisTimeout,
		ReadTimeout:  reliability.RedisTimeout,
		WriteTimeout: reliability.RedisTimeout,
	})
	defer func() { _ = rdb.Close() }()

	db, err := pgxpool.New(context.Background(), cfg.PostgresDSN)
	if err != nil {
		log.Error("configure database failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	endpoint := os.Getenv("VALHALLA_BASE_URL")
	if endpoint == "" {
		endpoint = "https://valhalla1.openstreetmap.de"
	}
	svc := &routingService{store: &postgresDrivers{db, config.DriverMaxAge()}, routes: &valhallaClient{endpoint, &http.Client{Timeout: 10 * time.Second}, rdb}, rdb: rdb, cfg: cfg, options: reliability.StreamOptionsFromEnv()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.consumeDriverLocations(ctx, log)

	mux := httpx.CommonMuxWithReadiness(log, map[string]httpx.ReadinessCheck{
		"redis":                  svc.checkRedis,
		"postgres":               svc.store.Ready,
		"driver_location_stream": svc.checkDriverLocationStream,
	})
	mux.HandleFunc("POST /v1/routes/nearest-driver", svc.nearestDriver)
	server := httpx.NewServer(cfg.HTTPAddr, mux)
	server.WriteTimeout = 65 * time.Second
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
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	start := time.Now()
	defer func() { routingDuration.Observe(time.Since(start).Seconds()) }()

	var req nearestDriverRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.RespondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if req.PickupLat == nil || req.PickupLng == nil || req.DropoffLat == nil || req.DropoffLng == nil || !validPoint(*req.PickupLat, *req.PickupLng) || !validPoint(*req.DropoffLat, *req.DropoffLng) {
		httpx.RespondJSON(w, 400, map[string]string{"error": "valid pickup and dropoff coordinates are required"})
		return
	}
	candidates, e := s.store.Available(r.Context())
	if e != nil {
		httpx.RespondJSON(w, 503, map[string]string{"error": "driver state unavailable"})
		return
	}
	activeDrivers.Set(float64(len(candidates)))
	if len(candidates) == 0 {
		httpx.RespondJSON(w, 503, map[string]string{"error": "no available drivers"})
		return
	}
	trip, e := s.routes.Estimate(r.Context(), *req.PickupLat, *req.PickupLng, *req.DropoffLat, *req.DropoffLng)
	if e != nil {
		httpx.RespondJSON(w, 503, map[string]string{"error": "passenger route unavailable"})
		return
	}
	var selected driver
	var approach routeEstimate
	found := false
	for i := 0; i < 5; i++ {
		candidate, _, ok := selectNearestDriver(candidates, *req.PickupLat, *req.PickupLng)
		if !ok {
			break
		}
		delete(candidates, candidate.ID)
		estimate, e := s.routes.Estimate(r.Context(), candidate.Latitude, candidate.Longitude, *req.PickupLat, *req.PickupLng)
		if e != nil {
			continue
		}
		if !found || estimate.DurationSeconds < approach.DurationSeconds || (estimate.DurationSeconds == approach.DurationSeconds && candidate.ID < selected.ID) {
			selected = candidate
			approach = estimate
			found = true
		}
	}
	if !found {
		httpx.RespondJSON(w, 503, map[string]string{"error": "driver route unavailable"})
		return
	}
	httpx.RespondJSON(w, 200, map[string]any{"driver_id": selected.ID, "distance_km": approach.DistanceKM, "eta_seconds": int(math.Ceil(approach.DurationSeconds)), "trip_distance_km": trip.DistanceKM, "trip_duration_seconds": trip.DurationSeconds, "route_provider": trip.Provider, "route_calculated_at": trip.CalculatedAt, "algorithm": nearestDriverAlgorithm})

}

// selectNearestDriver only needs the minimum, so it scans once instead of
// copying and sorting every candidate. Ties use the driver ID for deterministic
// results even though Go map iteration order is intentionally random.
func selectNearestDriver(drivers map[string]driver, pickupLat, pickupLng float64) (driver, float64, bool) {
	var selected driver
	bestDistance := math.Inf(1)
	found := false
	for _, candidate := range drivers {
		if !candidate.Available {
			continue
		}
		distance := haversine(pickupLat, pickupLng, candidate.Latitude, candidate.Longitude)
		if !found || distance < bestDistance || (distance == bestDistance && candidate.ID < selected.ID) {
			selected = candidate
			bestDistance = distance
			found = true
		}
	}
	return selected, bestDistance, found
}

func (s *routingService) consumeDriverLocations(ctx context.Context, log anyLogger) {
	cfg := s.cfg
	if cfg.ConsumerGroup == "" {
		cfg = config.Load("routing-service", ":8083")
	}
	o := s.options
	if o.MaxDeliveries == 0 {
		o = reliability.DefaultStreamOptions()
	}
	reliability.Consume(ctx, s.rdb, events.StreamDriverLocations, cfg.ConsumerGroup, cfg.ConsumerName, o, log, func(ctx context.Context, m redis.XMessage) error {
		env, e := events.DecodeEnvelope(m)
		if e != nil {
			return reliability.DecodeError(e)
		}
		if env.Type != events.TypeDriverLocationUpdated {
			return reliability.Permanent(errors.New("unexpected location event type"))
		}
		p, e := events.DecodePayload[events.DriverLocationUpdated](env)
		if e != nil {
			return reliability.DecodeError(e)
		}
		return s.store.Save(ctx, p)
	}, func(ctx context.Context, m redis.XMessage, cause error) error {
		env, e := events.NewEnvelope(uuid.NewString(), "dead_lettered", "routing-service", "", events.NewDeadLetter("routing-service", events.StreamDriverLocations, m, cause))
		if e != nil {
			return e
		}
		_, e = events.Publish(ctx, s.rdb, events.StreamDeadLetter, env)
		return e
	})
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

func (s *routingService) checkRedis(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return s.rdb.Ping(checkCtx).Err()
}

func (s *routingService) checkDriverLocationStream(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return ensureGroup(checkCtx, s.rdb, events.StreamDriverLocations, "routing-service")
}

func ensureGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}
