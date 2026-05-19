package main

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/httpx"
	"github.com/metroride/metroride/shared/pkg/logging"
	"github.com/redis/go-redis/v9"
)

type simulatedDriver struct {
	ID        string  `json:"id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Available bool    `json:"available"`
}

type driverService struct {
	drivers []simulatedDriver
	rdb     *redis.Client
}

func main() {
	cfg := config.Load("driver-service", ":8081")
	log := logging.New(cfg.ServiceName)
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	svc := &driverService{
		rdb: rdb,
		drivers: []simulatedDriver{
			{ID: "driver-1001", Latitude: 37.7749, Longitude: -122.4194, Available: true},
			{ID: "driver-1002", Latitude: 37.7840, Longitude: -122.4075, Available: true},
			{ID: "driver-1003", Latitude: 37.7680, Longitude: -122.4310, Available: true},
			{ID: "driver-1004", Latitude: 37.7890, Longitude: -122.4340, Available: true},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.publishLocations(ctx, log)

	mux := httpx.CommonMux(log)
	mux.HandleFunc("GET /v1/drivers", svc.listDrivers)
	server := httpx.NewServer(cfg.HTTPAddr, mux)
	go func() {
		log.Info("driver-service listening", "addr", cfg.HTTPAddr)
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

func (s *driverService) listDrivers(w http.ResponseWriter, r *http.Request) {
	httpx.RespondJSON(w, http.StatusOK, map[string]any{"drivers": s.drivers})
}

func (s *driverService) publishLocations(ctx context.Context, log interface {
	Info(string, ...any)
	Error(string, ...any)
}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := range s.drivers {
				s.drivers[i].Latitude += (rand.Float64() - 0.5) / 2000
				s.drivers[i].Longitude += (rand.Float64() - 0.5) / 2000
				s.drivers[i].Latitude = math.Round(s.drivers[i].Latitude*1000000) / 1000000
				s.drivers[i].Longitude = math.Round(s.drivers[i].Longitude*1000000) / 1000000
				payload := events.DriverLocationUpdated{
					DriverID: s.drivers[i].ID, Latitude: s.drivers[i].Latitude, Longitude: s.drivers[i].Longitude,
					Available: s.drivers[i].Available, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
				}
				envelope, err := events.NewEnvelope(uuid.NewString(), events.TypeDriverLocationUpdated, "driver-service", payload.DriverID, payload)
				if err != nil {
					log.Error("encode driver location failed", "error", err)
					continue
				}
				if _, err := events.Publish(ctx, s.rdb, events.StreamDriverLocations, envelope); err != nil {
					log.Error("publish driver location failed", "error", err, "driver_id", payload.DriverID)
				}
			}
		}
	}
}
