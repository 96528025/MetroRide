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

type trafficService struct {
	rdb        *redis.Client
	congestion map[string]float64
}

func main() {
	cfg := config.Load("traffic-service", ":8084")
	log := logging.New(cfg.ServiceName)
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	svc := &trafficService{
		rdb: rdb,
		congestion: map[string]float64{
			"sf-downtown": 1.0,
			"sf-soma":     1.1,
			"sf-mission":  0.9,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.publishTraffic(ctx, log)

	mux := httpx.CommonMux(log)
	mux.HandleFunc("GET /v1/traffic", svc.currentTraffic)
	server := httpx.NewServer(cfg.HTTPAddr, mux)
	go func() {
		log.Info("traffic-service listening", "addr", cfg.HTTPAddr)
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

func (s *trafficService) currentTraffic(w http.ResponseWriter, r *http.Request) {
	httpx.RespondJSON(w, http.StatusOK, map[string]any{"regions": s.congestion})
}

func (s *trafficService) publishTraffic(ctx context.Context, log interface {
	Info(string, ...any)
	Error(string, ...any)
}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for region, current := range s.congestion {
				next := current + (rand.Float64()-0.5)/5
				next = math.Max(0.6, math.Min(2.5, math.Round(next*100)/100))
				s.congestion[region] = next
				payload := events.TrafficUpdated{
					Region: region, Congestion: next, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
				}
				envelope, err := events.NewEnvelope(uuid.NewString(), events.TypeTrafficUpdated, "traffic-service", region, payload)
				if err != nil {
					log.Error("encode traffic update failed", "error", err)
					continue
				}
				if _, err := events.Publish(ctx, s.rdb, events.StreamTrafficUpdates, envelope); err != nil {
					log.Error("publish traffic update failed", "error", err, "region", region)
				}
			}
		}
	}
}
