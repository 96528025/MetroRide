package main

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/httpx"
	"github.com/metroride/metroride/shared/pkg/logging"
	"github.com/metroride/metroride/shared/pkg/metrics"
	"github.com/metroride/metroride/shared/pkg/reliability"
	"github.com/redis/go-redis/v9"
)

type notificationService struct {
	cfg       config.Config
	rdb       *redis.Client
	processed atomic.Uint64
	options   reliability.StreamOptions
}

func main() {
	metrics.RegisterCommon()
	cfg := config.Load("notification-service", ":8085")
	log := logging.New(cfg.ServiceName)
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  reliability.RedisTimeout,
		ReadTimeout:  reliability.RedisTimeout,
		WriteTimeout: reliability.RedisTimeout,
	})
	defer func() { _ = rdb.Close() }()

	svc := &notificationService{cfg: cfg, rdb: rdb, options: reliability.StreamOptionsFromEnv()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.consume(ctx, log)

	mux := httpx.CommonMuxWithReadiness(log, map[string]httpx.ReadinessCheck{
		"redis":               svc.checkRedis,
		"notification_stream": svc.checkNotificationStream,
	})
	mux.HandleFunc("GET /v1/notifications/stats", func(w http.ResponseWriter, r *http.Request) {
		httpx.RespondJSON(w, http.StatusOK, map[string]any{"processed": svc.processed.Load()})
	})
	server := httpx.NewServer(cfg.HTTPAddr, mux)
	go func() {
		log.Info("notification-service listening", "addr", cfg.HTTPAddr)
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

func (s *notificationService) consume(ctx context.Context, log interface {
	Info(string, ...any)
	Error(string, ...any)
}) {
	cfg := s.cfg
	if cfg.ConsumerGroup == "" {
		cfg = config.Load("notification-service", ":8085")
	}
	o := s.options
	if o.MaxDeliveries == 0 {
		o = reliability.DefaultStreamOptions()
	}
	reliability.Consume(ctx, s.rdb, events.StreamRideNotifications, cfg.ConsumerGroup, cfg.ConsumerName, "notification-service", o, log, func(ctx context.Context, m redis.XMessage) error {
		env, e := events.DecodeEnvelope(m)
		if e != nil {
			return reliability.DecodeError(e)
		}
		if env.Type != events.TypeRideAssigned {
			return reliability.Permanent(errors.New("unexpected notification event"))
		}
		p, e := events.DecodePayload[events.RideAssigned](env)
		if e != nil {
			return reliability.DecodeError(e)
		}
		if p.RideID == "" || p.DriverID == "" {
			return reliability.Permanent(errors.New("incomplete assignment notification"))
		}
		log.Info("notification simulated", "ride_id", p.RideID, "rider_id", p.RiderID, "driver_id", p.DriverID)
		s.processed.Add(1)
		return nil
	}, func(ctx context.Context, m redis.XMessage, cause error) error {
		env, e := events.NewEnvelope(uuid.NewString(), "dead_lettered", "notification-service", "", events.NewDeadLetter("notification-service", events.StreamRideNotifications, m, cause))
		if e != nil {
			return e
		}
		_, e = events.Publish(ctx, s.rdb, events.StreamDeadLetter, env)
		return e
	})
}

func (s *notificationService) checkRedis(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return s.rdb.Ping(checkCtx).Err()
}

func (s *notificationService) checkNotificationStream(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return ensureGroup(checkCtx, s.rdb, events.StreamRideNotifications, s.cfg.ConsumerGroup)
}

func ensureGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}
