package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

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

	svc := &notificationService{cfg: cfg, rdb: rdb}
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
	group := s.cfg.ConsumerGroup
	consumer := s.cfg.ConsumerName
	initCtx, initCancel := reliability.WithRedisTimeout(ctx)
	err := ensureGroup(initCtx, s.rdb, events.StreamRideNotifications, group)
	initCancel()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Error("notification consumer group failed", "error", err)
	}
	for {
		readCtx, readCancel := reliability.WithRedisTimeout(ctx)
		result, err := s.rdb.XReadGroup(readCtx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{events.StreamRideNotifications, ">"},
			Count:    10,
			Block:    time.Second,
		}).Result()
		readCancel()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			metrics.StreamConsumeErrors.WithLabelValues("notification-service", events.StreamRideNotifications).Inc()
			metrics.DependencyErrors.WithLabelValues("notification-service", "redis").Inc()
			log.Error("notification stream read failed", "error", err)
			continue
		}
		for _, stream := range result {
			for _, message := range stream.Messages {
				envelope, err := events.DecodeEnvelope(message)
				if err != nil {
					log.Error("decode notification event failed", "error", err)
					continue
				}
				if envelope.Type == events.TypeRideAssigned {
					payload, err := events.DecodePayload[events.RideAssigned](envelope)
					if err != nil {
						log.Error("decode assignment notification failed", "error", err)
						continue
					}
					log.Info("notification simulated", "event_type", envelope.Type, "ride_id", payload.RideID, "rider_id", payload.RiderID, "driver_id", payload.DriverID)
					s.processed.Add(1)
				}
				ackCtx, ackCancel := reliability.WithRedisTimeout(ctx)
				if err := s.rdb.XAck(ackCtx, events.StreamRideNotifications, group, message.ID).Err(); err != nil {
					metrics.DependencyErrors.WithLabelValues("notification-service", "redis").Inc()
					log.Error("notification ack failed", "error", err)
				}
				ackCancel()
			}
		}
	}
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
