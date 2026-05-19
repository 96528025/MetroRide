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
	"github.com/redis/go-redis/v9"
)

type notificationService struct {
	cfg       config.Config
	rdb       *redis.Client
	processed atomic.Uint64
}

func main() {
	cfg := config.Load("notification-service", ":8085")
	log := logging.New(cfg.ServiceName)
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	svc := &notificationService{cfg: cfg, rdb: rdb}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.consume(ctx, log)

	mux := httpx.CommonMux(log)
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
	err := s.rdb.XGroupCreateMkStream(ctx, events.StreamRideNotifications, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Error("notification consumer group failed", "error", err)
	}
	for {
		result, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{events.StreamRideNotifications, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
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
					log.Info("notification simulated", "ride_id", payload.RideID, "rider_id", payload.RiderID, "driver_id", payload.DriverID)
					s.processed.Add(1)
				}
				_ = s.rdb.XAck(ctx, events.StreamRideNotifications, group, message.ID).Err()
			}
		}
	}
}
