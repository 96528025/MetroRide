package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/metroride/metroride/shared/pkg/config"
	"github.com/metroride/metroride/shared/pkg/httpx"
	sharedkafka "github.com/metroride/metroride/shared/pkg/kafka"
	"github.com/metroride/metroride/shared/pkg/logging"
	"github.com/metroride/metroride/shared/pkg/metrics"
	"github.com/metroride/metroride/shared/pkg/reliability"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	kafkaDriverLocationEvents = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "metroride_kafka_driver_location_events_total",
		Help: "Total Kafka driver location events consumed by analytics-service.",
	})
	kafkaConsumeErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "metroride_kafka_consume_errors_total",
		Help: "Total Kafka consume or decode errors in analytics-service.",
	})
	kafkaLastEventTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "metroride_kafka_last_event_timestamp_seconds",
		Help: "Unix timestamp for the latest Kafka driver location event consumed by analytics-service.",
	})
)

type driverLocation struct {
	DriverID  string    `json:"driver_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	Available bool      `json:"available"`
	Timestamp time.Time `json:"timestamp"`
}

type analyticsService struct {
	cfg       sharedkafka.Config
	consumer  *sharedkafka.Consumer
	mu        sync.RWMutex
	latest    map[string]driverLocation
	consumed  uint64
	startedAt time.Time
}

func main() {
	metrics.RegisterCommon()
	prometheus.MustRegister(kafkaDriverLocationEvents, kafkaConsumeErrors, kafkaLastEventTimestamp)

	cfg := config.Load("analytics-service", ":8086")
	log := logging.New(cfg.ServiceName)
	kafkaCfg := sharedkafka.LoadConfig("metroride-analytics-service")

	consumer := sharedkafka.NewConsumer(kafkaCfg)
	defer func() { _ = consumer.Close() }()

	svc := &analyticsService{
		cfg:       kafkaCfg,
		consumer:  consumer,
		latest:    map[string]driverLocation{},
		startedAt: time.Now().UTC(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.consume(ctx, log)

	mux := httpx.CommonMuxWithReadiness(log, map[string]httpx.ReadinessCheck{
		"kafka": svc.checkKafka,
	})
	mux.HandleFunc("GET /v1/analytics/drivers", svc.listDrivers)

	server := httpx.NewServer(cfg.HTTPAddr, mux)
	go func() {
		log.Info("analytics-service listening", "addr", cfg.HTTPAddr, "topic", kafkaCfg.Topic, "group_id", kafkaCfg.GroupID)
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

func (s *analyticsService) consume(ctx context.Context, log interface {
	Info(string, ...any)
	Error(string, ...any)
}) {
	for {
		var event sharedkafka.DriverLocationEvent
		_, err := s.consumer.ReadJSON(ctx, &event)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			kafkaConsumeErrors.Inc()
			metrics.DependencyErrors.WithLabelValues("analytics-service", "kafka").Inc()
			log.Error("consume kafka driver location failed", "error", err)
			continue
		}
		eventTime, err := time.Parse(time.RFC3339Nano, event.Timestamp)
		if err != nil {
			kafkaConsumeErrors.Inc()
			log.Error("parse kafka driver location timestamp failed", "error", err, "driver_id", event.DriverID)
			continue
		}
		s.mu.Lock()
		s.latest[event.DriverID] = driverLocation{
			DriverID:  event.DriverID,
			Lat:       event.Lat,
			Lng:       event.Lng,
			Available: event.Available,
			Timestamp: eventTime,
		}
		s.consumed++
		s.mu.Unlock()
		kafkaDriverLocationEvents.Inc()
		kafkaLastEventTimestamp.Set(float64(eventTime.Unix()))
		log.Info("consumed kafka driver location", "event_type", event.EventType, "driver_id", event.DriverID)
	}
}

func (s *analyticsService) listDrivers(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	drivers := make([]driverLocation, 0, len(s.latest))
	for _, driver := range s.latest {
		drivers = append(drivers, driver)
	}
	consumed := s.consumed
	s.mu.RUnlock()
	httpx.RespondJSON(w, http.StatusOK, map[string]any{
		"topic":          s.cfg.Topic,
		"consumer_group": s.cfg.GroupID,
		"total_consumed": consumed,
		"drivers":        drivers,
	})
}

func (s *analyticsService) checkKafka(ctx context.Context) error {
	checkCtx, cancel := reliability.WithReadinessTimeout(ctx)
	defer cancel()
	return sharedkafka.Ping(checkCtx, s.cfg.Brokers)
}
