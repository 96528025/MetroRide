package reliability

import (
	"context"
	"errors"
	"fmt"
	"github.com/metroride/metroride/shared/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"os"
	"strconv"
	"strings"
	"time"
)

var deliveries = promauto.NewCounterVec(prometheus.CounterOpts{Name: "metroride_stream_deliveries_total", Help: "Consumer processing attempts by delivery path."}, []string{"stream", "path"})
var processingFailures = promauto.NewCounterVec(prometheus.CounterOpts{Name: "metroride_stream_processing_failures_total", Help: "Failed consumer processing attempts."}, []string{"stream"})

type permanentError struct{ error }

func Permanent(err error) error { return permanentError{err} }

type StreamOptions struct {
	ReclaimInterval, MinIdle, ProcessTimeout time.Duration
	MaxDeliveries                            int64
}

func DefaultStreamOptions() StreamOptions {
	return StreamOptions{5 * time.Second, 120 * time.Second, 60 * time.Second, 25}
}
func StreamOptionsFromEnv() StreamOptions {
	o := DefaultStreamOptions()
	for _, field := range []struct {
		name string
		dst  *time.Duration
	}{
		{"STREAM_RECLAIM_INTERVAL_SECONDS", &o.ReclaimInterval}, {"STREAM_MIN_IDLE_SECONDS", &o.MinIdle}, {"STREAM_PROCESS_TIMEOUT_SECONDS", &o.ProcessTimeout},
	} {
		if raw := os.Getenv(field.name); raw != "" {
			n, e := strconv.Atoi(raw)
			if e != nil || n <= 0 {
				panic("invalid " + field.name)
			}
			*field.dst = time.Duration(n) * time.Second
		}
	}
	if raw := os.Getenv("STREAM_MAX_DELIVERIES"); raw != "" {
		n, e := strconv.ParseInt(raw, 10, 64)
		if e != nil || n < 1 {
			panic("invalid STREAM_MAX_DELIVERIES")
		}
		o.MaxDeliveries = n
	}
	if e := o.Validate(); e != nil {
		panic(e)
	}
	return o
}
func (o StreamOptions) Validate() error {
	if o.ReclaimInterval <= 0 || o.ProcessTimeout <= 0 || o.MinIdle <= o.ProcessTimeout || o.MaxDeliveries < 1 {
		return errors.New("stream idle time must exceed positive processing timeout; delivery limit must be positive")
	}
	return nil
}

type StreamLogger interface{ Error(string, ...any) }

// Consume processes one delivery at a time. Acknowledgment follows the durable
// effect or confirmed dead-letter publication; failures leave work pending.
func Consume(ctx context.Context, rdb *redis.Client, stream, group, consumer, service string, o StreamOptions, log StreamLogger, handle func(context.Context, redis.XMessage) error, dead func(context.Context, redis.XMessage, error) error) {
	if err := o.Validate(); err != nil {
		log.Error("invalid consumer options", "error", err)
		return
	}
	countRedisError := func() {
		if ctx.Err() == nil {
			metrics.StreamConsumeErrors.WithLabelValues(service, stream).Inc()
			metrics.DependencyErrors.WithLabelValues(service, "redis").Inc()
		}
	}
	cursor := "0-0"
	nextClaim := time.Time{}
	process := func(m redis.XMessage, path string) {
		deliveries.WithLabelValues(stream, path).Inc()
		work, cancel := context.WithTimeout(ctx, o.ProcessTimeout)
		err := handle(work, m)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			processingFailures.WithLabelValues(stream).Inc()
			check, c := WithRedisTimeout(ctx)
			pending, pErr := rdb.XPendingExt(check, &redis.XPendingExtArgs{Stream: stream, Group: group, Start: m.ID, End: m.ID, Count: 1}).Result()
			c()
			if pErr != nil {
				countRedisError()
				log.Error("read pending delivery count failed", "error", pErr)
				return
			}
			var permanent permanentError
			exhausted := len(pending) > 0 && pending[0].RetryCount >= o.MaxDeliveries
			if !errors.As(err, &permanent) && !exhausted {
				log.Error("processing failed; delivery remains pending", "error", err, "message_id", m.ID)
				return
			}
			dlq, c := WithRedisTimeout(ctx)
			dErr := dead(dlq, m, err)
			c()
			if dErr != nil {
				log.Error("dead-letter publication failed; delivery remains pending", "error", dErr)
				return
			}
		}
		ack, c := WithRedisTimeout(ctx)
		e := rdb.XAck(ack, stream, group, m.ID).Err()
		c()
		if e != nil {
			countRedisError()
			log.Error("acknowledgment failed", "error", e)
		}
	}
	for ctx.Err() == nil {
		init, c := WithRedisTimeout(ctx)
		err := rdb.XGroupCreateMkStream(init, stream, group, "0").Err()
		c()
		if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			countRedisError()
			log.Error("ensure group failed", "error", err)
			if !pause(ctx, 250*time.Millisecond) {
				return
			}
			continue
		}
		if !time.Now().Before(nextClaim) {
			claim, c := WithRedisTimeout(ctx)
			messages, next, e := rdb.XAutoClaim(claim, &redis.XAutoClaimArgs{Stream: stream, Group: group, Consumer: consumer, MinIdle: o.MinIdle, Start: cursor, Count: 1}).Result()
			c()
			nextClaim = time.Now().Add(o.ReclaimInterval)
			if e != nil {
				countRedisError()
				log.Error("pending recovery failed", "error", e)
			} else {
				cursor = next
				for _, m := range messages {
					process(m, "reclaimed")
				}
			}
		}
		read, c := WithRedisTimeout(ctx)
		result, e := rdb.XReadGroup(read, &redis.XReadGroupArgs{Group: group, Consumer: consumer, Streams: []string{stream, ">"}, Count: 1, Block: time.Second}).Result()
		c()
		if e != nil {
			if !errors.Is(e, redis.Nil) && ctx.Err() == nil {
				countRedisError()
				log.Error("read stream failed", "error", e)
				if !pause(ctx, 250*time.Millisecond) {
					return
				}
			}
			continue
		}
		for _, batch := range result {
			for _, m := range batch.Messages {
				process(m, "new")
			}
		}
	}
}
func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func DecodeError(err error) error { return Permanent(fmt.Errorf("invalid event: %w", err)) }
