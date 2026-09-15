package reliability

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type quietLog struct{}

func (quietLog) Error(string, ...any) {}
func streamTest(t *testing.T) (*redis.Client, string, string) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR enables real Redis recovery tests")
	}
	r := redis.NewClient(&redis.Options{Addr: addr})
	stream := "test:recovery:" + uuid.NewString()
	group := "workers"
	if e := r.XGroupCreateMkStream(context.Background(), stream, group, "0").Err(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Del(context.Background(), stream); r.Close() })
	return r, stream, group
}
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	end := time.Now().Add(8 * time.Second)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func testOptions() StreamOptions {
	return StreamOptions{ReclaimInterval: 20 * time.Millisecond, MinIdle: 150 * time.Millisecond, ProcessTimeout: 100 * time.Millisecond, MaxDeliveries: 3}
}
func TestStreamOptionsRejectUnsafeClaimWindow(t *testing.T) {
	o := testOptions()
	o.MinIdle = o.ProcessTimeout
	if o.Validate() == nil {
		t.Fatal("must reject reclaim during processing")
	}
}
func TestRestartRecoversCommittedButUnacknowledgedDelivery(t *testing.T) {
	r, s, g := streamTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: map[string]any{"event": "committed"}})
	var effects atomic.Int32
	var attempts atomic.Int32
	done := make(chan struct{})
	handle := func(context.Context, redis.XMessage) error {
		attempts.Add(1)
		effects.CompareAndSwap(0, 1)
		cancel()
		return nil
	}
	go func() {
		defer close(done)
		Consume(ctx, r, s, g, "old", testOptions(), quietLog{}, handle, func(context.Context, redis.XMessage, error) error { return nil })
	}()
	<-done
	if p := r.XPending(context.Background(), s, g).Val(); p.Count != 1 {
		t.Fatalf("pending=%d", p.Count)
	}
	next, stop := context.WithCancel(context.Background())
	nextDone := make(chan struct{})
	defer func() { stop(); <-nextDone }()
	go func() {
		defer close(nextDone)
		Consume(next, r, s, g, "new", testOptions(), quietLog{}, func(context.Context, redis.XMessage) error { attempts.Add(1); effects.CompareAndSwap(0, 1); return nil }, func(context.Context, redis.XMessage, error) error { return nil })
	}()
	eventually(t, func() bool { return r.XPending(context.Background(), s, g).Val().Count == 0 })
	if effects.Load() != 1 || attempts.Load() != 2 {
		t.Fatalf("effects=%d attempts=%d", effects.Load(), attempts.Load())
	}
}
func TestFailedDeadLetterStaysPendingUntilPublicationSucceeds(t *testing.T) {
	r, s, g := streamTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	r.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: map[string]any{"event": "poison"}})
	var deadCalls atomic.Int32
	var allow atomic.Bool
	go func() {
		defer close(done)
		Consume(ctx, r, s, g, "one", testOptions(), quietLog{}, func(context.Context, redis.XMessage) error { return Permanent(errors.New("bad payload")) }, func(context.Context, redis.XMessage, error) error {
			deadCalls.Add(1)
			if !allow.Load() {
				return errors.New("publisher unavailable")
			}
			return nil
		})
	}()
	eventually(t, func() bool { return deadCalls.Load() > 0 })
	if r.XPending(ctx, s, g).Val().Count != 1 {
		t.Fatal("acknowledged failed dead letter")
	}
	allow.Store(true)
	eventually(t, func() bool { return r.XPending(ctx, s, g).Val().Count == 0 })
}
func TestRetryableFailureUsesRedisDeliveryCount(t *testing.T) {
	r, s, g := streamTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	r.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: map[string]any{"event": "transient"}})
	var attempts, dead atomic.Int32
	go func() {
		defer close(done)
		Consume(ctx, r, s, g, "one", testOptions(), quietLog{}, func(context.Context, redis.XMessage) error { attempts.Add(1); return errors.New("offline") }, func(context.Context, redis.XMessage, error) error { dead.Add(1); return nil })
	}()
	eventually(t, func() bool { return dead.Load() == 1 && r.XPending(ctx, s, g).Val().Count == 0 })
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
}

func TestReclaimCursorReachesOldWorkBeyondFreshPendingEntries(t *testing.T) {
	r, s, g := streamTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ids := []string{}
	for i := 0; i < 17; i++ {
		ids = append(ids, r.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: map[string]any{"event": "cursor"}}).Val())
	}
	if e := r.XReadGroup(ctx, &redis.XReadGroupArgs{Group: g, Consumer: "old", Streams: []string{s, ">"}, Count: 17}).Err(); e != nil {
		t.Fatal(e)
	}
	target := ids[16]
	if e := r.Do(ctx, "XCLAIM", s, g, "old", 0, target, "IDLE", 3000, "JUSTID").Err(); e != nil {
		t.Fatal(e)
	}
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				r.XClaimJustID(ctx, &redis.XClaimArgs{Stream: s, Group: g, Consumer: "old", MinIdle: 0, Messages: ids[:16]})
			}
		}
	}()
	var recovered atomic.Bool
	o := testOptions()
	o.MinIdle = 2 * time.Second
	done := make(chan struct{})
	go func() {
		defer close(done)
		Consume(ctx, r, s, g, "new", o, quietLog{}, func(_ context.Context, m redis.XMessage) error {
			if m.ID == target {
				recovered.Store(true)
			}
			return nil
		}, func(context.Context, redis.XMessage, error) error { return nil })
	}()
	defer func() { cancel(); <-done; <-refreshDone }()
	eventually(t, func() bool { return recovered.Load() })
}
