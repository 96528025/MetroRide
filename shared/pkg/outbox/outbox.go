package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/metroride/metroride/shared/pkg/reliability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const (
	defaultBatchSize    = 25
	defaultPollInterval = 250 * time.Millisecond
	maxRetryBackoff     = 30 * time.Second
)

var (
	publishedEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "metroride_outbox_events_published_total",
		Help: "Outbox events successfully published to Redis Streams.",
	}, []string{"service", "stream"})
	publishFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "metroride_outbox_publish_failures_total",
		Help: "Outbox event publication failures.",
	}, []string{"service", "stream"})
)

// RegisterMetrics registers the outbox relay metrics in the current process.
func RegisterMetrics() {
	prometheus.MustRegister(publishedEvents, publishFailures)
}

// EnsureSchema makes local upgrades safe for existing Docker volumes. A real
// deployment would run the same DDL through a versioned migration job.
func EnsureSchema(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
		create table if not exists event_outbox (
			id text not null,
			source_service text not null,
			aggregate_id text not null,
			event_type text not null,
			stream text not null,
			envelope jsonb not null,
			created_at timestamptz not null,
			published_at timestamptz,
			publish_attempts integer not null default 0,
			next_attempt_at timestamptz not null default now(),
			last_error text,
			primary key (id, stream)
		)
	`)
	if err != nil {
		return fmt.Errorf("create event outbox: %w", err)
	}
	_, err = db.Exec(ctx, `
		alter table event_outbox
		add column if not exists next_attempt_at timestamptz not null default now()
	`)
	if err != nil {
		return fmt.Errorf("add outbox retry schedule: %w", err)
	}
	_, err = db.Exec(ctx, `
		create index if not exists event_outbox_unpublished_schedule_idx
		on event_outbox (source_service, next_attempt_at, created_at, id)
		where published_at is null
	`)
	if err != nil {
		return fmt.Errorf("index event outbox: %w", err)
	}
	if _, err = db.Exec(ctx, `drop index if exists event_outbox_unpublished_idx`); err != nil {
		return fmt.Errorf("remove obsolete outbox index: %w", err)
	}
	return nil
}

// Enqueue stores an event in the caller's transaction. The domain state and
// intent to publish therefore commit or roll back together.
func Enqueue(ctx context.Context, tx pgx.Tx, stream string, envelope events.Envelope) error {
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal outbox envelope: %w", err)
	}
	_, err = tx.Exec(ctx, `
		insert into event_outbox (
			id, source_service, aggregate_id, event_type, stream, envelope, created_at
		) values ($1, $2, $3, $4, $5, $6, $7)
	`, envelope.ID, envelope.Source, envelope.CorrelationID, envelope.Type, stream, body, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("enqueue %s event: %w", envelope.Type, err)
	}
	return nil
}

type Relay struct {
	service      string
	log          *slog.Logger
	db           *pgxpool.Pool
	rdb          *redis.Client
	batchSize    int
	pollInterval time.Duration
}

func NewRelay(service string, log *slog.Logger, db *pgxpool.Pool, rdb *redis.Client) *Relay {
	return &Relay{
		service:      service,
		log:          log,
		db:           db,
		rdb:          rdb,
		batchSize:    defaultBatchSize,
		pollInterval: defaultPollInterval,
	}
}

// Run publishes committed events until the context is cancelled. Publication
// is at-least-once: a crash after Redis accepts an event but before PostgreSQL
// records published_at can cause a duplicate with the same envelope ID.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		if err := r.PublishPending(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("outbox relay failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type record struct {
	id              string
	stream          string
	envelope        events.Envelope
	publishAttempts int
}

func retryBackoff(previousAttempts int) time.Duration {
	delay := defaultPollInterval
	for attempt := 0; attempt < previousAttempts; attempt++ {
		if delay >= maxRetryBackoff/2 {
			return maxRetryBackoff
		}
		delay *= 2
	}
	if delay > maxRetryBackoff {
		return maxRetryBackoff
	}
	return delay
}

// PublishPending publishes at most one batch of currently eligible events.
// Callers may use it for a bounded drain; Run invokes it on every poll.
func (r *Relay) PublishPending(ctx context.Context) error {
	dbCtx, cancel := reliability.WithPostgresTimeout(ctx)
	tx, err := r.db.Begin(dbCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("begin outbox relay transaction: %w", err)
	}
	defer func() {
		rollbackCtx, rollbackCancel := reliability.WithPostgresTimeout(context.Background())
		defer rollbackCancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	dbCtx, cancel = reliability.WithPostgresTimeout(ctx)
	rows, err := tx.Query(dbCtx, `
		select id, stream, envelope, publish_attempts
		from event_outbox
		where source_service = $1 and published_at is null and next_attempt_at <= now()
		order by next_attempt_at, created_at, id
		limit $2
		for update skip locked
	`, r.service, r.batchSize)
	if err != nil {
		cancel()
		return fmt.Errorf("query pending outbox events: %w", err)
	}

	records := make([]record, 0, r.batchSize)
	for rows.Next() {
		var item record
		var body []byte
		if err := rows.Scan(&item.id, &item.stream, &body, &item.publishAttempts); err != nil {
			rows.Close()
			cancel()
			return fmt.Errorf("scan outbox event: %w", err)
		}
		if err := json.Unmarshal(body, &item.envelope); err != nil {
			rows.Close()
			cancel()
			return fmt.Errorf("decode outbox event %s: %w", item.id, err)
		}
		records = append(records, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		cancel()
		return fmt.Errorf("iterate outbox events: %w", err)
	}
	rows.Close()
	cancel()

	// A failed destination must not block unrelated streams. Record every attempt
	// in this transaction and continue through the batch. Failed rows receive a
	// capped exponential retry time; ordering all eligible rows by that time keeps
	// both retries and newly queued work moving without either class starving.
	type publishFailure struct {
		item record
		err  error
	}
	published := make([]record, 0, len(records))
	failures := make([]publishFailure, 0)
	for i := range records {
		item := &records[i]
		redisCtx, cancel := reliability.WithRedisTimeout(ctx)
		_, publishErr := events.Publish(redisCtx, r.rdb, item.stream, item.envelope)
		cancel()
		if publishErr != nil {
			retryDelaySeconds := retryBackoff(item.publishAttempts).Seconds()
			dbCtx, cancel := reliability.WithPostgresTimeout(ctx)
			_, err := tx.Exec(dbCtx, `
				update event_outbox
				set publish_attempts = publish_attempts + 1,
					next_attempt_at = clock_timestamp() + make_interval(secs => $3),
					last_error = $4
				where id = $1 and stream = $2 and published_at is null
			`, item.id, item.stream, retryDelaySeconds, publishErr.Error())
			cancel()
			if err != nil {
				return fmt.Errorf("record outbox event %s publish failure: %w", item.id, err)
			}
			failures = append(failures, publishFailure{item: *item, err: publishErr})
			continue
		}
		// A failure here aborts the transaction, so the batch's progress is lost
		// and those events are published again after the next poll. Postgres
		// forbids committing an aborted transaction, so the duplicate is the only
		// safe outcome; consumers stay idempotent for exactly this reason.
		dbCtx, cancel := reliability.WithPostgresTimeout(ctx)
		_, err := tx.Exec(dbCtx, `
			update event_outbox
			set published_at = clock_timestamp(), publish_attempts = publish_attempts + 1, last_error = null
			where id = $1 and stream = $2
		`, item.id, item.stream)
		cancel()
		if err != nil {
			return fmt.Errorf("mark outbox event %s published: %w", item.id, err)
		}
		published = append(published, *item)
	}

	dbCtx, cancel = reliability.WithPostgresTimeout(ctx)
	err = tx.Commit(dbCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("commit outbox relay transaction: %w", err)
	}

	for _, item := range published {
		publishedEvents.WithLabelValues(r.service, item.stream).Inc()
	}
	for _, failure := range failures {
		publishFailures.WithLabelValues(r.service, failure.item.stream).Inc()
	}
	if len(failures) > 0 {
		first := failures[0]
		return fmt.Errorf("publish %d outbox event(s); first failure for %s: %w", len(failures), first.item.id, first.err)
	}
	return nil
}
