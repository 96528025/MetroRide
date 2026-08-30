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
			last_error text,
			primary key (id, stream)
		)
	`)
	if err != nil {
		return fmt.Errorf("create event outbox: %w", err)
	}
	_, err = db.Exec(ctx, `
		create index if not exists event_outbox_unpublished_idx
		on event_outbox (source_service, created_at)
		where published_at is null
	`)
	if err != nil {
		return fmt.Errorf("index event outbox: %w", err)
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
		if err := r.publishBatch(ctx); err != nil && !errors.Is(err, context.Canceled) {
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
	id       string
	stream   string
	envelope events.Envelope
}

func (r *Relay) publishBatch(ctx context.Context) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox relay transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	rows, err := tx.Query(ctx, `
		select id, stream, envelope
		from event_outbox
		where source_service = $1 and published_at is null
		order by created_at
		limit $2
		for update skip locked
	`, r.service, r.batchSize)
	if err != nil {
		return fmt.Errorf("query pending outbox events: %w", err)
	}

	records := make([]record, 0, r.batchSize)
	for rows.Next() {
		var item record
		var body []byte
		if err := rows.Scan(&item.id, &item.stream, &body); err != nil {
			rows.Close()
			return fmt.Errorf("scan outbox event: %w", err)
		}
		if err := json.Unmarshal(body, &item.envelope); err != nil {
			rows.Close()
			return fmt.Errorf("decode outbox event %s: %w", item.id, err)
		}
		records = append(records, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate outbox events: %w", err)
	}
	rows.Close()

	for _, item := range records {
		redisCtx, cancel := reliability.WithRedisTimeout(ctx)
		_, publishErr := events.Publish(redisCtx, r.rdb, item.stream, item.envelope)
		cancel()
		if publishErr != nil {
			publishFailures.WithLabelValues(r.service, item.stream).Inc()
			_ = tx.Rollback(ctx)
			r.recordFailure(ctx, item.id, item.stream, publishErr)
			return fmt.Errorf("publish outbox event %s: %w", item.id, publishErr)
		}
		if _, err := tx.Exec(ctx, `
			update event_outbox
			set published_at = now(), publish_attempts = publish_attempts + 1, last_error = null
			where id = $1 and stream = $2
		`, item.id, item.stream); err != nil {
			return fmt.Errorf("mark outbox event %s published: %w", item.id, err)
		}
		publishedEvents.WithLabelValues(r.service, item.stream).Inc()
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox relay transaction: %w", err)
	}
	return nil
}

func (r *Relay) recordFailure(ctx context.Context, id, stream string, cause error) {
	dbCtx, cancel := reliability.WithPostgresTimeout(ctx)
	defer cancel()
	if _, err := r.db.Exec(dbCtx, `
		update event_outbox
		set publish_attempts = publish_attempts + 1, last_error = $3
		where id = $1 and stream = $2 and published_at is null
	`, id, stream, cause.Error()); err != nil {
		r.log.Error("record outbox publish failure failed", "error", err, "event_id", id)
	}
}
