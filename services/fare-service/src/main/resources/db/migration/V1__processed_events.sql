-- fare-service owns the `fare` schema. Flyway creates the schema and keeps its
-- history table there (spring.flyway.schemas=fare), so this service never touches
-- the tables that infrastructure/docker/postgres/init.sql creates in `public`
-- for the Go services.
--
-- processed_events is the idempotency record for consumed envelopes: one row per
-- event id, inserted with ON CONFLICT DO NOTHING before the stream entry is
-- acknowledged.
create table fare.processed_events (
    event_id text primary key,
    stream text not null,
    event_type text not null,
    processed_at timestamptz not null
);
