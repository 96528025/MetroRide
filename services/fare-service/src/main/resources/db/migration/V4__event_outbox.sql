-- Transactional outbox for the events fare-service publishes, in the fare schema.
--
-- The Go services share public.event_outbox, created by shared/pkg/outbox and scanned
-- by each service's relay by source_service. fare-service does not write there: the
-- table is owned by init.sql and the Go relays, and this service touches only the
-- fare schema (see V1). This table has the same columns, the same primary key and the
-- same partial index as the Go one, so an operator inspects both with the same SQL and
-- the Java relay runs the same statements as shared/pkg/outbox does.
--
-- A row is inserted in the transaction that writes the journal entries it announces
-- (ProcessedEventRecorder.settle), so an event exists exactly when its ledger entries
-- exist. The relay publishes rows with published_at null and next_attempt_at <= now(),
-- marks published_at on success and pushes next_attempt_at back with a capped
-- exponential delay on failure. Delivery is at-least-once: a crash after Redis accepted
-- the entry and before published_at is committed publishes the same envelope again.
create table fare.event_outbox (
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
);

create index event_outbox_unpublished_schedule_idx
    on fare.event_outbox (source_service, next_attempt_at, created_at, id)
    where published_at is null;
