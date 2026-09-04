create table if not exists rides (
    id uuid primary key,
    rider_id text not null,
    driver_id text,
    pickup_lat double precision not null,
    pickup_lng double precision not null,
    dropoff_lat double precision not null,
    dropoff_lng double precision not null,
    status text not null check (status in ('requested', 'assigned', 'in_progress', 'completed', 'cancelled')),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    assigned_at timestamptz
);

create index if not exists rides_rider_id_idx on rides (rider_id);
create index if not exists rides_status_idx on rides (status);

create table if not exists ride_assignments (
    id uuid primary key,
    ride_id uuid not null references rides(id),
    driver_id text not null,
    distance_km double precision not null,
    eta_seconds integer not null,
    created_at timestamptz not null
);

create index if not exists ride_assignments_driver_id_idx on ride_assignments (driver_id);

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
);

create index if not exists event_outbox_unpublished_schedule_idx
    on event_outbox (source_service, next_attempt_at, created_at, id)
    where published_at is null;
