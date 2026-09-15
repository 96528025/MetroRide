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

-- Apply inside a transaction; preserve existing rides and ledger amounts.
create table if not exists core_schema_migrations (version integer primary key, applied_at timestamptz not null default now());
create table if not exists driver_positions (
    driver_id text primary key,
    latitude double precision not null check (latitude between -90 and 90),
    longitude double precision not null check (longitude between -180 and 180),
    available boolean not null,
    updated_at timestamptz not null
);
create table if not exists driver_reservations (
    driver_id text primary key,
    ride_id uuid not null unique references rides(id),
    reserved_at timestamptz not null default now()
);
alter table ride_assignments add column if not exists schema_version integer not null default 1;
alter table ride_assignments add column if not exists trip_distance_km numeric check (trip_distance_km >= 0);
alter table ride_assignments add column if not exists trip_duration_seconds numeric check (trip_duration_seconds >= 0);
alter table ride_assignments add column if not exists route_provider text;
alter table ride_assignments add column if not exists route_calculated_at timestamptz;
-- A migration cannot choose a winner for already conflicting assignments.
do $$ begin
 if exists (select driver_id from rides where status in ('assigned','in_progress') group by driver_id having count(*) > 1)
    or exists (select 1 from rides where status in ('assigned','in_progress') and driver_id is null) then
  raise exception 'active rides have conflicting or missing drivers; resolve before migration';
 end if;
 if exists (select 1 from rides r join driver_reservations d on d.driver_id=r.driver_id
            where r.status in ('assigned','in_progress') and d.ride_id<>r.id) then
  raise exception 'existing reservation conflicts with active ride';
 end if;
end $$;
insert into driver_reservations(driver_id,ride_id,reserved_at)
select driver_id,id,coalesce(assigned_at,created_at) from rides where status in ('assigned','in_progress')
on conflict (driver_id) do nothing;
insert into core_schema_migrations(version) values(1) on conflict do nothing;
