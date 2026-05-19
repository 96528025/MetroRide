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
