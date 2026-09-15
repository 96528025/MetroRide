-- Historical holds keep their amounts. No route evidence or historical rates are invented.
create table fare.quote_context (
    ride_id text primary key,
    source_event_id text not null references fare.processed_events(event_id),
    pricing_version text not null,
    trip_distance_km numeric not null check (trip_distance_km >= 0),
    trip_duration_seconds numeric not null check (trip_duration_seconds >= 0),
    route_provider text not null,
    route_calculated_at timestamptz not null,
    base_fare numeric not null,
    per_km numeric not null,
    per_minute numeric not null,
    driver_share numeric not null check (driver_share between 0 and 1)
);
