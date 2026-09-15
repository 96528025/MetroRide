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
