-- Lock a persistent row even before an assignment has arrived from its separate stream.
create table fare.ride_state (
    ride_id text primary key,
    state text not null default 'active' check (state in ('active', 'completed', 'cancelled'))
);
insert into fare.ride_state(ride_id, state)
select distinct ride_id, 'completed' from fare.journal_entries where kind = 'settlement';
create unique index journal_entries_one_cancellation_per_ride
    on fare.journal_entries(ride_id) where kind = 'cancellation_reversal';
