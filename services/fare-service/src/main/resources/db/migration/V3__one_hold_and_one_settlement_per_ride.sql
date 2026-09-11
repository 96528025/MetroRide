-- One quote_hold and one settlement per ride, enforced by the database.
--
-- V2 keys journal_entries on (source_event_id, kind): one event cannot write the same
-- kind twice. It says nothing about how many holds or settlements a ride may have, and
-- the application's checks (lock the hold, count the holds, look for a settlement) run
-- inside one transaction that cannot see another transaction's uncommitted insert. A
-- completion settling hold A and a second, distinct ride_assigned inserting hold B for
-- the same ride could therefore both commit and leave B un-reversed. These partial
-- unique indexes make that state unrepresentable: the second hold, or the second
-- settlement, is refused at insert and the whole transaction rolls back.
--
-- The application still checks first (a clean already_settled instead of a constraint
-- error); the indexes are the backstop for the window those checks cannot close, i.e.
-- the second writer PR #16's README said a database-level rule should wait for.
-- hold_reversal needs no index: it is written in the same transaction as the settlement.
create unique index journal_entries_one_quote_hold_per_ride
    on fare.journal_entries (ride_id)
    where kind = 'quote_hold';

create unique index journal_entries_one_settlement_per_ride
    on fare.journal_entries (ride_id)
    where kind = 'settlement';
