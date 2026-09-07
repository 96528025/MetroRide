-- Double-entry ledger for fares. Both tables are append-only: rows are inserted
-- once and never updated or deleted. A correction is a new journal entry that
-- reverses an earlier one, so the history of every ride stays readable.
--
-- journal_entries: one row per business event on a ride's ledger. source_event_id
-- is the envelope ID that produced the entry, so one consumed event can produce at
-- most one entry; the row is written in the same transaction as the matching
-- fare.processed_events row.
--
-- postings: the debit and credit lines of one journal entry. Debits are positive,
-- credits negative, and the amounts of one journal entry sum to zero. That balance
-- rule is enforced by the JournalEntry constructor in the application rather than
-- by a database trigger; see the README for why.
create table fare.journal_entries (
    id bigint generated always as identity primary key,
    ride_id text not null,
    kind text not null,
    source_event_id text not null unique references fare.processed_events (event_id),
    created_at timestamptz not null
);

create index journal_entries_ride_id_idx on fare.journal_entries (ride_id);

create table fare.postings (
    id bigint generated always as identity primary key,
    journal_entry_id bigint not null references fare.journal_entries (id),
    account text not null,
    amount numeric(12, 2) not null
);

create index postings_journal_entry_id_idx on fare.postings (journal_entry_id);
