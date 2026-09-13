# fare-service

A Java service in the MetroRide monorepo. It consumes `events.ride.assignments` and
`events.ride.completions` from Redis Streams through the consumer group `fare-service`, records
every envelope it sees once in its own PostgreSQL schema, for each first-seen `ride_assigned`
quotes the fare from the assignment's distance and ETA and holds that quote in a double-entry
ledger, and for each first-seen `ride_completed` reverses that hold and settles the quoted
amount between the driver and the platform, and announces that settlement on
`events.ride.fares` through a transactional outbox in its own schema. Settlement is by quote: the
amount settled is the amount held, nothing is metered. An entry it cannot handle is retried from
the consumer group's pending list, or written to `events.dead_letter`; see "Failure handling and
pending-entry recovery". Nothing consumes `events.ride.fares` yet; see "Publication".

## What is here

| Piece | Where | Notes |
| --- | --- | --- |
| Environment mapping | `config/MetroRideEnvironmentPostProcessor` | Reads `FARE_SERVICE_ADDR`, `POSTGRES_DSN`, `REDIS_ADDR` (same names and defaults as `shared/pkg/config/config.go`) and derives the Spring properties |
| Consumer settings | `config/ConsumerProperties`, `application.yml` | Group and consumer name come from `CONSUMER_GROUP` and `CONSUMER_NAME` through the same post-processor; the `streams` list, batch size and block timeout live in `application.yml` |
| Envelope contract | `events/Envelope`, `events/RideAssigned`, `events/RideCompleted`, `events/EnvelopeCodec` | Field-for-field match with `shared/pkg/events/events.go`; the stream entry field is `event`, as written by `events.Publish` |
| Rate card | `pricing/FareProperties`, `application.yml` | `base-fare`, `per-km`, `per-minute`, `driver-share` as exact decimals under `metroride.fare` |
| Fare calculation | `pricing/FareCalculator` | Pure function of distance, ETA and the rate card; no Spring dependency |
| Ledger model | `ledger/Money`, `Account`, `JournalKind`, `Posting`, `JournalEntry` | Records and enums; the balance invariant is checked in the `JournalEntry` constructor; the `quoteHold`, `holdReversal` and `settlement` factories are the only three shapes written |
| Ledger storage | `ledger/LedgerRepository` | Insert-only `JdbcClient` access to `fare.journal_entries` and `fare.postings`; reads rebuild entries through the constructor; `lockQuoteHolds` is the one `select ... for update` |
| Settlement checks | `processing/QuoteHoldShape`, `processing/SettlementException`, `ledger/CorruptLedgerException` | The shape a hold must have to be settled, and the four settlement outcomes with the failure class each carries |
| Idempotency record | `processing/ProcessedEvent`, `ProcessedEventRepository`, `ProcessedEventRecorder` | `insert ... on conflict (event_id) do nothing`, then by type the quote and hold or the reversal and settlement, in one transaction |
| Consumer loop | `consumer/RideEventConsumer` | `XGROUP CREATE ... 0 MKSTREAM` on both streams at start, then on one dedicated thread and connection: `XAUTOCLAIM` per stream every `reclaim-interval`, one `XREADGROUP ... STREAMS s1 s2 > >`, `XACK` after commit or after a confirmed dead letter |
| Failure classes | `consumer/FailureClass`, `consumer/ClassifiedFailure` | Pure mapping from the exception `handle()` saw to `RETRYABLE`, `POISON`, `QUARANTINE` or `FATAL`; an exception that implements `ClassifiedFailure` names its own class and dead-letter reason |
| Failure handling | `consumer/FailureHandler`, `consumer/ConsumerHalt` | Leaves the entry pending, dead-letters it, or halts the consumer; owns the one `XACK` in the service; the halt fails `/readyz` |
| Entry age | `consumer/StreamEntryAge` | Age of an entry from the millisecond timestamp in its stream ID; logged, never decided on |
| Dead letters | `consumer/DeadLetterPublisher`, `events/DeadLetter` | `XADD` to `events.dead_letter` in the shape `publishDeadLetter` in dispatch-service writes; `DeadLetter` mirrors `events.DeadLetter` in `events.go` |
| Outbox storage | `outbox/OutboxRepository` | The statements of `shared/pkg/outbox` against `fare.event_outbox`: `enqueue` in the settlement's transaction, then the relay's take-batch, mark-published and mark-failed |
| Outbox relay | `outbox/OutboxRelay`, `outbox/RetryBackoff`, `outbox/OutboxProperties` | Own thread and connection; every `poll-interval` one batch under `for update skip locked`, `XADD` each row, record success or a capped exponential retry; at-least-once, like the Go relays |
| Published contract | `events/FareSettled` | Field-for-field match with `events.FareSettled` in `events.go`; amounts as strings |
| Schema | `db/migration/V1__processed_events.sql`, `V2__ledger.sql`, `V3__one_hold_and_one_settlement_per_ride.sql`, `V4__event_outbox.sql` | Flyway owns the `fare` schema; Hibernate validates the `processed_events` mapping, the ledger tables, the V3 per-ride unique indexes and the outbox table are checked by the integration tests |
| Endpoints | `web/HealthController`, `web/MetricsController`, `web/LedgerController` | `/healthz`, `/readyz`, `/metrics` with the same paths and JSON as `shared/pkg/httpx/httpx.go`; `GET /v1/rides/{ride_id}/ledger` |

### Processing rule

For each stream entry, from either stream (the type decides, never the stream it came on):

1. Decode the `event` field into an `Envelope`.
2. In one transaction:
   1. insert `(event_id, stream, event_type, processed_at)` into `fare.processed_events`
      with `ON CONFLICT DO NOTHING`; if nothing was inserted the envelope is a duplicate and
      the transaction ends here;
   2. if the type is `ride_assigned`, decode the payload, compute the quote, and append a
      `quote_hold` journal entry with two postings;
   3. if the type is `ride_completed`, decode the payload, lock the ride's `quote_hold`
      (`select ... for update`), check that there is exactly one (two cannot exist since V3;
      finding two is fatal) and that it has the shape the service writes, check that the ride
      has no `settlement` yet, append a `hold_reversal` and a `settlement`, both with this
      event's ID as `source_event_id` (see "Settlement"), and insert one `fare_settled` row for
      `events.ride.fares` into `fare.event_outbox` (see "Publication");
   4. any other type is only recorded.
3. After the transaction commits, `XACK` the entry. The outbox relay publishes the row on its
   next pass.

The transaction is limited to `metroride.postgres.timeout-seconds` (2s, the Go services'
PostgreSQL deadline). Spring hands the remaining time to every statement in it, whether Hibernate
or the ledger's `JdbcClient` issues it, so a write stuck on a lock is cancelled and the whole
transaction rolls back: no event row without its journal entry and no journal entry without its
event row. What happens to the entry after a failure is decided by its failure class, below.

Two deliveries of the same envelope that arrive at the same moment are serialised by the primary
key of `fare.processed_events`: the second insert waits for the first transaction to commit, then
lands on the conflict clause and returns without touching the ledger. The unique key on
`journal_entries (source_event_id, kind)` is a backstop for a writer that bypasses the recorder,
not the mechanism the service relies on. It is a compound key, not `source_event_id` alone,
because one completion event produces both a `hold_reversal` and a `settlement`.

Two *different* completion events for one ride (a dead letter replayed by hand next to the
original, or a relay duplicate that was given a new ID) are not caught by either of those keys:
their event rows do not collide. What serialises them is the row lock the settlement takes on
the ride's `quote_hold`: the second transaction waits on it until the first has committed, then
finds the ride settled and is quarantined as `already_settled`. This is the one new protection
this service has for running more than one instance, and it covers exactly one case, the
settlement of one ride. Everything else about multiple instances is as before (see "What is true
after this").

### Failure handling and pending-entry recovery

Every failure of step 1 or 2 is mapped by `FailureClass.of` to one of four classes. An exception
that implements `ClassifiedFailure` (`SettlementException`, `CorruptLedgerException`) is asked
first and names its own class; the rules below apply to everything else.

| Class | Exceptions | What happens to the entry |
| --- | --- | --- |
| `POISON` | `EnvelopeDecodeException` (the entry is not an envelope), `FareQuoteException` (the payload cannot be quoted), `SettlementException` with reason `payload` (a `ride_completed` whose payload is missing, undecodable or has no `ride_id`), `DataIntegrityViolationException` whose root SQLSTATE is class 22 (PostgreSQL or the driver refused a value: a NUL character in a text field, an invalid byte sequence, a numeric overflow) | Dead-lettered immediately, then acknowledged |
| `QUARANTINE` | `SettlementException` with reason `already_settled` (the ride has a `settlement` from another event, found under the hold's lock or refused by the per-ride settlement index); `CorruptLedgerException` (the ride's hold rows are not the entry this service writes: no postings, unbalanced, an unknown account or kind, a wrong account or side); `LedgerConflictException` (a `ride_assigned` for a ride that already has a `quote_hold`, refused by the per-ride index `journal_entries_one_quote_hold_per_ride`; reason `duplicate_hold`) | As poison: dead-lettered immediately under the reason the exception names, then acknowledged. Settlement reasons and `corrupt_hold` are counted on `metroride_fare_settlement_failures_total`; `duplicate_hold` only on the dead-letter counter, since the failed event is an assignment |
| `FATAL` | `InvalidDataAccessResourceUsageException` (bad SQL, wrong column type), `InvalidDataAccessApiUsageException` (a repository was misused, or a constructor threw inside one), any `DataIntegrityViolationException` other than the two per-ride indexes above (class 23, a constraint the writer cannot reach unless the schema or the data is already wrong), `SettlementException` with reason `ambiguous_hold` (two `quote_hold` rows for one ride, a state the V3 index makes unrepresentable, so the index is gone or the schema has drifted; the check stays so the service never settles against the first of several holds) | Left pending; the consumer halts and `/readyz` fails with the reason until the deployment is fixed and the service restarted |
| `RETRYABLE` | `SettlementException` with reason `missing_hold` (the ride has no `quote_hold` yet), every other `DataAccessException` (cancelled lock wait, lost connection, deadlock), every `TransactionException`, and anything unforeseen | Left pending and delivered again by the reclaim pass; dead-lettered when its `max-deliveries`-th delivery fails |

Why the settlement outcomes fall where they do. A missing hold says nothing is wrong: the
assignment and the completion of one ride travel on two streams through two outbox relays, and
the completion can arrive first under ordinary scheduling, so the entry waits in the pending list
and the reclaim pass retries it every `reclaim-interval` until the hold exists (see "Delivery cap,
not age" for the window). A corrupt hold is the opposite: the message may be perfectly fine, the
ride's ledger is not, and no number of retries repairs an append-only ledger. It is not fatal
either, because it is one ride's problem, and halting would let that one ride stop every other
ride's quotes and settlements; so it is set aside like poison but under its own reason. A
duplicate hold is the mirror image: the ledger is fine and the second assignment event is the
odd one out, refused by the database before it can write anything. Two holds for one ride are
neither: V3 makes that state impossible, so seeing it means the database is not what the code
assumes, which is the deployment's problem and halts the consumer rather than let it settle
against a hold it picked. `already_settled` is quarantined like a corrupt hold and is the last line against
settling one ride twice: rider-service's status guard never publishes a second completion, but a
dead letter replayed by hand can. A payload without a `ride_id` is poison, deliberately not a
missing hold: an entry that can never name its ride would otherwise be retried for its whole
delivery budget as "assignment not here yet".

`CorruptLedgerException` is a plain `RuntimeException`, not a `DataAccessException` and not an
`IllegalArgumentException`, on purpose. `LedgerRepository` is a `@Repository`, and Spring
translates an `IllegalArgumentException` thrown inside one (the `JournalEntry` constructor
refusing rows without postings, say) into `InvalidDataAccessApiUsageException`, which is fatal
above; the repository therefore wraps the constructor's refusal into `CorruptLedgerException`,
which the translation leaves alone. The balance rule itself still lives only in the constructor;
the hold-shape check (two postings, a positive `rider_receivable` debit and an equal `fare_hold`
credit) lives in the settlement code, because balance cannot tell a hold from any other balanced
pair.

Spring's own transient/non-transient split is not used because it files a refused connection under
non-transient. `DataIntegrityViolationException` is split by the SQLSTATE at the root of the chain
because Spring files two different things under it: a value the database refuses (class 22) is the
entry's fault and will be refused on every delivery, so it must not be able to halt the service;
a violated constraint (class 23) means the data or the schema is already wrong. An unforeseen exception is retried rather than dead-lettered on sight: it is most
likely a bug, but a bounded number of deliveries costs little and keeps the work for a fix to
recover. A fatal failure is neither retried nor dead-lettered because it is not the entry's: every
entry would fail the same way, and dead-lettering them one by one would empty the stream into
`events.dead_letter` after a bad deploy.

**Reclaim pass.** The consumer thread runs `XAUTOCLAIM <stream> <group> <consumer> <min-idle>
<cursor> COUNT <batch-size>` before its next `XREADGROUP` whenever `reclaim-interval` has elapsed,
and the first time before it reads anything. Every claimed entry goes through the same `handle()`
as a new one, on the same thread, so the recorder's single-writer model is unchanged and no
scheduler or second thread exists. The cursor is the ID Redis returned from the previous pass,
saved even when that pass claimed nothing; `0-0` means the previous scan reached the end of the
pending list and this one starts over. Redis scans at most ten times `COUNT` entries per call, so a
pass that always restarted at the head would claim the same failing entries every time and never
reach the ones behind them; with the cursor a long pending list is walked in turn. Claiming resets
an entry's idle time, so an entry that fails again waits another `reclaim-min-idle` before a pass
can take it: while the pending list is short, a retryable failure is retried within
`reclaim-interval + reclaim-min-idle` of its previous delivery, 10s with the defaults. The
delivery count of a reclaimed entry is fetched with one `XPENDING` over the claimed range; the age
from the stream ID is logged next to it (`message_id`, `age_seconds`, `delivery_count`).

**Delivery cap, not age.** A retryable failure on an entry's `max-deliveries`-th delivery
(25) dead-letters it. Redis keeps the delivery count in the pending entry list, so it survives a
restart of this service, and it only redelivers an entry idle for at least `reclaim-min-idle`, so
25 deliveries guarantee at least 24 × 5s = 120s of retrying counted from the first delivery,
however old the entry already was when the service first saw it and however long the service was
down before that. Under load the window is longer, never shorter. The guarantee is sized for the next
failure mode: the assignment and the completion of one ride will arrive on two streams from two
outbox relays that poll every 250ms and back off up to 30s between attempts after a failed publish
(`shared/pkg/outbox`). One failed publish therefore delays an assignment by up to 30s, and a
completion that arrives first is a retryable "no hold for this ride yet" that clears itself when
the assignment lands; 120s is four times that. The gap is not bounded, though: a relay or its
service that stays down longer delays the assignment for as long as it is down, and a completion
that waits out its 25 deliveries goes to `events.dead_letter` for manual replay. The cap buys time
for the ordinary case, not a proof that the completion is never given up on. The age of the entry, now minus
the timestamp in its stream ID, is deliberately not the input: it counts time the service may have
spent stopped, so after a restart it would dead-letter the whole backlog on its first hiccup, which
is precisely the backlog a restart exists to work through. Age is kept in the logs for diagnosis.

A PostgreSQL outage longer than the window still ends with entries in `events.dead_letter`
(deliveries accumulate while the database is away), replayed by hand. Pausing the consumer while
the database is unreachable is a possible later step; it is not attempted here.

**Dead letter first, acknowledge second.** A dead letter is one `XADD` to `events.dead_letter` on
the consumer's connection; only after Redis confirms it is the original entry `XACK`ed. If the
`XADD` fails the entry stays pending and the next reclaim pass dead-letters it again; if the `XACK`
fails after a confirmed `XADD` the same happens. Both produce a duplicate dead letter, which is
accepted: acknowledging an entry whose dead letter never landed would lose the record entirely,
while a duplicate can be dropped by whoever reads the stream. `payload.original_event_id` is the
key to deduplicate on. The success path and the dead-letter path share the single `XACK` in
`FailureHandler.acknowledge`.

The dead letter is the envelope dispatch-service writes, field for field: a new UUID as `id`,
`type` `dead_lettered`, `source` `fare-service`, `correlation_id` the ride ID, `occurred_at` in UTC,
and a payload with `original_event_id`, `original_event_type`, `ride_id` (omitted when unknown),
`error`, `service` (`fare-service`) and `failed_at` (RFC 3339 with fractional seconds). An entry that
is not a decodable envelope is dead-lettered under its stream message ID with type `decode_failed`.
The contract is pinned by `DeadLetterPublisherTest` against the tag names in `events.go`.

**Halt.** On a fatal failure the consumer thread logs the entry and the exception, records the
reason in `ConsumerHalt`, and leaves its loop; the rest of the batch and everything else pending
stay in the pending list. `/readyz` answers 503 with `{"status":"not_ready","failures":{"consumer":"<reason>"}}` and
`metroride_fare_consumer_halted` reads 1. Nothing clears a halt at runtime: fix the deployment and
restart, and the first reclaim pass of the new process picks the entries up.

**After a halt.** A halt caused by one entry is not cleared by a restart alone: if the entry was
misclassified as fatal, or the fix did not land, the first reclaim pass of the new process claims
the same entry and halts again. The steps, in order:

1. Find the root cause from the halt log line: `stream` and `message_id` identify the entry,
   `XRANGE <stream> <message_id> <message_id>` shows its content, and `reason` on `/readyz`
   carries the exception and its root cause.
2. Fix what is actually wrong: the code, the schema (a new Flyway migration), or the mapping in
   `FailureClass` if the exception should have been poison or retryable. Deploy the fix.
3. Restart the service. The reclaim pass delivers the entry again and it is handled by the fixed
   code. Nothing else is needed for the entries that queued up behind it.
4. Only when someone has decided that the entry must be skipped rather than fixed: first save the
   original entry (`XRANGE <stream> <message_id> <message_id>`), then write a `dead_lettered`
   envelope for it by hand with `XADD events.dead_letter '*' event '<json>'` (the fields are listed
   under "Dead letter first, acknowledge second"; `original_event_id` is the envelope `id` from the
   saved entry), confirm the `XADD` returned an ID, and only then
   `XACK <stream> fare-service <message_id>`. The order is the service's own: an `XADD` that
   succeeded followed by an `XACK` that failed leaves a duplicate dead letter, identified by
   `original_event_id`; the reverse order would lose the record. A bare `XACK` without the dead
   letter is an explicit data-loss operation and the last resort.

What is true after this:

- fare-service reclaims its own pending entries and dead-letters what it gives up on. The Go
  consumers still read only `>` and do not claim pending entries.
- Nothing consumes `events.dead_letter`. The stream is the record; replay is manual.
- A dead letter can appear twice for one entry; see above.
- A second instance of this service can claim an entry its owner has not reached yet: Redis
  measures idle time from the delivery of the whole `XREADGROUP` batch, and the entries of a
  batch are handled one after another, up to about 4s each. `reclaim-min-idle` bounds how soon
  that can happen; it is not a lock. The overlap is safe because the primary key of
  `fare.processed_events` serialises the two writers (one records, the other sees a duplicate),
  and a poison entry may then be dead-lettered twice, which is accepted. Within one instance the
  reclaim pass and the read loop are the same thread and cannot overlap.
- Two instances settling the same ride from two *different* completion events are serialised by
  the `select ... for update` on the ride's `quote_hold`; the second sees the first's settlement
  and quarantines its entry as `already_settled`. That lock cannot order a completion against a
  second, distinct *assignment* for the same ride (the assignment never touches the locked row),
  so the per-ride unique indexes of V3 do that: the second `quote_hold`, or a second `settlement`
  that slipped past the check, is refused by the database and the transaction rolls back whole.
  Together these cover one ride's hold and settlement across instances, and nothing else.

### Fare and ledger

The quote is

```
quote = base_fare + per_km * distance_km + per_minute * eta_seconds / 60
```

computed exactly in `BigDecimal` and rounded to cents once, half up, inside `Money`. No other
class rounds. With the default rate card (2.50 + 1.20/km + 0.30/min) the assignment
`distance_km=1.8612, eta_seconds=223` quotes 5.85.

Each `ride_assigned` produces one journal entry of kind `quote_hold` with two postings, debit
positive and credit negative:

| Account | Amount |
| --- | --- |
| `rider_receivable` | +quote |
| `fare_hold` | −quote |

Accounts are text codes (`rider_receivable`, `fare_hold`, `driver_payable`, `platform_revenue`);
there is no accounts table. Both tables are append-only: a correction is a new reversing entry,
never an update or delete.

### Settlement

Each first-seen `ride_completed` produces two journal entries in the same transaction, both with
the completion event's ID as `source_event_id`. The amount settled, `X`, is the debit on
`rider_receivable` in the ride's `quote_hold`: settlement is by quote, and nothing recomputes a
fare from what actually happened on the ride. The driver's share `D` is `X` times `driver-share`,
rounded to cents once, half up, by `Money.times`; the platform's share is the remainder
`X − D`, so the entry balances by construction and rounding can neither create nor lose a cent.
With `driver-share` 0.80 and the quote above (5.85), `D` is 4.68 and the platform gets 1.17.

| Entry | Account | Amount |
| --- | --- | --- |
| `hold_reversal` | `rider_receivable` | −X |
| `hold_reversal` | `fare_hold` | +X |
| `settlement` | `rider_receivable` | +X |
| `settlement` | `driver_payable` | −D |
| `settlement` | `platform_revenue` | −(X − D) |

The reversal is the hold posting for posting with the opposite sign, so after the three entries
`fare_hold` is back at zero, `rider_receivable` carries `X` once, and `driver_payable` plus
`platform_revenue` carry `−X` between them. The reversal and the settlement are two entries
rather than one so each stays readable on its own: the reversal says "the pre-authorisation is
released", the settlement says "this is what the ride is worth and to whom", and a later
correction can reverse either without touching the other.

A posting of zero is omitted rather than written, because a zero posting is refused by the model.
`driver-share` 0 gives a settlement with only the rider debit and the platform credit; 1 gives
only the rider debit and the driver credit; `X` 0.01 with share 0.80 rounds `D` to 0.01 and omits
the platform posting. The three cases are on `JournalEntry.settlement`.

Before writing, the settlement reads the ride's `quote_hold` rows with `select ... for update`
and classifies what it finds (see the failure table): none, retryable; more than one, fatal
(`ambiguous_hold`, a state V3 forbids); one that is not the two-posting shape `quoteHold` writes,
quarantined as `corrupt_hold`; and, with the lock held, an existing `settlement` on the ride,
quarantined as `already_settled`.

`V3__one_hold_and_one_settlement_per_ride.sql` adds two partial unique indexes, one `quote_hold`
and one `settlement` per `ride_id`. They exist because the checks above run inside one
transaction and cannot see another transaction's uncommitted insert: a completion that has
locked and read hold A cannot stop a second, distinct `ride_assigned` from inserting hold B for
the same ride, and without the index both would commit and leave B un-reversed behind a settled
ride. With it the second hold is refused at insert, the whole transaction (event row included)
rolls back, and the assignment event is quarantined as `duplicate_hold`; a second settlement that
slips past the checked path is refused the same way and reported as `already_settled`.
`LedgerRepository.append` recognises the two indexes by the constraint name in the driver's
error; any other integrity violation stays what Spring made of it and is fatal. The row lock is
still what gives the second completion its clean `already_settled` before writing anything; the
indexes are the backstop for the window the lock cannot close.

The balance rule (postings of one entry sum to zero, at least one posting, no zero posting) is
enforced in the `JournalEntry` constructor and nowhere else. There is no database trigger on
purpose: with a single writer, making the illegal state unrepresentable in the application is
enough, and a trigger would duplicate the rule in a second language with its own tests. A
trigger is defense in depth to add when a second writer appears. The per-ride *cardinality* rules
(one hold, one settlement) are the exception and live in the database since V3, because two
instances of this service are that second writer and no application check can order their
inserts. Reads go through the same
constructor (the query is a left join, so an entry that lost its postings is not hidden), so a
row set that no longer balances or has no postings is refused rather than served.

### Publication

A settled ride is announced with one `fare_settled` envelope on `events.ride.fares`:

```json
{"id":"<new uuid>","type":"fare_settled","source":"fare-service","correlation_id":"<ride_id>",
 "occurred_at":"2026-09-12T10:05:00.123456Z",
 "payload":{"ride_id":"...","rider_id":"rider-42","driver_id":"driver-2","assignment_id":"...",
            "settlement_event_id":"<the ride_completed envelope id>",
            "quote":"5.85","driver_amount":"4.68","platform_amount":"1.17","driver_share":"0.80",
            "settled_at":"2026-09-12T10:05:00.123456Z"}}
```

The payload is `events.FareSettled` in `events.go`, pinned there by a Go test and here by
`FareSettledContractTest`. Amounts are strings, as on the ledger endpoint, so no consumer turns
money into floating point by accident. The figures are copied from the `settlement` entry that
was just written, never recomputed; `settlement_event_id` is the `source_event_id` of both
journal entries, so a consumer can join the event to the ledger.

**Transactional outbox in the `fare` schema.** `settle()` inserts the envelope into
`fare.event_outbox` in the same transaction as the two journal entries, so the event exists
exactly when the entries exist and a rolled-back settlement leaves no event behind. The table has
the columns, primary key `(id, stream)` and partial index of the Go services' `public.event_outbox`
(`shared/pkg/outbox`), so both are inspected with the same SQL; it is a separate table because
`public.event_outbox` belongs to `init.sql` and the Go relays, and this service touches only its
own schema (V1). A Java relay is the price of that boundary.

**The relay.** `OutboxRelay` runs on its own thread and connection, the same pattern as the
consumer. Every `poll-interval` (250ms) it runs one pass in one transaction: take up to
`batch-size` (25) rows with `published_at is null and next_attempt_at <= now()`, ordered by
`next_attempt_at, created_at, id`, `for update skip locked`; `XADD` each to its stream; on
success set `published_at = clock_timestamp()`; on failure add one to `publish_attempts`, set
`last_error`, and push `next_attempt_at` back by the Go backoff (`RetryBackoff`: 250ms doubling
per previous attempt, 30s once the doubled value would reach 15s, pinned to the Go table by
`RetryBackoffTest`); commit the batch. A failed destination does not stop the batch, and ordering
by retry time keeps retries and new rows both moving. Metrics carry the Go names.

**Timeouts are per statement, not per transaction.** The Go relay bounds each statement, the
commit and each publish with its own 2s context and nothing else. Here each statement runs with
a 2s query timeout (`OutboxRepository`), each `XADD` under the 2s Redis command timeout, and the
relay's transaction has no overall budget on purpose. The consumer's transaction timeout would be
wrong here: with Redis slow, one 2s `XADD` would exhaust the batch's budget, the update that
records that failure would be cancelled, the transaction and the backoff with it would roll back,
and the row would be retried every poll. A query timeout covers neither `COMMIT` nor `ROLLBACK`
nor a server that has stopped answering; the datasource's pgjdbc `socketTimeout` (5s, in
`application.yml`) does, for every connection of this service. It is larger than every statement
budget so a statement is always cancelled by its own timeout first; when it fires the connection
is dead, Hikari discards it, and the relay counts a failed pass (`metroride_fare_outbox_pass_failures_total`)
and retries the batch. A commit cut off this way may still complete on the server, in which case
the rows are marked and not published again.

**At-least-once, in the same words as the Go relays.** If the process dies after Redis accepted
an entry and before the transaction recording `published_at` commits, the row is published again
with the same envelope ID. A failure to mark a row published (a cancelled statement, a lost
connection) aborts the batch's transaction for the same reason, and the rows Redis already has
are published again on the next pass. A timed-out `XADD` may also have reached Redis, in which
case the row is retried and the entry appears twice. None of this is deduplicated here: the
envelope ID is stable across attempts, and a consumer of `events.ride.fares` must deduplicate on
it, as this service does for its own streams. Neither the relay nor Redis is a readiness
dependency of the settlement: with Redis away, `record()` still commits the entries and the row,
`/readyz` answers 503 because the consumer's `redis` check fails (consumption is paused, work
already in a transaction still commits), and the rows wait with their backoff until Redis is back.
Shutdown lets the current pass finish and exits; rows still unpublished are published by the
next start.

**What this does not do.** Only `fare_settled` is published: the settlement is the result a
downstream would act on, and this event defines that boundary. No event marks the hold
(`fare_held` is an option if a consumer needs it, not a gap). There is no attempt ceiling and no
outbox dead letter, as in the Go relays; a row that can never be published is retried every 30s
for as long as it exists, and the operational answer to that is the `unpublished` gauge and the
row's `last_error`, not a second table. No consumer of `events.ride.fares` exists in this
repository, so the downstream loop is not demonstrated. The relay-crash window (Redis has the
entry, `published_at` is not yet committed) is not exercised by an automated test on either side:
the Go process-kill test kills `rider-service` while Redis is stopped, i.e. with a committed row
that has not been published at all, and verifies the restart publishes it once; this service's
tests cover the business transaction with Redis away, the relay's backoff and recovery, a commit
cut off by the socket timeout, and the whole chain, not process termination.

### Ledger endpoint

```
GET /v1/rides/{ride_id}/ledger
200 {"ride_id":"...","entries":[
      {"id":1,"kind":"quote_hold","source_event_id":"<ride_assigned id>","created_at":"...",
       "postings":[{"account":"rider_receivable","amount":"5.85"},{"account":"fare_hold","amount":"-5.85"}]},
      {"id":2,"kind":"hold_reversal","source_event_id":"<ride_completed id>","created_at":"...",
       "postings":[{"account":"rider_receivable","amount":"-5.85"},{"account":"fare_hold","amount":"5.85"}]},
      {"id":3,"kind":"settlement","source_event_id":"<ride_completed id>","created_at":"...",
       "postings":[{"account":"rider_receivable","amount":"5.85"},{"account":"driver_payable","amount":"-4.68"},
                   {"account":"platform_revenue","amount":"-1.17"}]}]}
404 {"error":"ledger not found"}
```

Amounts are strings with two decimals so no client turns them into floating point by accident.
Entries are in insertion order, so a settled ride reads hold, reversal, settlement. The endpoint
was not changed for settlement; the two new entries appear because they are rows on the ride.

### Configuration

| Variable | Default | Used for |
| --- | --- | --- |
| `FARE_SERVICE_ADDR` | `:8087` | HTTP listen address, Go `host:port` form |
| `POSTGRES_DSN` | `postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable` | Translated to a JDBC URL plus credentials |
| `REDIS_ADDR` | `localhost:6379` | Redis host and port |
| `CONSUMER_GROUP` | `fare-service` | Consumer group name |
| `CONSUMER_NAME` | `fare-service-1` | Consumer name within the group; every running instance needs its own, or two instances share one pending entry list and cannot tell their deliveries apart |
| `SHUTDOWN_TIMEOUT_SECONDS` | `10` | Graceful shutdown budget, also the consumer drain budget |

Pending-entry recovery is tuned in `application.yml` under `metroride.consumer`, next to the
batch size and block timeout:

| Key | Default | Meaning |
| --- | --- | --- |
| `reclaim-interval` | `5s` | How often the consumer thread runs one `XAUTOCLAIM` before its next read |
| `reclaim-min-idle` | `5s` | How long an entry must have gone without a delivery before that `XAUTOCLAIM` takes it; longer than `metroride.postgres.timeout-seconds` |
| `max-deliveries` | `25` | A retryable failure on this delivery dead-letters the entry; with `reclaim-min-idle` 5s this guarantees at least 120s of retrying since the first delivery, four times the outbox relay's 30s maximum backoff |

The relay is tuned under `metroride.outbox`, defaults being the constants of `shared/pkg/outbox`:

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Whether this instance runs the relay; rows are always enqueued. Off only in tests that start a second context on the same database |
| `poll-interval` | `250ms` | How often the relay thread runs one pass; also the first retry delay |
| `batch-size` | `25` | Rows taken per pass with `for update skip locked` |
| `max-retry-backoff` | `30s` | Cap on the delay between attempts of one failed row |

The rate card lives in `application.yml` under `metroride.fare`, not in the environment:

| Key | Default | Meaning |
| --- | --- | --- |
| `base-fare` | `2.50` | Charged on every ride |
| `per-km` | `1.20` | Per kilometre of `distance_km` |
| `per-minute` | `0.30` | Per minute of `eta_seconds` |
| `driver-share` | `0.80` | Driver's fraction of a settled fare, between 0 and 1; `driver_payable` is credited `quote × driver-share` rounded once, `platform_revenue` the remainder |

### Metrics

| Metric | Labels | Meaning |
| --- | --- | --- |
| `metroride_fare_events_processed_total` | `service`, `stream`, `outcome=recorded\|duplicate` | Envelopes recorded or skipped as duplicates |
| `metroride_fare_quotes_total` | `service`, `kind=quote_hold` | Quote holds written, counted after the commit; unchanged by settlement, which has its own counter below |
| `metroride_fare_journal_entries_total` | `service`, `kind=quote_hold\|hold_reversal\|settlement` | Journal entries written by kind, counted after the commit; a settled ride adds one to each |
| `metroride_fare_quote_failures_total` | `service`, `reason=payload\|calculation` | `ride_assigned` envelopes whose payload did not decode or whose figures the calculator rejected; the transaction rolled back and the entry is dead-lettered as poison |
| `metroride_fare_settlement_failures_total` | `service`, `reason=missing_hold\|ambiguous_hold\|corrupt_hold\|already_settled` | `ride_completed` envelopes that could not be settled, counted once per failed delivery, so `missing_hold` grows by one per retry of a completion that is waiting for its assignment |
| `metroride_fare_events_reclaimed_total` | `service`, `stream` | Pending entries delivered again by the reclaim pass |
| `metroride_fare_dead_letters_total` | `service`, `stream`, `reason=poison\|max_deliveries_reached\|duplicate_hold\|corrupt_hold\|already_settled` | Entries written to `events.dead_letter`; counted after Redis confirmed the `XADD`, before the `XACK`. The reason is only here: the dead letter JSON has no `reason` field, the specific cause is in its `error` text |
| `metroride_fare_dead_letter_publish_failures_total` | `service`, `stream` | Dead-letter `XADD`s Redis did not confirm; the entry stayed pending |
| `metroride_fare_consumer_halted` | `service` | Gauge, 1 once the consumer has stopped on a fatal failure |
| `metroride_outbox_events_published_total` | `service`, `stream` | Outbox rows published to their stream, counted after the batch's commit (same name as the Go relays' counter) |
| `metroride_outbox_publish_failures_total` | `service`, `stream` | Failed `XADD` attempts, counted after the batch's commit (same name as the Go relays' counter) |
| `metroride_fare_outbox_unpublished` | `service` | Gauge, rows of `fare.event_outbox` with `published_at` null, refreshed after every pass; no Go counterpart |
| `metroride_fare_outbox_pass_failures_total` | `service` | Relay passes whose transaction failed as a whole (a statement or commit timed out, the connection dropped, a row could not be marked); the batch is retried; no Go counterpart |
| `metroride_stream_consume_errors_total` | `service`, `stream` | Failed reads and undecodable entries (same name as the Go shared counter) |
| `metroride_dependency_errors_total` | `service`, `dependency=postgres\|redis` | Failed dependency calls (same name as the Go shared counter) |

Prometheus scrapes `fare-service:8087/metrics`; see `infrastructure/prometheus/prometheus.yml`.

## Build and test

Requirements: JDK 21 (CI and the image build stage use Temurin; the runtime image is
distroless `java21-debian12`), Docker for the integration tests.

```bash
cd services/fare-service
./mvnw -B verify          # unit tests (surefire) + Testcontainers integration tests (failsafe)
./mvnw -B test            # unit tests only, no Docker needed
```

Unit tests cover the calculator (zero distance, time only, very long distance, half-up rounding
on exact `.5` boundaries, negative and non-finite inputs rejected), `Money` (including `times`,
which rounds the exact product once), the `JournalEntry` invariants (unbalanced, one-sided,
empty and null posting lists are rejected; the list is copied and immutable) and factories (the
settlement split at 5.85 and share 0.80, the remainder going to the platform, the three
zero-posting cases, the reversal being the hold negated posting for posting), and
`QuoteHoldShapeTest` (a missing, duplicated, wrong-account, wrong-side or unequal posting is a
corrupt hold). For publication: `RetryBackoffTest` (the delay after 0 to 100 attempts against the
Go table), `FareSettledContractTest` (payload names against the `events.go` tags, `occurred_at`
as a string), `OutboxRelayTest` (one pass against a mocked Redis: a timed-out `XADD` is recorded
with the Go backoff and the batch goes on, a row that cannot be marked published aborts the pass
and counts nothing), and in `ProcessedEventRecorderTest` that a settlement enqueues one
`fare_settled` with the ledger's figures and a refused settlement enqueues nothing.

The integration tests share one `postgres:16-alpine` and one `redis:7-alpine` container
(`IntegrationTestSupport`). `RideEventConsumerIT` covers the consumer path: one row per
envelope, zero pending entries, a second delivery acknowledged without a second row, a
lock-waiting write cancelled by the transaction timeout, and the same on the completions
stream (an assignment and its completion each recorded under their own stream and acknowledged
there; a completion whose write was cancelled is reclaimed from the completions stream).
`QuoteLedgerIT` covers the ledger: a `ride_assigned` produces one event row, one `quote_hold`
entry and two postings summing to zero; a redelivery adds nothing; two threads recording the
same event concurrently produce one entry; a journal insert blocked until the timeout rolls back
the event row with it and the reclaimed delivery writes both once the lock is gone; the ledger
endpoint returns the Go-style JSON.

`SettlementIT` covers settlement on the same containers: a completion after its assignment
writes the reversal and the settlement (three entries, seven postings summing to zero,
`driver_payable` plus `platform_revenue` equal to −X, one increment per kind, the endpoint
listing hold, reversal, settlement in that order) and a redelivery of the same completion changes
nothing; a completion that arrives *before* its assignment fails as `missing_hold`, stays pending,
and settles on the reclaimed delivery once the assignment has landed, within
`reclaim-interval + reclaim-min-idle`, which is the case the reclaim pass exists for; two threads
recording two different completion events for one ride settle it once, the other failing as
`already_settled`, which is the `for update` on the hold at work; a second, distinct
`ride_assigned` for a ride whose hold is committed is dead-lettered as `duplicate_hold` with the
ledger unchanged and the ride still settling afterwards, and the same with the first hold still
uncommitted when the second arrives (the consumer's insert waits on the index and is refused once
the hold commits); the settlement index refuses a second `settlement` at the repository as a
`LedgerConflictException` (the recorder's `already_settled`), while a violation of the
`(source_event_id, kind)` key stays Spring's `DuplicateKeyException` and fatal; a hold credited to
`driver_payable` and a hold without postings each dead-letter the completion under
`corrupt_hold`, with the reason in both the metric label and `payload.error`, the entry
acknowledged, nothing recorded, and the consumer still running with `/readyz` 200 (two holds for
one ride can no longer be arranged against the real schema; `ProcessedEventRecorderTest` covers
that branch against a mocked ledger and asserts it is fatal and writes nothing); a
second completion event after settlement is dead-lettered as `already_settled` and the ledger
is unchanged; a completion without a `ride_id` is dead-lettered as `poison` and never counted as
a missing hold; and a settlement blocked on `fare.postings` past the timeout (the table is held
in `EXCLUSIVE` mode, which lets the hold lookup read but blocks the reversal's first posting
insert) rolls back the event row, the reversal row and the settlement, leaves the hold intact,
and settles on the reclaimed delivery once the lock is released.

Pending-entry recovery has its own tests. Unit: `FailureClassTest` (one case per branch of the
class mapping, including the subclasses the consumer really sees), `StreamEntryAgeTest` (the age
from a stream ID, with and without the `-<seq>` suffix, from the future, and IDs with no
timestamp), `DeadLetterPublisherTest` (the dead-letter JSON checked name by name against the tags
in `events.go`), and `FailureHandlerTest`, which proves the guarantees against a mocked Redis
because no integration test can arrange them: no `XACK` unless the dead-letter `XADD` was
confirmed, `XACK` only after the `XADD`, a failed `XACK` leaves the entry pending with the dead
letter already counted, a retryable failure is left pending below the cap and dead-lettered at it,
and a fatal failure neither acknowledges nor dead-letters and halts the consumer.

Integration, all on the containers from `IntegrationTestSupport`. `PendingEntryRecoveryIT` sends
a `ride_assigned` with a negative distance, an entry whose `event` field is not JSON, and an
envelope whose event ID contains a NUL character (refused as SQLSTATE 22023 by the driver, or 22021 by the server when the driver sends the parameter in binary), and
asserts the dead letter's content, the acknowledgement, the empty pending list, the untouched
tables, and for the NUL entry that the consumer did not halt. The lock-wait tests in `RideEventConsumerIT` and `QuoteLedgerIT` assert that the
entry is reclaimed and recorded within `reclaim-interval + reclaim-min-idle` of the lock being
released; nothing acknowledges by hand any more. Three classes run a consumer of their own (a
`@TestPropertySource` context on the same containers, each reading its own stream so it never
competes with the shared context's consumer): `DeliveryCapIT` holds the lock for good with
`max-deliveries` 3 and asserts the entry is dead-lettered on its third delivery with
`reason=max_deliveries_reached` and acknowledged; `OldEntryRecoveryIT` publishes an entry with a
stream ID from 2001, fails it once, and asserts it is retried and recorded rather than
dead-lettered for its age; `ReclaimCursorIT` keeps two entries failing with `batch-size` 2 and
asserts the third, behind them, is still reclaimed and recorded.

Not automated: a fatal failure end to end. Producing one against the real schema means breaking
the schema for every other test in the JVM; the halt is covered by `FailureHandlerTest` and its
readiness effect by `HealthControllerTest`.

`OutboxIT` covers publication in three layers: the whole chain (`ride_assigned` and
`ride_completed` in through the streams, one `fare_settled` out with the ledger's figures, no
second row or entry for a redelivered or a second completion), the business transaction and the
relay under a Redis fault (the Redis container is paused with the Docker API; three settlements
still commit their entries and rows, the relay records each failure with the backoff and the
`unpublished` gauge and `/readyz` reflect it, and after the container is unpaused every row is
published and marked; the stream is then asserted to hold at least one copy per ride, all
identical, because a timed-out `XADD` may have reached Redis), a settlement rolled back by a
lock leaving no row until its reclaimed delivery succeeds, and thirty settlements published with
thirty distinct envelope IDs, and a commit that hangs on the server (a deferred trigger sleeping
in the commit of a private stream's row) being cut off by the socket timeout at about 5s while the
row is still unmarked, with the row marked exactly once afterwards.

### The whole chain through the Compose stack

`OutboxIT` feeds `ride_assigned` and `ride_completed` into the streams itself. The check that the
Go services really produce what this service consumes, and that what this service publishes is
what the Go contract describes, is a Go test against a running stack:

```bash
bash scripts/fare-e2e-test.sh        # from the repository root; needs Docker, Go optional
```

It starts an isolated Compose project (core Go services plus this profile), creates a ride
through rider-service, waits for dispatch's assignment and for this service's `quote_hold`,
completes the ride through rider-service, and then checks the ledger (one hold, one reversal
that negates it, one settlement by the held quote with the driver's share rounded once and the
platform the remainder, both under the completion envelope's ID), `fare_settled` on
`events.ride.fares` (IDs, `settlement_event_id`, the amounts as strings equal to the ledger,
`driver_share` equal to the value the script configured with `FARE_DRIVER_SHARE`) and the
`fare.event_outbox` row (one, published, same envelope). A second completion must be a `409`
that adds nothing. Stages, assertions and limits are in
[docs/testing-and-ci.md](../../docs/testing-and-ci.md#ride-to-fare-settlement-flow). CI runs
it in the `fare-end-to-end` job; that job validates the chain in Compose and does not add this
service to the published images, the Helm chart or the KinD validation.

## Run in Compose

The service sits behind the optional `fare` Compose profile, so it is not part of the default stack:

```bash
docker compose --profile fare up -d --build      # --build so the image is the checkout, not a cached one
curl -s localhost:8087/healthz   # {"status":"ok"}
curl -s localhost:8087/readyz    # {"status":"ready"}
curl -s localhost:8087/metrics | grep metroride_fare
```

Drive a ride through the stack, read its ledger once dispatch has assigned it, then complete it
and read the ledger again:

```bash
curl -s localhost:8087/metrics | grep journal_entries_total          # note the three counts
RIDE=$(curl -s -X POST localhost:8080/v1/rides -H 'Content-Type: application/json' \
  -d '{"rider_id":"rider-42","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["ride_id"])')
until curl -s localhost:8080/v1/rides/$RIDE | grep -q '"status":"assigned"'; do sleep 0.5; done
# "assigned" is dispatch's commit; the assignment event still has to cross the relay and this
# consumer, so poll the ledger for the hold rather than reading it once.
until curl -s localhost:8087/v1/rides/$RIDE/ledger | grep -q quote_hold; do sleep 0.5; done
curl -s localhost:8087/v1/rides/$RIDE/ledger                          # the quote_hold
curl -s -X POST localhost:8080/v1/rides/$RIDE/complete                # 202 {"ride_id":...,"status":"completed","event_id":...}
sleep 2
curl -s localhost:8087/v1/rides/$RIDE/ledger                          # quote_hold, hold_reversal, settlement
curl -s -X POST localhost:8080/v1/rides/$RIDE/complete                # 409 {"error":"ride is completed"}
curl -s localhost:8087/metrics | grep journal_entries_total          # each kind up by one
docker compose logs fare-service | grep '"event recorded"'
docker compose exec postgres psql -U metroride -d metroride \
  -c "select j.kind, p.account, p.amount from fare.journal_entries j join fare.postings p on p.journal_entry_id = j.id where j.ride_id = '$RIDE' order by j.id, p.id"
```

```

The settlement is also on `events.ride.fares` by then, and its outbox row is marked published:

```bash
docker compose exec -T redis redis-cli --raw XRANGE events.ride.fares - + | grep "$RIDE"   # one fare_settled
docker compose exec postgres psql -U metroride -d metroride \
  -c "select id, stream, publish_attempts, published_at, last_error from fare.event_outbox where aggregate_id = '$RIDE'"
curl -s localhost:8087/metrics | grep outbox                          # published +1, failures unchanged, unpublished 0
```

Fault behaviour (Redis away while a settlement commits, the relay's backoff, recovery) is not
demonstrated by hand: stopping Redis in Compose also stops the relay that carries the completion
to this service, and pausing the container freezes the consumer's read as well, so neither lands
between "consumed" and "published". `OutboxIT` arranges each of those states directly.

The postings of the three entries sum to zero. Completion is a command on rider-service because,
in this simplified architecture, rider-service is the aggregate owner of `rides`; in a real
system the completion event would usually come from a trip or driver workflow. `in_progress`
exists in the `rides` status check but nothing writes it: the transition is `assigned` to
`completed` directly, by choice.

Replaying the same entry proves idempotency:

```bash
# --raw prints the JSON unescaped; the value to copy is the line after "event".
docker compose exec -T redis redis-cli --raw XREVRANGE events.ride.assignments + - COUNT 1
docker compose exec -T redis redis-cli XADD events.ride.assignments '*' event '<that JSON line>'
docker compose logs fare-service | grep '"duplicate event skipped"'
```

Two manual checks of the recovery path against the Compose stack. A poison entry:

```bash
docker compose exec -T redis redis-cli XADD events.ride.assignments '*' event not-json
docker compose exec -T redis redis-cli --raw XRANGE events.dead_letter - +      # one dead_lettered from fare-service
docker compose exec -T redis redis-cli XPENDING events.ride.assignments fare-service   # 0
docker compose logs fare-service | grep '"entry dead-lettered"'
```

A retryable failure that clears: stop PostgreSQL, publish an assignment, watch it stay pending,
start PostgreSQL, watch the reclaim pass record it.

```bash
docker compose stop postgres
docker compose exec -T redis redis-cli XADD events.ride.assignments '*' event \
  '{"id":"<new uuid>","type":"ride_assigned","source":"dispatch-service","correlation_id":"<ride uuid>","occurred_at":"2026-09-08T12:00:00Z","payload":{"ride_id":"<ride uuid>","rider_id":"rider-42","driver_id":"driver-2","distance_km":1.8612,"eta_seconds":223,"assignment_id":"<uuid>"}}'
docker compose logs fare-service | grep 'entry left pending'
docker compose exec -T redis redis-cli XPENDING events.ride.assignments fare-service   # 1
docker compose start postgres
sleep 15
docker compose logs fare-service | grep '"pending entry reclaimed"'
curl -s localhost:8087/v1/rides/<ride uuid>/ledger                                     # the quote_hold
```

## Not in this service yet

- Settlement is by quote only. There is no metering of actual distance or time, no adjustment
  entry, and no cancellation flow (a `cancelled` ride keeps its hold).
- Only `fare_settled` is published; no event marks the hold, and nothing in this repository
  consumes `events.ride.fares`. See "Publication" for what the outbox does not do (no attempt
  ceiling, no outbox dead letter, no deduplication, no process-kill test on the Java side).
- Multi-instance protection covers exactly one case: two instances settling the same ride. Two
  instances quoting the same assignment are serialised by the `processed_events` primary key as
  before; nothing else is coordinated between instances.
- No database trigger for the ledger balance rule; see "Fare and ledger" for when to add one.
- No consumer of `events.dead_letter` and no replay tool: a dead-lettered entry is inspected
  and replayed by hand (the original entry is still in its stream, acknowledged but not deleted).
- No deduplication of dead letters; `original_event_id` is the key for whoever adds it.
- No pause while PostgreSQL is unreachable: deliveries keep accumulating during an outage, so
  one longer than the retry window ends with entries in the dead-letter stream.
- No Helm chart entry. Compose is the only runtime for this service so far.
