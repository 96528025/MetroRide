# fare-service

A Java service in the MetroRide monorepo. It consumes `events.ride.assignments` from Redis
Streams through the consumer group `fare-service`, records every envelope it sees once in its
own PostgreSQL schema, and for each first-seen `ride_assigned` quotes the fare from the
assignment's distance and ETA and holds that quote in a double-entry ledger. The hold is a
pre-authorisation, not a charge: nothing settles it yet, because no service publishes
`ride_completed`. An entry it cannot handle is either retried from the consumer group's pending
list or written to `events.dead_letter`; see "Failure handling and pending-entry recovery".

## What is here

| Piece | Where | Notes |
| --- | --- | --- |
| Environment mapping | `config/MetroRideEnvironmentPostProcessor` | Reads `FARE_SERVICE_ADDR`, `POSTGRES_DSN`, `REDIS_ADDR` (same names and defaults as `shared/pkg/config/config.go`) and derives the Spring properties |
| Consumer settings | `config/ConsumerProperties`, `application.yml` | Group and consumer name come from `CONSUMER_GROUP` and `CONSUMER_NAME` through the same post-processor; batch size and block timeout live in `application.yml` |
| Envelope contract | `events/Envelope`, `events/RideAssigned`, `events/EnvelopeCodec` | Field-for-field match with `shared/pkg/events/events.go`; the stream entry field is `event`, as written by `events.Publish` |
| Rate card | `pricing/FareProperties`, `application.yml` | `base-fare`, `per-km`, `per-minute`, `driver-share` as exact decimals under `metroride.fare` |
| Fare calculation | `pricing/FareCalculator` | Pure function of distance, ETA and the rate card; no Spring dependency |
| Ledger model | `ledger/Money`, `Account`, `JournalKind`, `Posting`, `JournalEntry` | Records and enums; the balance invariant is checked in the `JournalEntry` constructor |
| Ledger storage | `ledger/LedgerRepository` | Insert-only `JdbcClient` access to `fare.journal_entries` and `fare.postings`; reads rebuild entries through the constructor |
| Idempotency record | `processing/ProcessedEvent`, `ProcessedEventRepository`, `ProcessedEventRecorder` | `insert ... on conflict (event_id) do nothing`, then the quote and ledger entry, in one transaction |
| Consumer loop | `consumer/RideAssignmentConsumer` | `XGROUP CREATE ... 0 MKSTREAM` on start, then on one dedicated thread and connection: `XAUTOCLAIM` every `reclaim-interval`, `XREADGROUP ... >`, `XACK` after commit or after a confirmed dead letter |
| Failure classes | `consumer/FailureClass` | Pure mapping from the exception `handle()` saw to `RETRYABLE` or `POISON`; the only place that decision is made |
| Entry age | `consumer/StreamEntryAge` | Age of an entry from the millisecond timestamp in its stream ID; the input to the retry budget |
| Dead letters | `consumer/DeadLetterPublisher`, `events/DeadLetter` | `XADD` to `events.dead_letter` in the shape `publishDeadLetter` in dispatch-service writes; `DeadLetter` mirrors `events.DeadLetter` in `events.go` |
| Schema | `db/migration/V1__processed_events.sql`, `V2__ledger.sql` | Flyway owns the `fare` schema; Hibernate validates the `processed_events` mapping, the ledger tables are checked by the integration tests |
| Endpoints | `web/HealthController`, `web/MetricsController`, `web/LedgerController` | `/healthz`, `/readyz`, `/metrics` with the same paths and JSON as `shared/pkg/httpx/httpx.go`; `GET /v1/rides/{ride_id}/ledger` |

### Processing rule

For each stream entry:

1. Decode the `event` field into an `Envelope`.
2. In one transaction:
   1. insert `(event_id, stream, event_type, processed_at)` into `fare.processed_events`
      with `ON CONFLICT DO NOTHING`; if nothing was inserted the envelope is a duplicate and
      the transaction ends here;
   2. if the type is `ride_assigned`, decode the payload, compute the quote, and append a
      `quote_hold` journal entry with two postings; any other type is only recorded.
3. After the transaction commits, `XACK` the entry.

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
because one completion event will later produce both a `hold_reversal` and a `settlement`.

### Failure handling and pending-entry recovery

Every failure of step 1 or 2 is mapped by `FailureClass.of` to one of two classes:

| Class | Exceptions | What happens to the entry |
| --- | --- | --- |
| `POISON` | `EnvelopeDecodeException` (the entry is not an envelope), `FareQuoteException` (the payload cannot be quoted), anything not listed below | Dead-lettered immediately, then acknowledged |
| `RETRYABLE` | `DataAccessException`, `TransactionException` (every PostgreSQL failure: cancelled lock wait, lost connection, failed commit) | Left in the pending list and delivered again by the reclaim pass; dead-lettered once its age exceeds `retry-budget` |

Exceptions of any other type are programming errors (all PostgreSQL access goes through Spring's
exception translation, so nothing transient arrives under another type) and are treated as poison
so they surface at once instead of after two minutes of identical failures.

**Reclaim pass.** The consumer thread runs `XAUTOCLAIM <stream> <group> <consumer> <min-idle> 0-0
COUNT <batch-size>` before its next `XREADGROUP` whenever `reclaim-interval` has elapsed, and the
first time before it reads anything. Every claimed entry goes through the same `handle()` as a new
one, on the same thread, so the recorder's single-writer model is unchanged and no scheduler or
second thread exists. Claiming resets the entry's idle time, so an entry that fails again waits
another `reclaim-min-idle` before the next pass sees it: a retryable failure is retried within
`reclaim-interval + reclaim-min-idle` of its previous delivery, 10s with the defaults. A pass
claims at most `batch-size` entries; a longer backlog drains one batch per interval. The delivery
count of a reclaimed entry is fetched with one `XPENDING` over the claimed range and logged
(`delivery_count`, next to `message_id` and `age_seconds`); it takes part in no decision.

**Retry budget by age, not by count.** An entry's age is now minus the millisecond timestamp in
its stream ID, which records the producer's `XADD`. A retryable failure of an entry older than
`retry-budget` (120s) is dead-lettered; a younger one is left pending. The budget is time because
the failure it exists for is a matter of time: the assignment and the completion of one ride will
arrive on two streams from two outbox relays that poll every 250ms and back off up to 30s after a
failed publish (`shared/pkg/outbox`), so a completion can precede its assignment by up to that long
under normal operation, and "no hold for this ride yet" is then a retryable failure that clears
itself when the assignment lands. A delivery-count cap cannot express that: with a fixed reclaim
interval, a cap of N means "give up after about N × 10s" only while nothing else is pending, and
an early-arriving completion would be dead-lettered before its assignment had even been published.
120s is four times the relay's maximum backoff. The age counts from `XADD`, not from first delivery
(Redis keeps no first-delivery time), so entries that failed throughout a PostgreSQL outage longer
than the budget end up in the dead-letter stream and are replayed by hand.

**Dead letter first, acknowledge second.** A dead letter is one `XADD` to `events.dead_letter` on
the consumer's connection; only after Redis confirms it is the original entry `XACK`ed. If the
`XADD` fails the entry stays pending and the next reclaim pass dead-letters it again; if the `XACK`
fails after a confirmed `XADD` the same happens. Both produce a duplicate dead letter, which is
accepted: acknowledging an entry whose dead letter never landed would lose the record entirely,
while a duplicate can be dropped by whoever reads the stream. `payload.original_event_id` is the
key to deduplicate on.

The dead letter is the envelope dispatch-service writes, field for field: a new UUID as `id`,
`type` `dead_lettered`, `source` `fare-service`, `correlation_id` the ride ID, `occurred_at` in UTC,
and a payload with `original_event_id`, `original_event_type`, `ride_id` (omitted when unknown),
`error`, `service` (`fare-service`) and `failed_at` (RFC 3339 with fractional seconds). An entry that
is not a decodable envelope is dead-lettered under its stream message ID with type `decode_failed`.
The contract is pinned by `DeadLetterPublisherTest` against the tag names in `events.go`.

What is true after this:

- fare-service reclaims its own pending entries and dead-letters what it gives up on. The Go
  consumers still read only `>` and do not claim pending entries.
- Nothing consumes `events.dead_letter`. The stream is the record; replay is manual.
- A dead letter can appear twice for one entry; see above.
- `reclaim-min-idle` (5s) is longer than the transaction timeout (2s) that bounds one handling
  attempt, so a second replica of this service can only claim an entry whose owner has stopped
  working on it. Within one instance the reclaim pass and the read loop are the same thread and
  cannot overlap.

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
there is no accounts table. `hold_reversal` and `settlement` exist in the `JournalKind` enum so
the vocabulary is fixed, but nothing writes them. Both tables are append-only: a correction is a
new reversing entry, never an update or delete.

The balance rule (postings of one entry sum to zero, at least one posting, no zero posting) is
enforced in the `JournalEntry` constructor and nowhere else. There is no database trigger on
purpose: with a single writer, making the illegal state unrepresentable in the application is
enough, and a trigger would duplicate the rule in a second language with its own tests. A
trigger is defense in depth to add when a second writer appears. Reads go through the same
constructor (the query is a left join, so an entry that lost its postings is not hidden), so a
row set that no longer balances or has no postings is refused rather than served.

### Ledger endpoint

```
GET /v1/rides/{ride_id}/ledger
200 {"ride_id":"...","entries":[{"id":1,"kind":"quote_hold","source_event_id":"...",
     "created_at":"2026-09-07T21:30:00.123456Z",
     "postings":[{"account":"rider_receivable","amount":"5.85"},
                 {"account":"fare_hold","amount":"-5.85"}]}]}
404 {"error":"ledger not found"}
```

Amounts are strings with two decimals so no client turns them into floating point by accident.
Entries are in insertion order.

### Configuration

| Variable | Default | Used for |
| --- | --- | --- |
| `FARE_SERVICE_ADDR` | `:8087` | HTTP listen address, Go `host:port` form |
| `POSTGRES_DSN` | `postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable` | Translated to a JDBC URL plus credentials |
| `REDIS_ADDR` | `localhost:6379` | Redis host and port |
| `CONSUMER_GROUP` | `fare-service` | Consumer group name |
| `CONSUMER_NAME` | `fare-service-1` | Consumer name within the group |
| `SHUTDOWN_TIMEOUT_SECONDS` | `10` | Graceful shutdown budget, also the consumer drain budget |

Pending-entry recovery is tuned in `application.yml` under `metroride.consumer`, next to the
batch size and block timeout:

| Key | Default | Meaning |
| --- | --- | --- |
| `reclaim-interval` | `5s` | How often the consumer thread runs one `XAUTOCLAIM` before its next read |
| `reclaim-min-idle` | `5s` | How long an entry must have gone without a delivery before that `XAUTOCLAIM` takes it; longer than `metroride.postgres.timeout-seconds` |
| `retry-budget` | `120s` | How old an entry may be, by its stream ID, and still be retried after a retryable failure; four times the outbox relay's 30s maximum backoff |

The rate card lives in `application.yml` under `metroride.fare`, not in the environment:

| Key | Default | Meaning |
| --- | --- | --- |
| `base-fare` | `2.50` | Charged on every ride |
| `per-km` | `1.20` | Per kilometre of `distance_km` |
| `per-minute` | `0.30` | Per minute of `eta_seconds` |
| `driver-share` | `0.80` | Driver's fraction of a settled fare; bound and validated now, read once settlement exists |

### Metrics

| Metric | Labels | Meaning |
| --- | --- | --- |
| `metroride_fare_events_processed_total` | `service`, `stream`, `outcome=recorded\|duplicate` | Envelopes recorded or skipped as duplicates |
| `metroride_fare_quotes_total` | `service`, `kind=quote_hold` | Journal entries written, counted after the commit |
| `metroride_fare_quote_failures_total` | `service`, `reason=payload\|calculation` | `ride_assigned` envelopes whose payload did not decode or whose figures the calculator rejected; the transaction rolled back and the entry is dead-lettered as poison |
| `metroride_fare_events_reclaimed_total` | `service`, `stream` | Pending entries delivered again by the reclaim pass |
| `metroride_fare_dead_letters_total` | `service`, `stream`, `reason=poison\|retry_budget_exhausted` | Entries written to `events.dead_letter` and acknowledged; counted after Redis confirmed the `XADD` |
| `metroride_fare_dead_letter_publish_failures_total` | `service`, `stream` | Dead-letter `XADD`s Redis did not confirm; the entry stayed pending |
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
on exact `.5` boundaries, negative and non-finite inputs rejected), `Money`, and the
`JournalEntry` invariants (unbalanced, one-sided, empty and null posting lists are rejected; the
list is copied and immutable).

The integration tests share one `postgres:16-alpine` and one `redis:7-alpine` container
(`IntegrationTestSupport`). `RideAssignmentConsumerIT` covers the consumer path: one row per
envelope, zero pending entries, a second delivery acknowledged without a second row, a
lock-waiting write cancelled by the transaction timeout. `QuoteLedgerIT` covers the ledger: a
`ride_assigned` produces one event row, one `quote_hold` entry and two postings summing to zero;
a redelivery adds nothing; two threads recording the same event concurrently produce one entry;
a journal insert blocked until the timeout rolls back the event row with it and the reclaimed
delivery writes both once the lock is gone; the ledger endpoint returns the Go-style JSON.

Pending-entry recovery has its own tests. `FailureClassTest`, `StreamEntryAgeTest` and
`DeadLetterPublisherTest` are unit tests: one case per branch of the class mapping, the age from
a stream ID (with and without the `-<seq>` suffix, from the future, and IDs with no timestamp),
and the dead-letter JSON checked name by name against the tags in `events.go`.
`PendingEntryRecoveryIT` sends a `ride_assigned` with a negative distance and an entry whose
`event` field is not JSON, and asserts the dead letter's content, the acknowledgement, the empty
pending list and the untouched tables. The lock-wait tests in `RideAssignmentConsumerIT` and
`QuoteLedgerIT` assert that the entry is reclaimed and recorded within
`reclaim-interval + reclaim-min-idle` of the lock being released; nothing acknowledges by hand any
more. `RetryBudgetIT` holds the lock for good with the budget shrunk to 3s (its own Spring context
on the same containers, reading its own stream so it does not compete with the other tests'
consumer) and asserts the entry is dead-lettered with `reason=retry_budget_exhausted` and
acknowledged.

Not automated: the dead-letter `XADD` failing while the rest of the consumer keeps working. With
one connection to one Redis, the only way to make that `XADD` fail is to take Redis away, and then
the read loop stops too, so there is no state in which the publish-failure branch runs on its own.
The branch is four lines (count, log, return without acknowledging) and is read, not tested.

## Run in Compose

The service sits behind the optional `fare` Compose profile, so it is not part of the default stack:

```bash
docker compose --profile fare build fare-service
docker compose --profile fare up -d
curl -s localhost:8087/healthz   # {"status":"ok"}
curl -s localhost:8087/readyz    # {"status":"ready"}
curl -s localhost:8087/metrics | grep metroride_fare
```

Drive a ride through the stack and read its ledger once dispatch has assigned it:

```bash
RIDE=$(curl -s -X POST localhost:8080/v1/rides -H 'Content-Type: application/json' \
  -d '{"rider_id":"rider-42","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["ride_id"])')
sleep 3
curl -s localhost:8087/v1/rides/$RIDE/ledger
docker compose logs fare-service | grep '"event recorded"'
docker compose exec postgres psql -U metroride -d metroride \
  -c 'select j.kind, p.account, p.amount from fare.journal_entries j join fare.postings p on p.journal_entry_id = j.id'
```

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
docker compose logs fare-service | grep '"entry left pending"'
docker compose exec -T redis redis-cli XPENDING events.ride.assignments fare-service   # 1
docker compose start postgres
sleep 15
docker compose logs fare-service | grep '"pending entry reclaimed"'
curl -s localhost:8087/v1/rides/<ride uuid>/ledger                                     # the quote_hold
```

## Not in this service yet

- No settlement. The `quote_hold` is never reversed or settled: `ride_completed` exists in
  `events.go` but nobody publishes it, so `hold_reversal` and `settlement` are enum values only,
  and `driver-share` is bound but unused.
- No outbox and no publication to `events.ride.fares`.
- No database trigger for the ledger balance rule; see "Fare and ledger" for when to add one.
- No consumer of `events.dead_letter` and no replay tool: a dead-lettered entry is inspected
  and replayed by hand (the original entry is still in its stream, acknowledged but not deleted).
- No deduplication of dead letters; `original_event_id` is the key for whoever adds it.
- No Helm chart entry. Compose is the only runtime for this service so far.
