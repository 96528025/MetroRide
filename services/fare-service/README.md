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
| Failure classes | `consumer/FailureClass` | Pure mapping from the exception `handle()` saw to `RETRYABLE`, `POISON` or `FATAL`; the only place that decision is made |
| Failure handling | `consumer/FailureHandler`, `consumer/ConsumerHalt` | Leaves the entry pending, dead-letters it, or halts the consumer; owns the one `XACK` in the service; the halt fails `/readyz` |
| Entry age | `consumer/StreamEntryAge` | Age of an entry from the millisecond timestamp in its stream ID; logged, never decided on |
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

Every failure of step 1 or 2 is mapped by `FailureClass.of` to one of three classes:

| Class | Exceptions | What happens to the entry |
| --- | --- | --- |
| `POISON` | `EnvelopeDecodeException` (the entry is not an envelope), `FareQuoteException` (the payload cannot be quoted) | Dead-lettered immediately, then acknowledged |
| `FATAL` | `InvalidDataAccessResourceUsageException` (bad SQL, wrong column type), `InvalidDataAccessApiUsageException` (a repository was misused, or a constructor threw inside one), `DataIntegrityViolationException` (a constraint the writer cannot reach unless the schema or the data is already wrong) | Left pending; the consumer halts and `/readyz` fails with the reason until the deployment is fixed and the service restarted |
| `RETRYABLE` | Every other `DataAccessException` (cancelled lock wait, lost connection, deadlock), every `TransactionException`, and anything unforeseen | Left pending and delivered again by the reclaim pass; dead-lettered when its `max-deliveries`-th delivery fails |

Spring's own transient/non-transient split is not used because it files a refused connection under
non-transient. An unforeseen exception is retried rather than dead-lettered on sight: it is most
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
down before that. Under load the window is longer, never shorter. The guarantee is what the next
failure mode needs: the assignment and the completion of one ride will arrive on two streams from
two outbox relays that poll every 250ms and back off up to 30s after a failed publish
(`shared/pkg/outbox`), so a completion can precede its assignment by up to that long under normal
operation, and "no hold for this ride yet" is then a retryable failure that clears itself when the
assignment lands. 120s is four times the relay's maximum backoff. The age of the entry, now minus
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
stay in the pending list. `/readyz` answers 503 with `{"consumer": "<reason>"}` and
`metroride_fare_consumer_halted` reads 1. Nothing clears a halt at runtime: fix the deployment and
restart, and the first reclaim pass of the new process picks the entries up.

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
| `max-deliveries` | `25` | A retryable failure on this delivery dead-letters the entry; with `reclaim-min-idle` 5s this guarantees at least 120s of retrying since the first delivery, four times the outbox relay's 30s maximum backoff |

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
| `metroride_fare_dead_letters_total` | `service`, `stream`, `reason=poison\|retry_budget_exhausted` | Entries written to `events.dead_letter`; counted after Redis confirmed the `XADD`, before the `XACK` |
| `metroride_fare_dead_letter_publish_failures_total` | `service`, `stream` | Dead-letter `XADD`s Redis did not confirm; the entry stayed pending |
| `metroride_fare_consumer_halted` | `service` | Gauge, 1 once the consumer has stopped on a fatal failure |
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
a `ride_assigned` with a negative distance and an entry whose `event` field is not JSON, and
asserts the dead letter's content, the acknowledgement, the empty pending list and the untouched
tables. The lock-wait tests in `RideAssignmentConsumerIT` and `QuoteLedgerIT` assert that the
entry is reclaimed and recorded within `reclaim-interval + reclaim-min-idle` of the lock being
released; nothing acknowledges by hand any more. Three classes run a consumer of their own (a
`@TestPropertySource` context on the same containers, each reading its own stream so it never
competes with the shared context's consumer): `DeliveryCapIT` holds the lock for good with
`max-deliveries` 3 and asserts the entry is dead-lettered on its third delivery with
`reason=retry_budget_exhausted` and acknowledged; `OldEntryRecoveryIT` publishes an entry with a
stream ID from 2001, fails it once, and asserts it is retried and recorded rather than
dead-lettered for its age; `ReclaimCursorIT` keeps two entries failing with `batch-size` 2 and
asserts the third, behind them, is still reclaimed and recorded.

Not automated: a fatal failure end to end. Producing one against the real schema means breaking
the schema for every other test in the JVM; the halt is covered by `FailureHandlerTest` and its
readiness effect by `HealthControllerTest`.

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
- No pause while PostgreSQL is unreachable: deliveries keep accumulating during an outage, so
  one longer than the retry window ends with entries in the dead-letter stream.
- No Helm chart entry. Compose is the only runtime for this service so far.
