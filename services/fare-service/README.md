# fare-service

A Java service in the MetroRide monorepo. It consumes `events.ride.assignments` from Redis
Streams through the consumer group `fare-service`, records every envelope it sees once in its
own PostgreSQL schema, and for each first-seen `ride_assigned` quotes the fare from the
assignment's distance and ETA and holds that quote in a double-entry ledger. The hold is a
pre-authorisation, not a charge: nothing settles it yet, because no service publishes
`ride_completed`.

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
| Consumer loop | `consumer/RideAssignmentConsumer` | `XGROUP CREATE ... 0 MKSTREAM` on start, `XREADGROUP ... >` on a dedicated thread and connection, `XACK` after commit |
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
event row. A decode failure, a payload the calculator rejects, or a failed or timed-out
transaction is logged and counted, and the entry is left un-acknowledged in the group's pending
list. Nothing claims pending entries yet; see the `TODO(pending-entry recovery)` note in
`RideAssignmentConsumer`.

Two deliveries of the same envelope that arrive at the same moment are serialised by the primary
key of `fare.processed_events`: the second insert waits for the first transaction to commit, then
lands on the conflict clause and returns without touching the ledger. The unique constraint on
`journal_entries.source_event_id` is a backstop for a writer that bypasses the recorder, not the
mechanism the service relies on.

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
constructor, so a row set that no longer balances is refused rather than served.

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
| `metroride_fare_quote_failures_total` | `service`, `reason=payload\|calculation` | `ride_assigned` envelopes whose payload did not decode or whose figures the calculator rejected; the transaction rolled back and the entry stays pending |
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
a journal insert blocked until the timeout rolls back the event row with it; a rejected payload
records nothing and counts a quote failure; the ledger endpoint returns the Go-style JSON.

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

## Not in this service yet

- No settlement. The `quote_hold` is never reversed or settled: `ride_completed` exists in
  `events.go` but nobody publishes it, so `hold_reversal` and `settlement` are enum values only,
  and `driver-share` is bound but unused.
- No outbox and no publication to `events.ride.fares`.
- No database trigger for the ledger balance rule; see "Fare and ledger" for when to add one.
- No claiming of abandoned pending entries (`XAUTOCLAIM`), no retry cap, no dead-letter
  publication. The consumer reads only new entries, like the Go consumers do today, so a
  `ride_assigned` whose payload cannot be quoted stays pending until that exists.
- No Helm chart entry. Compose is the only runtime for this service so far.
