# fare-service

A Java service in the MetroRide monorepo. Today it does exactly one thing: it consumes
`events.ride.assignments` from Redis Streams through the consumer group `fare-service`
and records every envelope it sees once in its own PostgreSQL schema. That proves the
whole path works end to end (Maven build, container image, Compose wiring, Flyway
migration, consumer-group reads, idempotent writes, CI) before any business logic is
added.

## What is here

| Piece | Where | Notes |
| --- | --- | --- |
| Environment mapping | `config/MetroRideEnvironmentPostProcessor` | Reads `FARE_SERVICE_ADDR`, `POSTGRES_DSN`, `REDIS_ADDR` (same names and defaults as `shared/pkg/config/config.go`) and derives the Spring properties |
| Consumer settings | `config/ConsumerProperties`, `application.yml` | Group and consumer name come from `CONSUMER_GROUP` and `CONSUMER_NAME` through the same post-processor; batch size and block timeout live in `application.yml` |
| Envelope contract | `events/Envelope`, `events/RideAssigned`, `events/EnvelopeCodec` | Field-for-field match with `shared/pkg/events/events.go`; the stream entry field is `event`, as written by `events.Publish` |
| Idempotency record | `processing/ProcessedEvent`, `ProcessedEventRepository`, `ProcessedEventRecorder` | `insert ... on conflict (event_id) do nothing` inside one transaction |
| Consumer loop | `consumer/RideAssignmentConsumer` | `XGROUP CREATE ... 0 MKSTREAM` on start, `XREADGROUP ... >` on a dedicated thread and connection, `XACK` after commit |
| Schema | `db/migration/V1__processed_events.sql` | Flyway owns the `fare` schema; Hibernate only validates the mapping |
| Endpoints | `web/HealthController`, `web/MetricsController` | `/healthz`, `/readyz`, `/metrics` with the same paths and JSON as `shared/pkg/httpx/httpx.go` |

### Processing rule

For each stream entry:

1. Decode the `event` field into an `Envelope`.
2. In one transaction, insert `(event_id, stream, event_type, processed_at)` into
   `fare.processed_events` with `ON CONFLICT DO NOTHING`.
3. After the transaction commits, `XACK` the entry.

A redelivered envelope hits the conflict clause, is counted as a duplicate, and is
acknowledged. A decode failure or a failed transaction is logged and counted, and the
entry is left un-acknowledged in the group's pending list. Nothing claims pending
entries yet; see the `TODO(pending-entry recovery)` note in `RideAssignmentConsumer`.

### Configuration

| Variable | Default | Used for |
| --- | --- | --- |
| `FARE_SERVICE_ADDR` | `:8087` | HTTP listen address, Go `host:port` form |
| `POSTGRES_DSN` | `postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable` | Translated to a JDBC URL plus credentials |
| `REDIS_ADDR` | `localhost:6379` | Redis host and port |
| `CONSUMER_GROUP` | `fare-service` | Consumer group name |
| `CONSUMER_NAME` | `fare-service-1` | Consumer name within the group |
| `SHUTDOWN_TIMEOUT_SECONDS` | `10` | Graceful shutdown budget, also the consumer drain budget |

### Metrics

| Metric | Labels | Meaning |
| --- | --- | --- |
| `metroride_fare_events_processed_total` | `service`, `stream`, `outcome=recorded\|duplicate` | Envelopes recorded or skipped as duplicates |
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

The integration test `RideAssignmentConsumerIT` starts `postgres:16-alpine` and
`redis:7-alpine`, publishes an envelope the way the Go outbox relay does, and asserts
one row, zero pending entries, and that the second delivery of the same envelope is
acknowledged without a second row.

## Run in Compose

The service sits behind the optional `fare` Compose profile, so it is not part of the default stack:

```bash
docker compose --profile fare build fare-service
docker compose --profile fare up -d
curl -s localhost:8087/healthz   # {"status":"ok"}
curl -s localhost:8087/readyz    # {"status":"ready"}
curl -s localhost:8087/metrics | grep metroride_fare
```

Drive a ride through the stack and watch it arrive:

```bash
curl -s -X POST localhost:8080/v1/rides -H 'Content-Type: application/json' \
  -d '{"rider_id":"rider-42","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}'
docker compose logs fare-service | grep '"event recorded"'
docker compose exec postgres psql -U metroride -d metroride -c 'select * from fare.processed_events'
```

Replaying the same entry proves idempotency:

```bash
# --raw prints the JSON unescaped; the value to copy is the line after "event".
docker compose exec -T redis redis-cli --raw XREVRANGE events.ride.assignments + - COUNT 1
docker compose exec -T redis redis-cli XADD events.ride.assignments '*' event '<that JSON line>'
docker compose logs fare-service | grep '"duplicate event skipped"'
```

## Not in this service yet

- No fare calculation, no ledger tables, no double-entry postings.
- No handling of `ride_completed` (the constant exists in `events.go`, nobody publishes it yet).
- No outbox and no publication to `events.ride.fares`.
- No claiming of abandoned pending entries (`XAUTOCLAIM`), no retry cap, no dead-letter
  publication. The consumer reads only new entries, like the Go consumers do today.
- No Helm chart entry. Compose is the only runtime for this service so far.
