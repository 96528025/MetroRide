# MetroRide Architecture

MetroRide is a small-scale distributed ride dispatch project focused on backend systems design. It models a real-time workflow where rider requests, driver locations, routing decisions, traffic updates, and notifications are owned by separate services and coordinated through asynchronous events.

The default Docker Compose profile runs six core application service roles and 10 total Compose components after PostgreSQL, Redis, Prometheus, and Grafana are included. The optional `kafka` profile adds a seventh role, `analytics-service`, plus a second driver-service runtime instance, Kafka, and the one-shot Kafka init job, for 14 profile-expanded Compose components. The optional `fare` profile adds one more role, the Java `fare-service`, as a single Compose component. Runtime instances and infrastructure containers are not counted as new application service roles.

## Design Goals

- Isolate service responsibilities so each component has a clear operational boundary.
- Use event-driven communication for workflow coordination and backpressure tolerance.
- Keep PostgreSQL as the durable source of truth for ride and assignment state.
- Expose health, readiness, metrics, and structured logs from every service.
- Provide a clear path from Docker Compose to Kubernetes and Helm deployment.

## Design Rationale

See [system design](system-design.md) for the tradeoffs behind service boundaries, asynchronous coordination, and the choice of PostgreSQL and Redis Streams.

## Service Boundaries

| Service role | Startup | Primary Ownership | State |
| --- | --- | --- | --- |
| `rider-service` | Default | Ride request API and rider-facing ride status | PostgreSQL ride rows |
| `driver-service` | Default | Simulated driver availability and coordinate updates | In-memory simulation, Redis Stream output |
| `dispatch-service` | Default | Assignment workflow and ride state transition | PostgreSQL assignment rows, Redis Stream offsets |
| `routing-service` | Default | Driver proximity and ETA calculation | PostgreSQL driver positions and reservations; configured road routes |
| `traffic-service` | Default | Regional congestion simulation | In-memory traffic model, Redis Stream output |
| `notification-service` | Default | Simulated rider/driver notification handling | Consumer group offsets |
| `fare-service` | Optional `fare` profile | Consumes assignment, completion, and cancellation streams, records each envelope ID once, quotes the fare and holds it in a double-entry ledger, on completion reverses the hold and settles the quoted amount, and publishes `fare_settled` to `events.ride.fares` (Java) | PostgreSQL `fare.processed_events`, `fare.ride_state`, `fare.quote_context`, `fare.journal_entries`, `fare.postings`, `fare.event_outbox`, consumer group offsets |
| `analytics-service` | Optional `kafka` profile | Driver-location telemetry analytics | In-memory view hydrated from a Kafka consumer group |

## Event-Driven Architecture

Redis Streams provide the first event transport. Services publish typed event envelopes to named streams and consumers process those streams through consumer groups.

Streams (constants in `shared/pkg/events/events.go`):

- `events.ride.requests`
- `events.driver.locations`
- `events.ride.assignments`
- `events.ride.notifications`
- `events.ride.completions`
- `events.ride.cancellations`
- `events.ride.fares`
- `events.traffic.updates`
- `events.dead_letter`

Event types that are emitted today:

- `ride_requested` (rider-service, via the outbox)
- `driver_location_updated` (driver-service, direct `XADD`)
- `ride_assigned` (dispatch-service, via the outbox, to both the assignments and notifications streams)
- `ride_completed` (rider-service, via the outbox, to `events.ride.completions`, when `POST /v1/rides/{ride_id}/complete` moves an `assigned` ride to `completed`)
- `ride_cancelled` (rider-service, via its outbox, with the guarded cancellation and driver release)
- `fare_settled` (fare-service, via its own outbox in the `fare` schema, to `events.ride.fares`, once a completion has been settled; nothing consumes it yet)
- `traffic_updated` (traffic-service, direct `XADD`)
- `dead_lettered` (Go consumers and fare-service, direct `XADD` before source acknowledgment; malformed or retry-exhausted messages and Java quarantine failures go here. Failed publication leaves the original pending. See [reliability](reliability.md) for limits and [fare-service](../services/fare-service/README.md) for failure classes.)

`notification_created` is defined as a constant but nothing publishes it yet.

The shared event envelope includes event ID, type, source, correlation ID, timestamp, and payload. This keeps service contracts stable and gives the project a migration path to Kafka without changing domain payloads.

## Runtime Workflow

1. Rider-service commits the requested ride and its outbox event, then returns `202`.
2. Its relay publishes to Redis; dispatch consumes the request and asks routing for a candidate and passenger-route estimate.
3. Routing reads shared driver positions and calls the configured provider before dispatch opens the assignment transaction.
4. Dispatch rechecks the candidate and commits the reservation, ride assignment, and both outgoing events in PostgreSQL.
5. The dispatch relay publishes to the assignment and notification streams. Notification-service logs delivery; optional fare-service creates the quote hold.
6. Rider-service handles completion or cancellation by updating the ride, releasing its reservation, and writing an event in one transaction.
7. Fare-service processes that event in its own transaction. Completion also creates a `fare_settled` outbox row; cancellation reverses an existing hold. The fare relay publishes settlement events asynchronously.

Each database transaction ends at its owning service. The event transport permits duplicate and out-of-order delivery between those transactions. The [reliability guide](reliability.md#state-guards) explains the state guards; the [fare guide](../services/fare-service/README.md#cancellation) covers ledger ordering. Field-level contracts are in the [event reference](../shared/events/README.md), and provider behavior is in [road routing](routing.md).

## Fault Tolerance Concepts

MetroRide includes foundational production hooks:

- Go and Java consumers reclaim eligible pending deliveries with `XAUTOCLAIM`; processing and dead-letter failures keep the original pending until a durable outcome is confirmed (see [reliability](reliability.md)).
- PostgreSQL is the authoritative store for ride status and assignment state.
- Services expose `/healthz` and `/readyz` for orchestration and load balancer integration.
- Structured logs include service names and workflow identifiers for cross-service debugging.
- Docker Compose health checks gate Redis and PostgreSQL readiness before dependent services start.

Dispatch uses bounded retries, an idempotent PostgreSQL state transition, a transactional outbox, and `events.dead_letter` for retry-exhausted failures. Automated outage tests validate both routing dead-letter behavior and Redis recovery without event loss. Next resilience steps include dead-letter replay tooling, circuit breakers around routing calls, and stream lag alerting.

## Scalability Considerations

Redis consumer groups distribute new messages and reclaim eligible pending deliveries across workers. Routing replicas use shared PostgreSQL positions and reservations. These correctness mechanisms still require sustained load and failure testing before any capacity or production-scale claim. PostgreSQL can be indexed and eventually partitioned by region or creation time as ride volume grows.

The architecture is intentionally region-aware in concept: future work can shard drivers and riders by city or geohash, then replicate cross-region events for failover and analytics.

## Observability Strategy

Operational visibility is treated as part of the system design:

- Prometheus scrapes `/metrics` from services.
- Grafana provisions a dashboard for request rate, dispatch latency, routing duration, assignment failures, and active drivers.
- JSON logs support aggregation in systems such as Loki, Datadog, or Cloud Logging.
- Health and readiness endpoints give deployment platforms simple lifecycle signals.

See `docs/observability.md` for the metric and dashboard strategy.

## Production Deployment Goals

The repository includes Docker Compose for local orchestration, raw Kubernetes manifests for cloud-native deployment structure, and a Helm chart for parameterized releases.

The Helm chart is not scaffolding: CI installs it on an ephemeral KinD cluster on every run, with commit-SHA-pinned images, tuned health and readiness probes, resource requests and limits, and chart-owned test-only PostgreSQL and Redis, then drives a real ride through the deployed system before deleting the cluster. See [cicd.md](cicd.md).

What that does *not* demonstrate is a hosted environment. No cloud account or persistent infrastructure exists, and environment-specific work such as secrets management, ingress, persistent volumes and autoscaling is deliberately left for future implementation. The raw manifests in `infrastructure/k8s` remain scaffolding.
