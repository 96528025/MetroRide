# MetroRide Architecture

MetroRide is a portfolio-scale distributed ride dispatch project focused on backend systems design. It models a real-time workflow where rider requests, driver locations, routing decisions, traffic updates, and notifications are owned by separate services and coordinated through asynchronous events.

The default Docker Compose profile runs six core application service roles and 10 total Compose components after PostgreSQL, Redis, Prometheus, and Grafana are included. The optional `kafka` profile adds a seventh role, `analytics-service`, plus a second driver-service runtime instance, Kafka, and the one-shot Kafka init job, for 14 profile-expanded Compose components. Runtime instances and infrastructure containers are not counted as new application service roles.

## Design Goals

- Isolate service responsibilities so each component has a clear operational boundary.
- Use event-driven communication for workflow coordination and backpressure tolerance.
- Keep PostgreSQL as the durable source of truth for ride and assignment state.
- Expose health, readiness, metrics, and structured logs from every service.
- Provide a clear path from Docker Compose to Kubernetes and Helm deployment.

## Why Microservices?

The ride dispatch domain has naturally separate scaling and failure profiles:

- Ride intake is request-driven and latency sensitive.
- Driver location ingestion is high frequency and stream-oriented.
- Dispatch assignment is workflow-oriented and benefits from consumer groups.
- Routing is compute-heavy and can evolve independently.
- Notifications are side effects that should not block ride creation.

Separating these responsibilities makes the architecture easier to scale and reason about. It also prevents non-critical workflows, such as notification delivery, from directly impacting the rider request path.

## Service Boundaries

| Service role | Startup | Primary Ownership | State |
| --- | --- | --- | --- |
| `rider-service` | Default | Ride request API and rider-facing ride status | PostgreSQL ride rows |
| `driver-service` | Default | Simulated driver availability and coordinate updates | In-memory simulation, Redis Stream output |
| `dispatch-service` | Default | Assignment workflow and ride state transition | PostgreSQL assignment rows, Redis Stream offsets |
| `routing-service` | Default | Driver proximity and ETA calculation | In-memory driver cache hydrated from events |
| `traffic-service` | Default | Regional congestion simulation | In-memory traffic model, Redis Stream output |
| `notification-service` | Default | Simulated rider/driver notification handling | Consumer group offsets |
| `analytics-service` | Optional `kafka` profile | Driver-location telemetry analytics | In-memory view hydrated from a Kafka consumer group |

## Event-Driven Architecture

Redis Streams provide the first event transport. Services publish typed event envelopes to named streams and consumers process those streams through consumer groups.

Core streams:

- `events.ride.requests`
- `events.driver.locations`
- `events.ride.assignments`
- `events.ride.notifications`
- `events.traffic.updates`

Core events:

- `ride_requested`
- `driver_location_updated`
- `ride_assigned`
- `ride_completed`
- `traffic_updated`

The shared event envelope includes event ID, type, source, correlation ID, timestamp, and payload. This keeps service contracts stable and gives the project a migration path to Kafka without changing domain payloads.

## Runtime Workflow

1. `rider-service` receives `POST /v1/rides`.
2. The ride and a pending `ride_requested` outbox event are committed together in PostgreSQL.
3. `rider-service` returns `202`; its relay publishes the pending event to Redis Streams asynchronously.
4. `dispatch-service` consumes the request with a Redis consumer group.
5. `dispatch-service` calls `routing-service` for nearest-driver selection.
6. `routing-service` calculates distance and ETA from its driver-location view.
7. `dispatch-service` commits the assignment, status update, and pending assignment and notification outbox events together.
8. Its relay publishes both pending events to their Redis Streams asynchronously.
9. `notification-service` consumes notification events and logs simulated delivery.

## Why Redis Streams?

Redis Streams are a pragmatic transport for the MVP because they provide:

- Durable append-only stream semantics.
- Consumer groups for horizontal consumer scaling.
- Explicit acknowledgements for retry and replay behavior.
- Simple local operations through Docker Compose.
- A clean bridge toward Kafka-style event logs.

Kafka is the natural next transport when the system requires stronger partitioning semantics, longer retention, higher fanout, and broader ecosystem integration. MetroRide keeps event definitions transport-neutral to make that migration incremental.

## Fault Tolerance Concepts

MetroRide includes foundational production hooks:

- Consumer groups keep unacknowledged dispatch or notification work pending rather than dropping it; the current workers do not yet reclaim abandoned pending entries.
- PostgreSQL is the authoritative store for ride status and assignment state.
- Services expose `/healthz` and `/readyz` for orchestration and load balancer integration.
- Structured logs include service names and workflow identifiers for cross-service debugging.
- Docker Compose health checks gate Redis and PostgreSQL readiness before dependent services start.

Dispatch uses bounded retries, an idempotent PostgreSQL state transition, a transactional outbox, and `events.dead_letter` for retry-exhausted failures. Automated outage tests validate both routing dead-letter behavior and Redis recovery without event loss. Next resilience steps include dead-letter replay tooling, abandoned pending-message claiming, circuit breakers around routing calls, and stream lag alerting.

## Scalability Considerations

Redis consumer groups can divide new dispatch messages across workers, but the current implementation still needs pending-entry recovery and load testing before making a horizontal-scale claim. Routing cannot safely share its current process-local driver view across replicas without a shared store or explicit regional partitioning. PostgreSQL can be indexed and eventually partitioned by region or creation time as ride volume grows.

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
