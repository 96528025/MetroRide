# MetroRide Architecture

MetroRide is a production-style distributed ride dispatch platform focused on backend systems design. The system models a real-time workflow where rider requests, driver locations, routing decisions, traffic updates, and notifications are owned by separate services and coordinated through asynchronous events.

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

| Service | Primary Ownership | State |
| --- | --- | --- |
| `rider-service` | Ride request API and rider-facing ride status | PostgreSQL ride rows |
| `driver-service` | Simulated driver availability and coordinate updates | In-memory simulation, Redis Stream output |
| `dispatch-service` | Assignment workflow and ride state transition | PostgreSQL assignment rows, Redis Stream offsets |
| `routing-service` | Driver proximity and ETA calculation | In-memory driver cache hydrated from events |
| `traffic-service` | Regional congestion simulation | In-memory traffic model, Redis Stream output |
| `notification-service` | Simulated rider/driver notification handling | Consumer group offsets |

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
2. The ride is inserted into PostgreSQL with `requested` status.
3. `rider-service` publishes `ride_requested` to Redis Streams.
4. `dispatch-service` consumes the request with a Redis consumer group.
5. `dispatch-service` calls `routing-service` for nearest-driver selection.
6. `routing-service` calculates distance and ETA from its driver-location view.
7. `dispatch-service` persists the assignment and updates ride status to `assigned`.
8. `dispatch-service` emits assignment and notification events.
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

- Consumer groups allow failed dispatch or notification work to remain pending rather than disappear.
- PostgreSQL is the authoritative store for ride status and assignment state.
- Services expose `/healthz` and `/readyz` for orchestration and load balancer integration.
- Structured logs include service names and workflow identifiers for cross-service debugging.
- Docker Compose health checks gate Redis and PostgreSQL readiness before dependent services start.

Next resilience steps would include idempotency keys, dead-letter streams, retry budgets, circuit breakers around routing calls, and stream lag alerting.

## Scalability Considerations

Dispatch workers can scale horizontally within the same Redis consumer group. Routing can scale independently behind a service endpoint. Driver location processing can be partitioned by geographic region. PostgreSQL can be indexed and eventually partitioned by region or creation time as ride volume grows.

The architecture is intentionally region-aware in concept: future work can shard drivers and riders by city or geohash, then replicate cross-region events for failover and analytics.

## Observability Strategy

Operational visibility is treated as part of the system design:

- Prometheus scrapes `/metrics` from services.
- Grafana provisions a dashboard for request rate, dispatch latency, routing duration, assignment failures, and active drivers.
- JSON logs support aggregation in systems such as Loki, Datadog, or Cloud Logging.
- Health and readiness endpoints give deployment platforms simple lifecycle signals.

See `docs/observability.md` for the metric and dashboard strategy.

## Production Deployment Goals

The repository includes Docker Compose for local orchestration, Kubernetes manifests for cloud-native deployment structure, and Helm scaffolding for parameterized releases. The current manifests provide the shape of a production deployment, while leaving room for environment-specific additions such as secrets, ingress, persistent volumes, service monitors, autoscaling, and resource limits.
