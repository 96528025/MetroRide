# MetroRide Architecture Decisions

This document captures the major architecture decisions behind MetroRide using an ADR-style format.

## ADR 001: Go for Backend Services

### Context

MetroRide needs multiple lightweight backend services with clear concurrency, networking, and deployment characteristics.

### Decision

Use Go as the primary backend language for all services.

### Alternatives Considered

- Python for faster prototyping.
- Node.js for lightweight HTTP services.
- Java/Kotlin for enterprise service patterns.

### Tradeoffs

Go provides simple static binaries, strong standard-library networking, good concurrency primitives, and straightforward containerization. The tradeoff is more boilerplate than Python and less framework-level structure than Java ecosystems.

## ADR 002: Redis Streams for Asynchronous Eventing

### Context

Ride requests, driver locations, assignment events, traffic updates, and notifications need asynchronous coordination. The local system should remain easy to run.

### Decision

Use Redis Streams as the initial event transport.

### Alternatives Considered

- Direct REST calls between all services.
- Kafka.
- RabbitMQ or NATS.

### Tradeoffs

Redis Streams provide durable streams, consumer groups, acknowledgements, and simple local operations. Kafka would be better for large-scale partitioning, retention, and high-throughput fanout, but it adds operational complexity. Direct REST is simpler but creates tighter coupling and weaker replay behavior.

## ADR 003: PostgreSQL for Durable Ride State

### Context

Ride status and assignment state need durable, queryable storage independent of the event bus.

### Decision

Use PostgreSQL as the system of record for ride and assignment data.

### Alternatives Considered

- Redis as the primary state store.
- A document database.
- Event sourcing only.

### Tradeoffs

PostgreSQL provides transactional writes, indexes, relational constraints, and simple status queries. The tradeoff is that event publication and database mutation are not atomic without an outbox pattern, which is a future improvement.

## ADR 004: Docker Compose for Local Orchestration

### Context

The project needs to run locally with multiple services and infrastructure dependencies.

### Decision

Use Docker Compose as the primary local runtime.

### Alternatives Considered

- Running services directly on the host.
- Local Kubernetes only.
- Makefile-managed background processes.

### Tradeoffs

Docker Compose keeps local setup simple and reproducible. It is not a production orchestrator, so the repo also includes Kubernetes manifests and Helm scaffolding to show the cloud-native direction.

## ADR 005: Prometheus and Grafana for Observability

### Context

The project needs visible service health and workflow metrics.

### Decision

Expose Prometheus metrics from services and provision Grafana dashboards.

### Alternatives Considered

- Logs only.
- OpenTelemetry-first metrics.
- Vendor-specific monitoring only.

### Tradeoffs

Prometheus and Grafana are widely used, local-friendly, and fit service-level metrics well. They do not provide distributed tracing by themselves, so OpenTelemetry is a future improvement.

## ADR 006: Idempotent Dispatch Logic

### Context

Redis Streams can redeliver events, and dispatch workers may restart or process duplicate ride requests.

### Decision

Make ride assignment idempotent by checking PostgreSQL state before assignment and only updating rides where `status = 'requested'`.

### Alternatives Considered

- Assume each event is processed once.
- Use only Redis message IDs for deduplication.
- Store a separate idempotency table.

### Tradeoffs

The PostgreSQL state check is simple and directly tied to the authoritative ride state. It prevents duplicate assignment in the current workflow. A separate idempotency table could support broader deduplication semantics but adds complexity.

## ADR 007: Dead-Letter Stream for Failed Events

### Context

Some events may fail repeatedly due to malformed payloads, unavailable dependencies, or unexpected runtime errors.

### Decision

Publish failed dispatch events to `events.dead_letter` after bounded retries.

### Alternatives Considered

- Retry indefinitely.
- Drop failed events after logging.
- Keep failed messages pending forever.

### Tradeoffs

A dead-letter stream makes failures inspectable and prevents poison messages from blocking progress. It requires operational follow-up: replay tooling, dashboards, and alerting would be needed in a production system.
