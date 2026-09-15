# MetroRide System Design

MetroRide is a small-scale local distributed-systems project that demonstrates backend infrastructure concepts through a ride dispatch domain. It is not deployed at real production scale; it is designed to show how a distributed backend can be decomposed, instrumented, and hardened for reliability.

## Problem Statement

Ride dispatch is a real-time coordination problem. A rider creates a request, drivers continuously publish location updates, a dispatch system chooses an available driver, routing estimates distance and ETA, and notifications are emitted after assignment. The system must coordinate these steps without tightly coupling every service through synchronous calls.

MetroRide models this workflow with six default core Go application services, Redis Streams, PostgreSQL, Docker Compose, a Helm chart that CI installs on an ephemeral KinD cluster, raw Kubernetes manifests kept as scaffolding, Prometheus, and Grafana. An optional Kafka profile adds `analytics-service` as a seventh application role for driver-location telemetry; its extra driver producer is another instance of the existing driver-service role.

## System Goals

- Accept ride requests through a rider-facing API.
- Persist durable ride state in PostgreSQL.
- Dispatch ride requests asynchronously through an event stream.
- Maintain a current driver-location view for routing.
- Assign drivers idempotently so duplicate events do not create duplicate assignments.
- Expose operational health, readiness, metrics, and structured logs.
- Provide a clear path from local orchestration to cloud-native deployment patterns.

## Non-Goals

- Full consumer mobile application or frontend experience.
- A map frontend, geocoding, payment collection, identity, or commercial fare policies.
- Globally distributed production deployment.
- Exactly-once distributed transactions across PostgreSQL and Redis.
- Production-grade route optimization or ML ETA prediction.

## System Structure

The [architecture guide](architecture.md) records service ownership and the runtime sequence. This document explains why those boundaries were chosen and what they cost.

## Why Microservices?

The domain has separate scaling and failure characteristics:

- Rider request intake is latency-sensitive.
- Driver location ingestion is stream-oriented.
- Dispatch is workflow-oriented and benefits from consumer groups.
- Routing is compute-oriented and can evolve independently.
- Notification delivery is a side effect and should not block ride creation.

Microservices make these boundaries explicit. In a larger system, each service could have independent deployment, scaling, ownership, and failure budgets.

## Why Event-Driven Architecture?

The dispatch workflow should not require every downstream side effect to complete during the initial ride request. An event-driven design decouples ride intake from assignment processing and notification delivery. It also supports replay, backpressure handling, and independent consumers.

Synchronous REST is still used where it fits: `dispatch-service` calls `routing-service` because driver selection is required before assignment can be persisted.

## Why Redis Streams?

Redis Streams provide a pragmatic event log for a local distributed systems project:

- Durable append-only streams.
- Consumer groups for horizontal processing.
- Explicit acknowledgement semantics.
- Simple local operation through Docker Compose.
- A migration path toward Kafka while preserving event payloads.

Kafka would be a stronger choice for high-volume, multi-consumer, long-retention production workloads. Redis Streams keeps the MVP operationally small while still demonstrating event-driven design.

## Why PostgreSQL?

PostgreSQL is the system of record for ride and assignment state. Redis coordinates workflow events, but PostgreSQL owns durable queryable truth. This separation avoids treating the event bus as the primary ride database and makes status lookup straightforward.

## Why Prometheus and Grafana?

Prometheus and Grafana are common infrastructure choices for service metrics and dashboards. MetroRide uses them to expose request volume, assignment latency, routing duration, active drivers, dependency errors, and stream consume errors. This makes system behavior inspectable during local development and gives a realistic observability story.

## Routing and Lifecycle Tradeoffs

- **Compute routes before reserving.** External HTTP calls can be slow or unavailable. Keeping them outside the assignment transaction limits lock time, at the cost of rechecking the candidate and retrying if another ride takes that driver. See [routing](routing.md).
- **Store reservations in the database.** A shared unique constraint arbitrates ownership across instances, while database availability and lock contention become part of the dispatch path. The [state guards](reliability.md#state-guards) describe the enforced transitions.
- **Settle an upfront quote.** Persisting pricing inputs makes completion reproducible after configuration changes, but it cannot account for an unmeasured detour. The [fare guide](../services/fare-service/README.md#quote-context-and-legacy-holds) specifies that policy and its historical-data boundary.
- **Coordinate through events.** Services can recover independently, but an HTTP response does not mean every consumer has processed the change. A cancellation can reach the ledger before its assignment, so per-ride ordering must be enforced at the consumer; see [cancellation](../services/fare-service/README.md#cancellation).

Operational failure behavior and observability settings are maintained in [reliability](reliability.md) and [observability](observability.md).

## Scalability Considerations

- Redis consumer groups can divide new dispatch messages across multiple workers; eligible pending messages are reclaimed, while production capacity still needs load testing.
- `routing-service` shares driver positions and reservations through PostgreSQL; regional partitioning remains future work.
- Driver location processing can be partitioned by region or geohash.
- PostgreSQL can be indexed and eventually partitioned by time or region.
- Redis Streams can be replaced by Kafka for stronger partitioning, retention, and high-throughput fanout.

## Bottlenecks and Tradeoffs

- Redis Streams are simple and local-friendly, but Kafka would be more appropriate for very high event volume.
- The dispatch-to-routing call is synchronous, which keeps assignment simple but adds routing availability to the critical path.
- Outbox delivery is at-least-once, so consumers must remain idempotent when a relay repeats a stable event ID.
- Routing reads shared PostgreSQL state; database latency and public route-provider limits constrain throughput.
- The system prioritizes clear architecture and operational hooks over full domain completeness.

## Future Improvements

- Migrate appropriate core event streams to Kafka if production throughput and retention requirements justify it; the current Kafka profile is an optional telemetry extension only.
- Move service-to-service calls to gRPC with deadlines and typed protobuf contracts.
- Add OpenTelemetry distributed tracing.
- Add durable idempotency keys to every side-effecting consumer.
- Add dead-letter replay tooling.
- Add Kubernetes autoscaling based on stream lag and latency.
- Partition drivers and rides by region.
- Introduce ML-assisted ETA prediction and demand forecasting.
