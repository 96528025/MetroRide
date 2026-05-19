# MetroRide System Design

MetroRide is a production-style local distributed systems project that demonstrates backend infrastructure concepts through a ride dispatch domain. It is not deployed at real production scale; it is designed to show how a distributed backend can be decomposed, instrumented, and hardened for reliability.

## Problem Statement

Ride dispatch is a real-time coordination problem. A rider creates a request, drivers continuously publish location updates, a dispatch system chooses an available driver, routing estimates distance and ETA, and notifications are emitted after assignment. The system must coordinate these steps without tightly coupling every service through synchronous calls.

MetroRide models this workflow with Go microservices, Redis Streams, PostgreSQL, Docker Compose, Kubernetes manifests, Helm scaffolding, Prometheus, and Grafana.

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
- Real maps, geocoding, payments, identity, or pricing.
- Globally distributed production deployment.
- Exactly-once distributed transactions across PostgreSQL and Redis.
- Production-grade route optimization or ML ETA prediction.

## High-Level Architecture

MetroRide uses a service-oriented architecture with asynchronous workflow coordination:

- REST is used at API boundaries and for dispatch-to-routing lookup.
- Redis Streams are used for durable event flow and consumer-group processing.
- PostgreSQL stores authoritative ride and assignment state.
- Prometheus and Grafana provide metrics and dashboards.
- Docker Compose runs the full local system, while Kubernetes and Helm artifacts show the deployment direction.

## Service Responsibilities

| Service | Responsibility |
| --- | --- |
| `rider-service` | Accepts ride requests, stores ride state, publishes `ride_requested`. |
| `driver-service` | Simulates driver location and availability updates. |
| `dispatch-service` | Consumes ride requests, coordinates routing, persists assignments, emits assignment and notification events. |
| `routing-service` | Maintains driver-location state and computes nearest-driver ETA. |
| `traffic-service` | Simulates congestion updates for future route weighting. |
| `notification-service` | Consumes assignment notifications and simulates delivery. |

## End-to-End Ride Request Flow

1. A client sends `POST /v1/rides` to `rider-service`.
2. `rider-service` inserts a ride row in PostgreSQL with status `requested`.
3. `rider-service` publishes a `ride_requested` event to `events.ride.requests`.
4. `dispatch-service` consumes the event through a Redis consumer group.
5. `dispatch-service` checks PostgreSQL to avoid duplicate assignment.
6. `dispatch-service` calls `routing-service` for nearest-driver selection.
7. `routing-service` uses its driver-location view to return driver ID, distance, and ETA.
8. `dispatch-service` updates the ride to `assigned` inside PostgreSQL.
9. `dispatch-service` emits `ride_assigned` and notification events.
10. `notification-service` consumes the notification event and logs simulated delivery.

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

## Reliability Design

MetroRide includes production-oriented reliability controls:

- `/healthz` confirms each process is alive.
- `/readyz` checks dependencies such as Redis, PostgreSQL, consumer groups, and routing readiness where applicable.
- Redis, PostgreSQL, and routing calls use explicit timeouts.
- Transient dispatch operations use bounded retries.
- Failed dispatch events are moved to `events.dead_letter`.

## Idempotency Design

Redis Streams can redeliver messages after failures or restarts. `dispatch-service` handles this by checking persisted ride state before assignment. If a ride is already assigned or no longer in `requested` status, the event is skipped. The update query also guards on `status = 'requested'`, so concurrent workers cannot assign the same ride twice.

## Dead-Letter Stream Design

If `dispatch-service` cannot process a `ride_requested` event after retries, it publishes a failure record to:

```text
events.dead_letter
```

The dead-letter event includes original event type, ride ID, error message, service name, and timestamp. This prevents a poison message from blocking the consumer group indefinitely and gives operators a place to inspect failed work.

## Observability Strategy

Every service exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

Key metrics include:

- `metroride_ride_requests_total`
- `metroride_rides_assigned_total`
- `metroride_dispatch_latency_seconds`
- `metroride_assignment_failures_total`
- `metroride_stream_consume_errors_total`
- `metroride_dependency_errors_total`
- `metroride_routing_computation_seconds`
- `metroride_active_drivers`

Structured JSON logs include service names, event types, ride IDs, driver IDs, and errors where relevant.

## Scalability Considerations

- `dispatch-service` can scale horizontally through Redis consumer groups.
- `routing-service` can scale behind a service endpoint.
- Driver location processing can be partitioned by region or geohash.
- PostgreSQL can be indexed and eventually partitioned by time or region.
- Redis Streams can be replaced by Kafka for stronger partitioning, retention, and high-throughput fanout.

## Bottlenecks and Tradeoffs

- Redis Streams are simple and local-friendly, but Kafka would be more appropriate for very high event volume.
- The dispatch-to-routing call is synchronous, which keeps assignment simple but adds routing availability to the critical path.
- Assignment persistence and event publication are not atomic across PostgreSQL and Redis; a transactional outbox would improve reliability.
- Routing state is in memory, which is acceptable for the simulation but would need partitioning or a shared location store at scale.
- The system prioritizes clear architecture and operational hooks over full domain completeness.

## Future Improvements

- Add Kafka as the event backbone.
- Move service-to-service calls to gRPC with deadlines and typed protobuf contracts.
- Add OpenTelemetry distributed tracing.
- Add a transactional outbox for reliable event publication.
- Add dead-letter replay tooling.
- Add Kubernetes autoscaling based on stream lag and latency.
- Partition drivers and rides by region.
- Introduce ML-assisted ETA prediction and demand forecasting.
