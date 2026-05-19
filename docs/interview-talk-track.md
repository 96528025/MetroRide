# MetroRide Interview Talk Track

Use this document to explain MetroRide as a backend infrastructure project in interviews. Keep the framing honest: MetroRide is a production-style local distributed systems project, not a real production service operating at large scale.

## 30-Second Project Pitch

MetroRide is a Go-based distributed ride dispatch platform I built to demonstrate backend infrastructure concepts. It uses six microservices, Redis Streams for event-driven coordination, PostgreSQL for durable ride state, and Prometheus/Grafana for observability. A rider request is persisted, published as an event, consumed by dispatch workers, routed to an available driver, assigned idempotently, and then emitted as a notification event. The project focuses on service boundaries, reliability, observability, and cloud-native deployment patterns rather than frontend features.

## 2-Minute Technical Explanation

MetroRide models the core backend workflow of ride dispatch. `rider-service` accepts ride requests over REST and writes the requested ride to PostgreSQL. It then publishes a `ride_requested` event to Redis Streams. `dispatch-service` consumes that stream through a consumer group, checks PostgreSQL to make sure the ride has not already been assigned, calls `routing-service` to find the nearest available driver, and persists the assignment. After that, it publishes assignment and notification events.

Driver state is fed by `driver-service`, which simulates location updates. `routing-service` consumes those updates and maintains an in-memory view of available drivers for ETA calculation. `notification-service` consumes assignment notifications. `traffic-service` simulates congestion updates for future route weighting.

The reliability story includes dependency-aware readiness checks, explicit timeouts, bounded retries, idempotent assignment logic, and a Redis dead-letter stream for failed dispatch events. Observability is handled with `/metrics`, Prometheus, Grafana dashboards, health checks, readiness checks, and structured JSON logs.

## 5-Minute System Design Walkthrough

Start with the problem: a ride request triggers multiple downstream actions, but the rider-facing API should not block on every side effect. MetroRide splits the domain into services with clear ownership.

The first boundary is ride intake. `rider-service` validates the request, creates the ride in PostgreSQL, and emits an event. PostgreSQL is the system of record because ride status needs durable and queryable state.

The second boundary is dispatch. `dispatch-service` consumes `ride_requested` asynchronously. This allows dispatch workers to scale independently from ride intake and lets the system tolerate temporary downstream failures. Redis Streams provide consumer groups and acknowledgement semantics, which are enough for the local production-style version.

The third boundary is routing. Dispatch calls routing synchronously because it needs a driver decision before it can assign the ride. Routing maintains driver availability from location events. In a scaled system, this could be partitioned by region or backed by a location store.

Reliability is handled through timeouts, retries, and idempotency. If routing is unavailable, dispatch retries. If it still fails, the event is written to `events.dead_letter`. If dispatch crashes, Redis consumer group state preserves unacknowledged work. If the same event is processed twice, dispatch checks PostgreSQL and only updates rides still in `requested` status.

Observability is built into every service. Each service exposes health, readiness, and Prometheus metrics. Grafana visualizes request rate, dispatch latency, routing duration, assignment failures, and active drivers. Structured logs include service names, event types, ride IDs, driver IDs, and errors.

For cloud deployment, Docker Compose runs the full local system, and Kubernetes/Helm artifacts show how the services could be deployed with service discovery, health probes, and future autoscaling.

## Common Interviewer Questions

### Why did you choose microservices?

The domain naturally separates into components with different scaling and failure profiles: ride intake, driver location updates, dispatch coordination, routing computation, traffic simulation, and notification delivery. Splitting them makes ownership and failure boundaries explicit. For a real small product, a modular monolith might be simpler, but this project intentionally demonstrates distributed backend design.

### Why Redis Streams instead of direct REST calls?

Direct REST would tightly couple ride intake to downstream dispatch and notification work. Redis Streams let ride intake persist the request and publish an event, while dispatch processes asynchronously. That provides backpressure tolerance, consumer groups, acknowledgement, and replay semantics. REST is still used where synchronous behavior is required, such as dispatch asking routing for a driver decision.

### How does the dispatch flow work?

`rider-service` writes a requested ride to PostgreSQL and publishes `ride_requested`. `dispatch-service` reads that event, checks if the ride is already assigned, calls `routing-service`, updates the ride to `assigned`, writes an assignment record, and emits `ride_assigned` plus a notification event.

### What happens if dispatch-service crashes?

Redis Streams consumer groups preserve unacknowledged messages. If dispatch crashes before acknowledgement, the message remains pending and can be recovered by a consumer. If the ride was already assigned before the crash, idempotency logic prevents a duplicate assignment when the message is seen again.

### How do you prevent duplicate driver assignment?

`dispatch-service` checks PostgreSQL before assigning. If the ride is not in `requested` status or already has a driver, it skips the event. The update query also includes `where status = 'requested'`, so concurrent workers cannot both assign the same ride.

### How do you observe the system?

Each service exposes `/healthz`, `/readyz`, and `/metrics`. Prometheus scrapes metrics such as ride requests, assignments, dispatch latency, routing duration, dependency errors, stream consume errors, and active drivers. Grafana dashboards visualize system behavior, and structured JSON logs carry service, event type, ride ID, driver ID, and error context.

### What would you improve if this had to support 1 million rides per day?

I would migrate eventing to Kafka, partition rides and drivers by region or geohash, add a transactional outbox, introduce OpenTelemetry tracing, autoscale dispatch workers based on stream lag, make routing state partition-aware, add caching/location indexing, and harden PostgreSQL with read replicas, partitioning, and connection pool tuning.

### What are the main bottlenecks?

The main bottlenecks are Redis Streams throughput, the synchronous dispatch-to-routing call, in-memory routing state, and PostgreSQL write throughput for ride and assignment state. At higher scale, partitioning and Kafka would be the most important changes.

### How would you migrate this from Docker Compose to Kubernetes/cloud?

I would build and push service images to a registry, configure Kubernetes Deployments and Services, move credentials into Secrets, use ConfigMaps for service configuration, add persistent storage for PostgreSQL/Redis or use managed services, configure ingress, add resource requests/limits, and add autoscaling based on CPU, latency, and stream lag. The repo already includes Kubernetes manifests and a Helm scaffold as the starting point.

### What tradeoffs did you make?

I chose Redis Streams instead of Kafka to keep the local system easy to run while still demonstrating event-driven design. I used REST for dispatch-to-routing because it is simple and visible, though gRPC would be better for typed internal APIs. I did not implement a transactional outbox, so PostgreSQL and Redis publication are not atomic. Routing uses in-memory state, which is good for simulation but would need partitioning or external state at scale.
