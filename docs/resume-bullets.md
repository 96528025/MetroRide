# MetroRide Resume Bullets

Use these as source material and adjust numbers only after measuring the deployed system.

## Concise Bullets

- Built MetroRide, a Go-based distributed ride dispatch platform with six independently deployable microservices coordinated through Redis Streams and backed by PostgreSQL.
- Designed an event-driven dispatch workflow where ride requests are durably persisted, published as stream events, consumed by dispatch workers, routed to available drivers, and emitted as assignment notifications.
- Implemented production-oriented service boundaries across rider intake, driver location simulation, dispatch orchestration, routing, traffic simulation, and notification processing.
- Added Prometheus metrics, Grafana dashboards, health checks, readiness endpoints, and structured JSON logging for service-level observability.
- Containerized the platform with Docker Compose and added Kubernetes manifests plus Helm scaffolding for cloud-native deployment.

## Backend Engineering Version

- Developed a cloud-native dispatch backend in Go using Redis Streams consumer groups for asynchronous ride assignment and PostgreSQL for durable ride state.
- Built a routing service that maintains real-time driver availability from location events and computes nearest-driver ETA for dispatch coordination.
- Designed shared event contracts with correlation IDs and typed payloads to support replayable workflows and future Kafka migration.
- Exposed operational metrics including ride request volume, dispatch latency, assignment failures, routing duration, and active driver count through Prometheus.
- Structured the repository as a production-style monorepo with service isolation, infrastructure-as-code, observability configs, and deployment documentation.

## Google-Style Project Description

- Architected and implemented MetroRide, a distributed ride dispatch system that models production backend patterns including asynchronous event processing, service isolation, durable state management, and cloud-native deployment.
- Coordinated ride assignment across Go microservices using Redis Streams, consumer groups, explicit acknowledgements, and shared event envelopes designed for future Kafka adoption.
- Established an observability baseline with Prometheus, Grafana, structured logs, health probes, and latency/failure metrics to support debugging and operational readiness.

## Interview Talking Points

- Why Redis Streams were chosen for the MVP and how the design can migrate to Kafka.
- How PostgreSQL and Redis responsibilities are separated between durable state and workflow coordination.
- How dispatch consumers can scale horizontally through consumer groups.
- What failure modes remain and how idempotency, dead-letter streams, retries, and tracing would address them.
- How Kubernetes and Helm artifacts evolve toward a production deployment with autoscaling, secrets, ingress, and service monitors.
