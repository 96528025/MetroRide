# MetroRide Architecture

MetroRide is a backend-focused distributed ride dispatch simulation. It is intentionally service-oriented and event-driven so the repository demonstrates production backend engineering patterns rather than a simple CRUD app.

## Runtime Flow

1. `rider-service` accepts `POST /v1/rides`, persists the ride in PostgreSQL, and emits `ride_requested` to Redis Streams.
2. `dispatch-service` consumes `events.ride.requests` with a consumer group.
3. `dispatch-service` calls `routing-service` for nearest-driver selection.
4. `routing-service` maintains available driver state from `driver_location_updated` events.
5. `dispatch-service` updates PostgreSQL, emits `ride_assigned`, and emits notification events.
6. `notification-service` consumes assignment notifications and logs simulated rider/driver delivery.
7. Prometheus scrapes service metrics, and Grafana provisions a dashboard.

## Eventing

Redis Streams provide durable, asynchronous communication for the MVP. Event envelopes are defined in `shared/pkg/events` and intentionally separate event contracts from the transport. Kafka support can be added by implementing an alternate publisher/consumer adapter that preserves the same envelope.

## Fault Tolerance Hooks

- Redis Stream consumer groups support acknowledgement and replay.
- Services expose `/healthz`, `/readyz`, and `/metrics`.
- Docker Compose health checks gate startup dependencies.
- PostgreSQL remains the system of record for ride state.
- Event correlation IDs use ride IDs for traceability across logs.

## Future Work

- Replace REST dispatch-to-routing call with gRPC and protobuf contracts.
- Add Kafka transport behind the event publisher interface.
- Add OpenTelemetry tracing and trace ID propagation.
- Add autoscaling manifests based on queue lag and request latency.
- Replace in-memory routing state with a partitioned driver-location cache.
- Introduce demand forecasting and ETA prediction services.
