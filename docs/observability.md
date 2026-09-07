# MetroRide Observability

MetroRide treats observability as part of the system contract. Each service exposes operational endpoints, emits structured logs, and provides Prometheus-compatible metrics for workflow health and latency analysis.

## Monitoring Goals

- Detect ride intake, dispatch, and routing failures quickly.
- Track dispatch latency and routing computation time.
- Understand driver availability and assignment throughput.
- Provide service-level health signals for orchestration.
- Keep logs structured enough for centralized aggregation.

## Prometheus Metrics

Every service exposes metrics at:

```http
GET /metrics
```

Current project metrics (13; names are fixed by the Go registrations linked in the Owner column):

| Metric | Type | Owner | Purpose |
| --- | --- | --- | --- |
| `metroride_ride_requests_total` | Counter | [`rider-service`](../services/rider-service/cmd/main.go) | Total accepted ride requests |
| `metroride_rides_assigned_total` | Counter | [`dispatch-service`](../services/dispatch-service/cmd/main.go) | Total successful ride assignments |
| `metroride_dispatch_latency_seconds` | Histogram | `dispatch-service` | Assignment workflow latency |
| `metroride_assignment_failures_total` | Counter | `dispatch-service` | Failed dispatch attempts |
| `metroride_stream_consume_errors_total` | Counter | [shared](../shared/pkg/metrics/metrics.go) | Redis Stream consume failures, labelled by service and stream |
| `metroride_dependency_errors_total` | Counter | shared | Redis, PostgreSQL, routing and Kafka dependency failures, labelled by service and dependency |
| `metroride_outbox_events_published_total` | Counter | [outbox relay](../shared/pkg/outbox/outbox.go) in `rider-service` and `dispatch-service` | Outbox rows published to Redis, labelled by service and stream |
| `metroride_outbox_publish_failures_total` | Counter | outbox relay | Outbox publish attempts that failed and were rescheduled, labelled by service and stream |
| `metroride_routing_computation_seconds` | Histogram | [`routing-service`](../services/routing-service/cmd/main.go) | Nearest-driver computation latency |
| `metroride_active_drivers` | Gauge | `routing-service` | Available drivers in routing state |
| `metroride_kafka_driver_location_events_total` | Counter | [`analytics-service`](../services/analytics-service/cmd/main.go) (`kafka` profile only) | Driver-location events consumed from Kafka |
| `metroride_kafka_consume_errors_total` | Counter | `analytics-service` | Kafka consume or decode failures |
| `metroride_kafka_last_event_timestamp_seconds` | Gauge | `analytics-service` | Timestamp of the most recent Kafka event |

The default Prometheus configuration also scrapes `analytics-service:8086`, which only exists under the `kafka` Compose profile, so that target reads as down in the default profile.

## Grafana Dashboards

Grafana is provisioned from `infrastructure/grafana/provisioning/` (Prometheus datasource and dashboard provider) and loads the single dashboard at `infrastructure/grafana/dashboards/metroride-overview.json`. Its five panels are:

- Ride Requests (rate of `metroride_ride_requests_total`).
- Dispatch Latency p95 (from `metroride_dispatch_latency_seconds`).
- Active Drivers (`metroride_active_drivers`).
- Routing Computation p95 (from `metroride_routing_computation_seconds`).
- Assignment Failures (rate of `metroride_assignment_failures_total`).

Local Grafana:

```text
http://localhost:3000
admin / admin
```

## Health Checks

Every service exposes:

```http
GET /healthz
GET /readyz
```

`/healthz` is a liveness check and always returns `200` while the process serves HTTP. `/readyz` runs the service's named dependency checks one after another, each under its own 1.5-second deadline (dispatch's `routing_service` check instead uses a 3-second HTTP client), and returns `503` with the failing check names when any fails:

| Service | Readiness checks |
| --- | --- |
| `rider-service` | `postgres` only, so ride intake stays ready during a Redis outage; the outbox relay catches up when Redis returns |
| `driver-service` | `redis` (the optional Kafka producer is not probed) |
| `dispatch-service` | `postgres`, `redis`, `ride_request_stream`, `routing_service` |
| `routing-service` | `redis`, `driver_location_stream` |
| `traffic-service` | `redis` |
| `notification-service` | `redis`, `notification_stream` |
| `analytics-service` | `kafka` |

The Helm chart wires `/healthz` and `/readyz` into liveness and readiness probes for all six core services. The raw manifests in `infrastructure/k8s` only probe `rider-service` (both) and `dispatch-service` (readiness only).

## Structured Logging

Services use JSON logs with a `service` field. Workflow logs include identifiers such as `ride_id`, `rider_id`, `driver_id`, stream message IDs, and ETA values where relevant.

This makes the logs suitable for ingestion into systems such as:

- Grafana Loki
- Datadog Logs
- Google Cloud Logging
- Elasticsearch/OpenSearch

## Alerting Strategy

Recommended production alerts:

- High p95 dispatch latency over a sustained window.
- Non-zero assignment failure rate.
- Routing service unavailable or returning no drivers.
- Redis Stream consumer lag above threshold.
- PostgreSQL connection failures.
- Sudden drop in active drivers.

## Future Observability Work

- Add OpenTelemetry tracing across REST calls and Redis event processing.
- Add stream lag metrics per consumer group.
- Add a dead-letter counter. `metroride_assignment_failures_total` counts messages that exhausted their assignment retries and is incremented before the dead-letter publish is attempted, so the `events.dead_letter` stream is the only record of what was actually dead-lettered.
- Add RED metrics for every REST endpoint: rate, errors, duration.
- Add resource dashboards for CPU, memory, goroutines, and database pool utilization.
