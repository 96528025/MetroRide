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

Current project metrics:

| Metric | Type | Owner | Purpose |
| --- | --- | --- | --- |
| `metroride_ride_requests_total` | Counter | `rider-service` | Total accepted ride requests |
| `metroride_dispatch_latency_seconds` | Histogram | `dispatch-service` | Assignment workflow latency |
| `metroride_assignment_failures_total` | Counter | `dispatch-service` | Failed dispatch attempts |
| `metroride_routing_computation_seconds` | Histogram | `routing-service` | Nearest-driver computation latency |
| `metroride_active_drivers` | Gauge | `routing-service` | Available drivers in routing state |

Portfolio-friendly metric aliases often used in discussion:

- `dispatch_latency_seconds`: assignment latency from stream consumption to assignment emission.
- `routing_duration_ms`: route computation duration, usually derived from routing histogram data.
- `ride_assignments_total`: future counter for successful assignments.

## Grafana Dashboards

Grafana is provisioned from:

```text
infrastructure/grafana/
```

The included dashboard tracks:

- Ride request rate.
- Dispatch latency p95.
- Active drivers.
- Routing computation p95.
- Assignment failure rate.

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

`/healthz` is intended for liveness checks. `/readyz` is intended for readiness checks and future dependency validation. Kubernetes manifests use these endpoints as deployment lifecycle signals.

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
- Add successful assignment counter: `metroride_ride_assignments_total`.
- Add dead-letter stream metrics.
- Add RED metrics for every REST endpoint: rate, errors, duration.
- Add resource dashboards for CPU, memory, goroutines, and database pool utilization.
