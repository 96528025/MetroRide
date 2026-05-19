# MetroRide

MetroRide is a production-style distributed ride dispatch platform built to demonstrate backend and infrastructure engineering: microservices, event-driven workflows, real-time dispatch, Redis Streams, PostgreSQL state, Prometheus metrics, Grafana dashboards, Docker, Kubernetes, and Helm.

It is not a frontend Uber clone. The focus is service boundaries, asynchronous communication, operational hooks, and extensible architecture.

## Services

- `rider-service`: accepts ride requests, stores ride state, publishes `ride_requested`.
- `driver-service`: simulates live driver coordinates and publishes `driver_location_updated`.
- `dispatch-service`: consumes ride requests, assigns drivers, persists assignment state, emits notification events.
- `routing-service`: tracks available drivers and computes nearest-driver ETA.
- `traffic-service`: simulates congestion changes and emits `traffic_updated`.
- `notification-service`: consumes assignment notifications and simulates delivery.

## Run Locally

```bash
docker compose up --build
```

Create a ride:

```bash
curl -X POST http://localhost:8080/v1/rides \
  -H 'Content-Type: application/json' \
  -d '{"rider_id":"rider-42","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}'
```

Check status using the returned `ride_id`:

```bash
curl http://localhost:8080/v1/rides/<ride_id>
```

Or run:

```bash
bash scripts/smoke-test.sh
```

## Local Ports

- Rider service: `8080`
- Driver service: `8081`
- Dispatch service: `8082`
- Routing service: `8083`
- Traffic service: `8084`
- Notification service: `8085`
- Prometheus: `9090`
- Grafana: `3000` (`admin` / `admin`)
- PostgreSQL: `5432`
- Redis: `6379`

## Observability

Every service exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

Key MVP metrics:

- `metroride_ride_requests_total`
- `metroride_dispatch_latency_seconds`
- `metroride_assignment_failures_total`
- `metroride_routing_computation_seconds`
- `metroride_active_drivers`

## Repository Layout

```text
services/
  rider-service/
  driver-service/
  dispatch-service/
  routing-service/
  traffic-service/
  notification-service/
infrastructure/
  docker/
  k8s/
  helm/
  prometheus/
  grafana/
shared/
  proto/
  events/
  utils/
  pkg/
scripts/
docs/
```

See `docs/architecture.md` and `docs/api.md` for design details.
