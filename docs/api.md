# MetroRide API

MetroRide exposes a small REST surface for ride intake, status lookup, routing queries, and operational inspection. Most service-to-service workflow coordination happens asynchronously through Redis Streams.

## Common Operational Endpoints

Every Go service exposes:

```http
GET /healthz
GET /readyz
GET /metrics
```

Successful health response:

```json
{
  "status": "ok"
}
```

Successful readiness response:

```json
{
  "status": "ready"
}
```

## Rider Service

Base URL: `http://localhost:8080`

### Create Ride

Creates a ride request, persists it in PostgreSQL, and publishes `ride_requested` to Redis Streams.

```http
POST /v1/rides
Content-Type: application/json
```

Request:

```json
{
  "rider_id": "rider-42",
  "pickup_lat": 37.775,
  "pickup_lng": -122.419,
  "dropoff_lat": 37.789,
  "dropoff_lng": -122.401
}
```

Response:

```http
202 Accepted
Content-Type: application/json
```

```json
{
  "ride_id": "7b2c6e17-8e76-4d4b-b8d6-0c8ff4c7f1b1",
  "status": "requested",
  "event_id": "f2ad55c9-35c5-4bc9-bc97-7d0d2a42ef11"
}
```

### Get Ride

Reads the current ride state from PostgreSQL.

```http
GET /v1/rides/{ride_id}
```

Response:

```json
{
  "id": "7b2c6e17-8e76-4d4b-b8d6-0c8ff4c7f1b1",
  "rider_id": "rider-42",
  "driver_id": "driver-1001",
  "status": "assigned",
  "created_at": "2026-05-19T20:00:00Z",
  "updated_at": "2026-05-19T20:00:02Z",
  "assigned_at": "2026-05-19T20:00:02Z"
}
```

## Dispatch Service

Base URL: `http://localhost:8082`

The dispatch service is primarily event-driven. It consumes `ride_requested` from `events.ride.requests`, calls routing synchronously for nearest-driver selection, updates PostgreSQL, then emits `ride_assigned` and notification events.

Operational endpoints:

```http
GET /healthz
GET /readyz
GET /metrics
```

Important metrics:

- `metroride_dispatch_latency_seconds`
- `metroride_assignment_failures_total`

## Routing Service

Base URL: `http://localhost:8083`

### Find Nearest Driver

Calculates the nearest available driver using the routing service's current driver-location view.

```http
POST /v1/routes/nearest-driver
Content-Type: application/json
```

Request:

```json
{
  "pickup_lat": 37.775,
  "pickup_lng": -122.419
}
```

Response:

```json
{
  "driver_id": "driver-1001",
  "distance_km": 0.08,
  "eta_seconds": 60,
  "algorithm": "haversine-nearest-with-dijkstra-ready-graph",
  "computed_at": "2026-05-19T20:00:02Z"
}
```

## Driver Service

Base URL: `http://localhost:8081`

### List Simulated Drivers

```http
GET /v1/drivers
```

Response:

```json
{
  "drivers": [
    {
      "id": "driver-1001",
      "latitude": 37.7749,
      "longitude": -122.4194,
      "available": true
    }
  ]
}
```

## Traffic Service

Base URL: `http://localhost:8084`

### Current Traffic State

```http
GET /v1/traffic
```

Response:

```json
{
  "regions": {
    "sf-downtown": 1.0,
    "sf-soma": 1.1,
    "sf-mission": 0.9
  }
}
```

## Notification Service

Base URL: `http://localhost:8085`

### Notification Stats

```http
GET /v1/notifications/stats
```

Response:

```json
{
  "processed": 12
}
```
