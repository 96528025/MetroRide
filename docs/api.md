# MetroRide API

## Rider Service

### Create Ride

```http
POST /v1/rides
Content-Type: application/json
```

```json
{
  "rider_id": "rider-42",
  "pickup_lat": 37.775,
  "pickup_lng": -122.419,
  "dropoff_lat": 37.789,
  "dropoff_lng": -122.401
}
```

Returns `202 Accepted` with a `ride_id`. Dispatch proceeds asynchronously through Redis Streams.

### Get Ride

```http
GET /v1/rides/{ride_id}
```

Returns the current persisted ride state from PostgreSQL.

## Routing Service

### Nearest Driver

```http
POST /v1/routes/nearest-driver
Content-Type: application/json
```

```json
{
  "pickup_lat": 37.775,
  "pickup_lng": -122.419
}
```
