# Event Contracts

MetroRide starts with Redis Streams as the durable event log. Services publish JSON envelopes with a stable `type`, `correlation_id`, `occurred_at`, and typed payload.

Streams (see `shared/pkg/events/events.go`):

- `events.ride.requests` (`ride_requested`, from the rider-service outbox)
- `events.driver.locations` (`driver_location_updated`, direct from driver-service)
- `events.ride.assignments` (`ride_assigned`, from the dispatch-service outbox)
- `events.ride.notifications` (`ride_assigned`, from the dispatch-service outbox)
- `events.ride.completions` (`ride_completed`, from the rider-service outbox when `POST /v1/rides/{ride_id}/complete` moves an `assigned` ride to `completed`; payload `RideCompleted`: `ride_id`, `rider_id`, `driver_id`, `assignment_id`, `completed_at` as RFC 3339 with nanoseconds)
- `events.traffic.updates` (`traffic_updated`, direct from traffic-service)
- `events.dead_letter` (`dead_lettered`, written with a direct `XADD`, not via the outbox, by dispatch-service after three failed attempts and by fare-service for an entry it cannot decode, quote or settle or that failed 25 deliveries; same envelope and payload shape; nothing consumes it yet)

`notification_created` exists as a constant and is not published by any service.

The optional `kafka` Compose profile already carries driver locations on Kafka, using a separate flat `DriverLocationEvent` (`shared/pkg/kafka/events.go`) rather than this envelope. Moving the Redis streams to Kafka would mean choosing between that flat shape and the envelope; the envelope itself is transport-neutral.

`services/fare-service` (Java) runs under the optional `fare` Compose profile, consumes `events.ride.assignments` and `events.ride.completions` through the consumer group `fare-service` with one `XREADGROUP`, records each envelope ID once and, for `ride_assigned`, reads `distance_km` and `eta_seconds` from the payload to quote and hold the fare; for `ride_completed` it reverses that hold and settles the quoted amount between driver and platform. It decodes the same `event` field and envelope JSON the Go services publish, and is the first consumer that reads the payload fields rather than only the envelope. The two streams come from two outbox relays, so the completion of a ride can reach fare-service before its assignment; fare-service treats that as a retryable failure and settles once the assignment has landed.
