# Event Contracts

MetroRide starts with Redis Streams as the durable event log. Services publish JSON envelopes with a stable `type`, `correlation_id`, `occurred_at`, and typed payload.

Streams (see `shared/pkg/events/events.go`):

- `events.ride.requests` (`ride_requested`, from the rider-service outbox)
- `events.driver.locations` (`driver_location_updated`, direct from driver-service)
- `events.ride.assignments` (`ride_assigned`, from the dispatch-service outbox)
- `events.ride.notifications` (`ride_assigned`, from the dispatch-service outbox)
- `events.traffic.updates` (`traffic_updated`, direct from traffic-service)
- `events.dead_letter` (`dead_lettered`, written by dispatch-service with a direct `XADD` after three failed attempts, not via the outbox; nothing consumes it yet)

`ride_completed` and `notification_created` exist as constants and are not published by any service.

The optional `kafka` Compose profile already carries driver locations on Kafka, using a separate flat `DriverLocationEvent` (`shared/pkg/kafka/events.go`) rather than this envelope. Moving the Redis streams to Kafka would mean choosing between that flat shape and the envelope; the envelope itself is transport-neutral.

`services/fare-service` (Java) runs under the optional `fare` Compose profile, consumes `events.ride.assignments` through the consumer group `fare-service`, records each envelope ID once and, for `ride_assigned`, reads `distance_km` and `eta_seconds` from the payload to quote and hold the fare; it decodes the same `event` field and envelope JSON the Go services publish. It is the first consumer that reads the `RideAssigned` payload fields rather than only the envelope.
