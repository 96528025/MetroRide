# Event Contracts

MetroRide starts with Redis Streams as the durable event log. Services publish JSON envelopes with a stable `type`, `correlation_id`, `occurred_at`, and typed payload.

Streams (see `shared/pkg/events/events.go`):

- `events.ride.requests` (`ride_requested`, from the rider-service outbox)
- `events.driver.locations` (`driver_location_updated`, direct from driver-service)
- `events.ride.assignments` (`ride_assigned`, from the dispatch-service outbox)
- `events.ride.notifications` (`ride_assigned`, from the dispatch-service outbox)
- `events.ride.completions` (`ride_completed`, from the rider-service outbox when `POST /v1/rides/{ride_id}/complete` moves an `assigned` ride to `completed`; payload `RideCompleted`: `ride_id`, `rider_id`, `driver_id`, `assignment_id`, `completed_at` as RFC 3339 with nanoseconds)
- `events.ride.cancellations` (`ride_cancelled`, from the rider-service outbox when a requested or assigned ride is canceled)
- `events.ride.fares` (`fare_settled`, from the fare-service outbox (`fare.event_outbox`, a Java relay) once a ride's hold has been reversed and settled; payload `FareSettled`: `ride_id`, `rider_id`, `driver_id`, `assignment_id`, `settlement_event_id` (the `ride_completed` envelope ID), `quote`, `driver_amount`, `platform_amount`, `driver_share` as decimal strings, `settled_at` as RFC 3339 with fractional seconds; nothing consumes it yet; at-least-once, deduplicate on the envelope `id`)
- `events.traffic.updates` (`traffic_updated`, direct from traffic-service)
- `events.dead_letter` (`dead_lettered`, direct `XADD` by Go consumers for malformed or retry-exhausted deliveries, and by fare-service for poison, quarantined, or retry-exhausted events; the source is acknowledged only after successful publication; nothing consumes this stream yet)

`notification_created` exists as a constant and is not published by any service.

The optional `kafka` Compose profile already carries driver locations on Kafka, using a separate flat `DriverLocationEvent` (`shared/pkg/kafka/events.go`) rather than this envelope. Moving the Redis streams to Kafka would mean choosing between that flat shape and the envelope; the envelope itself is transport-neutral.

`services/fare-service` (Java) runs under the optional `fare` Compose profile and consumes assignments, completions, and cancellations through its `fare-service` group. It deduplicates envelope IDs and serializes each ride's ledger transitions through `fare.ride_state`. Version 2 assignments quote passenger distance and duration, preserving immutable rate and driver-share context. Completion settles the stored quote; cancellation reverses an existing hold without a fee. A completion arriving before assignment remains retryable. A cancellation arriving first records a tombstone that prevents a later assignment from creating a hold. `fare_settled` is published through the Java transactional outbox with at-least-once delivery.

Go dead letters retain `original_stream` and `original_values`, including the original encoded event when present. These optional fields support investigation and deliberate replay without treating a failure summary as the original payload. Default delivery limits and timeout settings are documented in [reliability](../../docs/reliability.md).

## Assignment version 2 and cancellation

`ride_assigned` sets `schema_version: 2`. `distance_km` / `eta_seconds` remain the driver's approach; `trip_distance_km` / `trip_duration_seconds` describe the passenger route. `route_provider` and `route_calculated_at` retain the source and calculation time. Missing passenger inputs cannot create a fare hold.

`events.ride.cancellations` carries `ride_cancelled` from rider-service with `ride_id`, `rider_id`, `cancelled_at`, and optional `driver_id` / `assignment_id`. Cancellation of a requested ride has no assignment. Both release and outgoing event commit with the guarded ride transition. Fare consumers use the persistent cancellation state to handle assignment/cancellation arriving out of order.
