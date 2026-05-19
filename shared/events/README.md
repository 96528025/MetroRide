# Event Contracts

MetroRide starts with Redis Streams as the durable event log. Services publish JSON envelopes with a stable `type`, `correlation_id`, `occurred_at`, and typed payload.

Primary streams:

- `events.ride.requests`
- `events.driver.locations`
- `events.ride.assignments`
- `events.ride.notifications`
- `events.traffic.updates`

Kafka can be introduced behind the same envelope contract by replacing the publisher and consumer adapters.
