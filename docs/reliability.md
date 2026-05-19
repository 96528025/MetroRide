# MetroRide Reliability and Failure Handling

MetroRide keeps the existing service architecture and event flow while adding production-oriented reliability controls around dependency readiness, bounded retries, explicit timeouts, idempotent assignment, and dead-letter handling.

## Timeout Strategy

All external dependency calls are bounded:

- Redis operations use short request contexts and Redis client dial/read/write timeouts.
- PostgreSQL reads and writes use bounded contexts around query and transaction calls.
- `dispatch-service` calls `routing-service` through an HTTP client with an explicit timeout and per-request context.
- Readiness checks use shorter timeouts so orchestration probes fail quickly instead of hanging.

The goal is to fail fast, log clearly, and preserve the ability to retry transient failures without blocking worker loops indefinitely.

## Retry Strategy

Transient operations use bounded retries with exponential backoff:

- Dispatch message processing retries before a ride request is considered failed.
- Dispatch-to-routing calls retry because routing failures may be transient.
- Redis publishes from dispatch retry before surfacing failure.

Retries are intentionally bounded. Infinite retry loops can hide outages, increase tail latency, and prevent failed events from moving into an inspectable failure path.

## Idempotency Design

`dispatch-service` treats ride assignment as idempotent:

1. Before routing, it checks the ride's persisted PostgreSQL state.
2. If the ride is no longer `requested` or already has a `driver_id`, duplicate `ride_requested` delivery is skipped.
3. Assignment updates are guarded with `where status = 'requested'`.
4. If another worker already assigned the ride, the duplicate worker exits without creating another assignment.

This protects against Redis Stream redelivery, consumer restarts, and duplicate events. A future production version would add a transactional outbox so assignment persistence and event publication are committed atomically.

## Dead-Letter Stream

Failed dispatch events are published to:

```text
events.dead_letter
```

After retries are exhausted, `dispatch-service` publishes a dead-letter event containing:

- Original event ID.
- Original event type.
- Ride ID when available.
- Error message.
- Service name.
- Failure timestamp.

If dead-letter publication succeeds, the original stream message is acknowledged so it does not poison the consumer group indefinitely. If dead-letter publication fails, the original message is left unacknowledged for later recovery.

## Failure Modes

### Routing Service Unavailable

If `routing-service` is unavailable, `dispatch-service` retries the route request. If all retries fail, the ride remains in `requested` state and the failed event is written to `events.dead_letter`. Operators can inspect the dead-letter payload and replay or repair the ride after routing recovers.

### Redis Unavailable

If Redis is unavailable:

- Readiness checks for Redis-dependent services fail.
- Event publishers increment dependency error metrics and log structured errors.
- Stream consumers increment stream consume error metrics.
- Ride creation may persist in PostgreSQL but return a warning if event publication fails.

In a production system, the next step would be an outbox table so ride creation and later event publishing remain recoverable when Redis is down.

### PostgreSQL Unavailable

If PostgreSQL is unavailable:

- `rider-service` and `dispatch-service` readiness checks fail.
- Ride creation and ride status reads fail fast with bounded query timeouts.
- Dispatch assignment fails before mutating ride state and can be retried or dead-lettered.

### Dispatch Service Restarts

Redis Stream consumer groups preserve unacknowledged messages. If `dispatch-service` restarts before acknowledging a message, the message remains pending and can be recovered by a future consumer. The idempotency check prevents duplicate assignment if the ride was already assigned before the restart.

## Metrics

Reliability-related metrics include:

- `metroride_rides_assigned_total`
- `metroride_assignment_failures_total`
- `metroride_stream_consume_errors_total`
- `metroride_dependency_errors_total`
- `metroride_dispatch_latency_seconds`

These metrics are designed for alerting on dependency health, dispatch failure rate, and stream processing reliability.
