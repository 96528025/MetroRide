# MetroRide Reliability and Failure Handling

MetroRide uses dependency readiness, bounded foreground retries, explicit timeouts, idempotent assignment, transactional outbox delivery, and dead-letter handling around the core ride workflow.

## Timeout Strategy

All external dependency calls are bounded:

- Redis operations use short request contexts and Redis client dial/read/write timeouts.
- PostgreSQL reads and writes use bounded contexts around query and transaction calls.
- `dispatch-service` calls `routing-service` through an HTTP client with an explicit timeout and per-request context.
- Readiness checks use shorter timeouts so orchestration probes fail quickly instead of hanging.

The goal is to fail fast, log clearly, and preserve the ability to retry transient failures without blocking worker loops indefinitely.

## Retry Strategy

Foreground transient operations use bounded retries with exponential backoff:

- Dispatch message processing retries before a ride request is considered failed.
- Dispatch-to-routing calls retry because routing failures may be transient.
- Dead-letter publication uses bounded retries before surfacing failure.

Those request and consumer retries are intentionally bounded. The durable outbox is the explicit exception: unpublished rows remain scheduled until delivery succeeds, using exponential backoff capped at 30 seconds. A failed row does not extend request latency or block other eligible rows, but there is not yet an attempt ceiling or outbox dead-letter path.

## Idempotency Design

`dispatch-service` treats ride assignment as idempotent:

1. Before routing, it checks the ride's persisted PostgreSQL state.
2. If the ride is no longer `requested` or already has a `driver_id`, duplicate `ride_requested` delivery is skipped.
3. Assignment updates are guarded with `where status = 'requested'`.
4. If another worker already assigned the ride, the duplicate worker exits without creating another assignment.

This protects the PostgreSQL state transition when the same logical ride request reaches the handler more than once. It does not by itself recover work abandoned in a consumer group's pending-entry list: the current readers request only new stream entries and do not claim pending entries.

## Transactional Outbox

Both state-changing workflow steps use a PostgreSQL outbox:

1. `rider-service` commits the new ride and its `ride_requested` event in one transaction.
2. `dispatch-service` commits the assignment and both downstream `ride_assigned` deliveries in one transaction.
3. A relay scoped to each service selects unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes them to Redis Streams, and records `published_at`.

Delivery is intentionally at-least-once. If Redis accepts an event and the relay crashes before PostgreSQL records the publication, the same envelope may be published again. The envelope ID remains stable across attempts, and the authoritative assignment transition is idempotent. This avoids the state/event dual-write gap without claiming exactly-once delivery across PostgreSQL and Redis.

Relay progress is isolated per row. When one event cannot be published, the relay records that attempt, assigns a capped exponential retry time, and continues through the batch. Eligible rows are ordered by retry time so poison rows cannot monopolize every batch while retries still make progress during sustained new traffic. The relay-progress integration test creates real Redis `WRONGTYPE` failures and verifies both that an earlier delivery is not replayed and that a healthy event behind a full batch of poison rows is still published.

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

If dead-letter publication succeeds, the original stream message is acknowledged so it does not poison the consumer group indefinitely. If dead-letter publication fails, the original message remains pending. The repository does not yet include the claim/replay worker needed to recover that pending entry automatically.

### Automated Verification Status

The CI-required routing-outage integration test verifies this path across real components rather than mocks. It stops `routing-service`, creates a ride through `rider-service`, lets the running dispatch consumer exhaust bounded retries, reads the matching entry from the real Redis dead-letter stream, and confirms in PostgreSQL that the ride remains `requested` with no assignment row.

The routing failure test validates that path specifically. A separate Redis-outage test proves that ride state and an unpublished event commit while Redis is stopped, then verifies automatic relay and assignment after Redis restarts. PostgreSQL outages, process termination at every relay boundary, abandoned Redis pending-entry claiming, and dead-letter replay remain outside current automated coverage.

## Failure Modes

### Routing Service Unavailable

If `routing-service` is unavailable, `dispatch-service` retries the route request. If all retries fail, the ride remains in `requested` state and the failed event is written to `events.dead_letter`. Operators can inspect the payload, but replay or repair is currently manual because no dead-letter replay tool is implemented.

### Redis Unavailable

If Redis is unavailable:

- `rider-service` remains ready while PostgreSQL is healthy because accepting a ride only requires the database transaction that stores the ride and its outbox event. Redis is not a startup or readiness dependency for this service.
- The rider outbox relay treats Redis loss as degraded delivery: it logs publication failures and increments `metroride_outbox_publish_failures_total` while retrying unpublished rows.
- Readiness checks for services that must use Redis synchronously still fail.
- Direct Redis operations increment dependency error metrics and log structured errors; outbox relays use their dedicated failure metric.
- Stream consumers increment stream consume error metrics.
- Ride creation commits both the ride and its outbox event, still returns `202`, and requires no client retry.
- The relay retains the unpublished row, records failed attempts, and publishes it after Redis recovers.
- Dispatch state changes use the same pattern, so an assignment cannot commit without durable publication intent.

### PostgreSQL Unavailable

If PostgreSQL is unavailable:

- `rider-service` and `dispatch-service` readiness checks fail.
- Ride creation and ride status reads fail fast with bounded query timeouts.
- Dispatch assignment fails before mutating ride state and can be retried or dead-lettered.

### Dispatch Service Restarts

Redis Stream consumer groups preserve unacknowledged messages. If `dispatch-service` restarts before acknowledging one, the entry remains pending rather than disappearing. The current worker reads new messages with `>` and does not claim abandoned pending entries, so it will not resume that work automatically. `XAUTOCLAIM`/`XCLAIM` handling or dedicated replay tooling is required; if a request is later delivered again, the PostgreSQL guard prevents a second assignment state transition.

## Metrics

Reliability-related metrics include:

- `metroride_rides_assigned_total`
- `metroride_assignment_failures_total`
- `metroride_stream_consume_errors_total`
- `metroride_dependency_errors_total`
- `metroride_outbox_events_published_total`
- `metroride_outbox_publish_failures_total`
- `metroride_dispatch_latency_seconds`

These metrics are designed for alerting on dependency health, dispatch failure rate, and stream processing reliability.
