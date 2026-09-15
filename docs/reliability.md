# Reliability and Failure Recovery

## Transactional outbox

Ride creation, assignment, completion, and cancellation commit their state change and outgoing events in one PostgreSQL transaction. The assignment transaction also acquires the driver's unique reservation. Outbox relays publish at least once, so consumers must tolerate redelivery. A confirmed database commit is separate from asynchronous event publication.

## Consumer recovery

The Go consumers for dispatch, driver locations, and notifications read new deliveries and periodically reclaim eligible pending entries with `XAUTOCLAIM`. Each instance has its own consumer name, and reclaim scanning retains its cursor. Successful processing is acknowledged only after the relevant state change has completed.

Reclaiming does not make delivery exactly-once. Dispatch uses guarded database transitions and unique reservations; location updates reject older timestamps. Notification logs and delivery counters can repeat after redelivery and are not presented as unique notifications.

Retryable failures remain pending. Malformed messages and exhausted delivery attempts are copied to the dead-letter stream before acknowledgment. If dead-letter publication fails, the original remains pending. Reclaim idle time must exceed the bounded processing budget; repeated delivery remains possible and is handled by the state guards.

| Go setting | Default | Purpose |
| --- | --- | --- |
| `STREAM_PROCESS_TIMEOUT_SECONDS` | 60 | Overall processing budget per delivery |
| `STREAM_MIN_IDLE_SECONDS` | 120 | Minimum idle time before reclaim; must exceed processing budget |
| `STREAM_RECLAIM_INTERVAL_SECONDS` | 5 | Interval between cursor-based pending scans |
| `STREAM_MAX_DELIVERIES` | 25 | Redis delivery-count limit for retryable failures |
| `CONSUMER_NAME` | Service name plus a fresh UUID | Unique consumer identity unless explicitly configured |

The Go loop reads one message at a time, interleaves new reads and pending scans, and uses bounded Redis operations. Routing HTTP retries share the delivery deadline. A delivery exhausted after a prolonged dependency failure goes to `events.dead_letter`; restoration does not automatically replay that stream. Operators must inspect the recorded event and current ride state before deliberate replay.

Fare-service has its own Java consumer configuration and failure classes. See its [README](../services/fare-service/README.md) for poison, retryable, quarantine, and fatal failures. Its persistent ride-state lock serializes assignment, completion, and cancellation, including cancellation arriving before the hold.

## State guards

Driver locations and active reservations are shared through PostgreSQL. A location update can refresh a driver's position without clearing an active reservation. Stale location reports are excluded from selection, and an older event cannot overwrite a newer position.

Dispatch considers available drivers with recent locations. It ranks a bounded shortlist using estimated road travel time to the pickup; the shortlist is not a global optimization across every driver. The assignment, exclusive driver reservation, ride state change, and outgoing events commit in one database transaction. If another ride reserves the candidate first, dispatch retries selection.

Completing or canceling an assigned ride releases its driver in the same transaction as the ride state change and outgoing event. Conditional state changes prevent completion and cancellation from both succeeding for the same ride. A delayed duplicate request cannot reserve a driver again for an ended ride.

## Timeouts and boundaries

Go PostgreSQL operations use two-second contexts, including commit and rollback. Redis operations use two-second budgets; readiness checks use 1.5 seconds. Routing provider calls have ten-second deadlines inside the sixty-second operation budget. A failed response does not prove an earlier commit was rolled back; inspect current ride state before retrying.

Driver locations remain simulated. The notification endpoint counts processing attempts, so recovery may increase it more than once for the same assignment. There is no email/SMS delivery or payment gateway. Redis loss/retention, external provider availability, manual dead-letter replay, and client-create deduplication remain operational limits.

## Verification

`stream_consumer_test.go` checks restart after a simulated durable effect before acknowledgment, retry caps, failed dead-letter publication, and timeout configuration against real Redis. Routing tests check shared persistence, stale updates, active reservations, response validation, and concurrent cache use. Full-stack tests and outage scripts are listed in [testing and CI](testing-and-ci.md).
