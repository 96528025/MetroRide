# MetroRide Testing and CI

MetroRide uses automated validation to keep the local distributed system reliable as the codebase evolves. The test strategy is intentionally backend-focused: it validates Go packages, Docker Compose configuration, service readiness, the ride assignment workflow, duplicate-event idempotency, Redis-outage recovery, process-kill recovery of a committed outbox row, outbox relay progress, a real routing-outage dead-letter path, and the ride-to-fare-settlement flow across the Go services and the Java fare-service.

## CI Pipeline

GitHub Actions runs on `pull_request`, on `push` to `main`, on `v*` tags and on
manual dispatch. The full pipeline — including how images are published and
deployed — is documented in [cicd.md](cicd.md); this page covers the test
layers.

### Validation job (every event)

1. Checkout repository.
2. Set up Go from the version in `go.mod`.
3. Fail if any tracked Go file is not `gofmt`-formatted. The check compares
   `gofmt` output and reports the offending files; it never rewrites files
   during CI.
4. Run `go vet ./...`.
5. Run package tests with `go test -race ./...`.
6. Validate Docker Compose with `docker compose config`.
7. Build all service images with `docker compose build`.
8. Start the stack with `docker compose up -d`.
9. Run `bash scripts/smoke-test.sh`.
10. Run integration tests without the Go test cache using `go test -race -count=1 -tags=integration ./tests/integration`.
11. Stop Redis, accept a ride durably, restart Redis, and verify automatic outbox recovery.
12. Stop Redis, accept a ride durably, kill `rider-service` with SIGKILL while its
    outbox row is unpublished, restart both, and verify the restarted relay
    publishes the row exactly once and dispatch assigns the ride.
13. Stop `routing-service` and run `bash scripts/failure-integration-test.sh`.
14. Verify retry exhaustion, the real Redis dead-letter entry, and unchanged
    PostgreSQL ride state.
15. Print focused service and dependency logs if an outage test fails, plus
    full Compose logs for any CI failure.
16. Shut down the stack with `docker compose down -v`, even when an earlier
    step fails.

Nothing is published and nothing is deployed unless this job passes.

### fare-service job (every event)

An independent job for the Java service. It sets up Temurin 21, restores the
Maven cache, and runs `./mvnw -B verify` in `services/fare-service`: the unit
tests through surefire, then the Testcontainers integration test through failsafe
against real `postgres:16-alpine` and `redis:7-alpine` containers on the runner.
The `backend` job does not build or start fare-service: it sits behind the
optional `fare` Compose profile and is not part of the smoke test, the
published image set, or the Helm chart. The cross-service check lives in the
next job.

### Ride-to-fare-settlement flow job (every event)

`fare-end-to-end` runs `bash scripts/fare-e2e-test.sh`, the same entry point
used locally. The script starts its own Compose project (core Go services plus
the `fare` profile, no Prometheus/Grafana), runs the `fareintegration` Go test
against it, writes every service log to a directory that is uploaded as a
workflow artifact when the job fails, and removes the project's containers,
volumes and built images whether the test passed or not. It is a separate job so
its stack is never shared with the backend job's outage tests, which stop Redis
and kill rider-service. Like the `fare-service` job it is not a dependency of
image publication or deployment validation: it proves the Go-to-Java chain in
Compose and does not add fare-service to GHCR, the Helm chart or KinD.

### Deployment-validation job (every event)

After validation, the release is installed on a throwaway KinD cluster and the
smoke test is re-run against the deployed system. Pull requests deploy images
built in the runner; trusted events deploy the exact SHA-tagged images that
were published to and pulled back from GHCR. Both paths run the same
deployment, smoke test, diagnostics and unconditional teardown — see
[cicd.md](cicd.md).

### Concurrency

Superseded pull-request runs on the same ref are cancelled so obsolete runs do
not hold runners. Cancellation is disabled for pushes to `main`, so a trusted
delivery run is never interrupted mid-publish.

## Unit Tests vs Smoke Tests vs Integration Tests

### Package Build/Test Gate

```bash
go test -race ./...
```

This command compiles all 15 Go packages and runs 35 untagged unit tests across nine packages under the Go race detector. They cover event-envelope encoding and decoding, configuration defaults and overrides, common HTTP and readiness behavior, retry and timeout helpers, the dispatch-to-routing request contract and failure paths, the API algorithm label, nearest-driver correctness, retry-delay capping, the rider service's PostgreSQL-only readiness contract, and the routing and notification consume loops returning once their context is cancelled. The tests do not require Docker Compose or external services.

The race detector is a guard for future concurrent code, not evidence about the current system: the only goroutines in the unit tests are the two consume-loop cancellation tests, which run a loop against an unreachable Redis until its context is cancelled, and there is no `t.Parallel`, so it is expected to report nothing. In the integration run it instruments the in-process relay used by the relay-progress test and the pgx and go-redis client internals, while the six services under test run uninstrumented in their containers. It therefore says nothing about relay or consumer concurrency in the running services.

Seven packages still have no unit-test files: the analytics, driver, notification, and traffic service entry points, plus the Kafka, logging, and metrics helpers. The repository therefore makes no comprehensive unit-coverage claim.

The routing benchmark is opt-in so ordinary test runs remain fast:

```bash
go test -run '^$' -bench BenchmarkSelectNearestDriver10000 -benchmem ./services/routing-service/cmd
```

### Smoke Test

```bash
bash scripts/smoke-test.sh
```

The smoke test assumes the Compose stack is already running. It validates:

- `/healthz` for every Go service.
- `/readyz` for every Go service.
- `/metrics` for key services.
- Ride creation through `rider-service`, asserting an exact `HTTP 202`.
- Event-driven dispatch through Redis Streams.
- Final ride state becomes `assigned`.
- Assigned ride includes a non-empty `driver_id`.

The script waits for services before asserting behavior, so it works both locally and in GitHub Actions.

### Kubernetes Smoke Test

```bash
bash scripts/kind-smoke-test.sh
```

Runs against the Helm release installed on the ephemeral KinD cluster. It waits
for `postgres`, `redis` and all six core service Deployments to report
Available, opens `kubectl port-forward` to the six services, and then runs the
same `scripts/smoke-test.sh` above, so Compose and Kubernetes are validated by
identical assertions.

It then confirms the same outcome through two further channels:

- **PostgreSQL:** the `rides` row is `assigned` with a non-null `driver_id`, and
  exactly one `ride_assignments` row exists for that ride.
- **notification-service:** `GET /v1/notifications/stats` reports at least one
  processed assignment event.

Every wait is bounded polling against a deadline with a descriptive failure
message. There are no fixed sleeps used as synchronisation.

### Integration Tests

```bash
go test -race -count=1 -tags=integration ./tests/integration
```

Integration tests require the Docker Compose stack to be running. They validate the backend workflow through real service boundaries:

- Happy path ride assignment.
- Duplicate `ride_requested` event handling.
- Idempotency: a duplicated event must not create a second assignment for the same ride.
- Outbox relay progress: undeliverable rows must neither replay an earlier delivery nor starve healthy work queued behind them.

The tests use the public rider API, Redis Streams, and PostgreSQL state to verify distributed behavior.

The relay-progress test runs its own relay under a unique `source_service` and points 25 events at a Redis key containing the wrong data type. This fills the production-sized batch with poison rows. The test waits for repeated attempts, verifies an earlier healthy event was not replayed, and verifies a later healthy event still reached its stream.

### Routing-Outage Failure Integration Test

```bash
bash scripts/failure-integration-test.sh
```

The script requires the default Compose stack to be running. It stops `routing-service`, waits with a bounded deadline until the service is unreachable, runs the `failureintegration` Go test, and restores routing on exit.

The Go test:

1. Confirms routing is unavailable.
2. Records the current end of `events.dead_letter` so old failures cannot satisfy the test.
3. Creates a ride through the public rider API, which persists the ride and its outbox entry before a relay publishes the real `ride_requested` event to Redis Streams.
4. Lets the running dispatch consumer exhaust its production bounded-retry path.
5. Polls only new dead-letter records and matches the exact ride and original event ID.
6. Validates the dead-letter event type, dispatch source, routing error context, and failure timestamp.
7. Confirms the ride remains `requested`, has no driver, and has zero rows in `ride_assignments`.

The test uses a 30-second context deadline and Redis blocking reads with short polling intervals. It does not rely on a fixed delay or a mock transport.

### Redis-Outage Outbox Recovery Test

```bash
bash scripts/outbox-recovery-test.sh
```

The script stops the real Redis container, verifies that `rider-service` remains ready, creates a ride through the public API with an exact `HTTP 202`, and confirms PostgreSQL contains exactly one unpublished `ride_requested` outbox row. It then restarts Redis and waits until the relay has published every event for that ride and dispatch has assigned it. This verifies that deployment readiness preserves traffic to the durable write path and that the accepted request recovers without a client retry.

### Process-Kill Outbox Recovery Test

```bash
bash scripts/process-kill-recovery-test.sh
```

The script proves that a ride request committed to PostgreSQL survives a `SIGKILL` of the process that owns its outbox relay. It stops Redis so publication cannot happen, creates a ride through the public API with an exact `HTTP 202`, confirms exactly one unpublished `ride_requested` outbox row, and then kills `rider-service` with `SIGKILL` and asserts the container is no longer running. It restarts Redis and `rider-service` together (rider-service's `depends_on` lists only PostgreSQL, so both are named explicitly), waits for `rider-service` readiness, and then waits until the ride is `assigned` and no unpublished row remains for it. Finally it reads `events.ride.requests` with `XRANGE` and requires the event ID to appear exactly once, because the kill happened before any publication and so leaves no window for the documented at-least-once duplicate.

Stopping Redis first makes the kill window deterministic instead of racing a relay that polls every 250 ms. Redis is stopped rather than removed because the dispatch consumer group lives in Redis and is only recreated at dispatch-service startup, and `dispatch-service` keeps running throughout because it exits at startup when Redis is unreachable. The recovery wait is bounded by `PROCESS_KILL_RECOVERY_TIMEOUT_SECONDS` (default 90): failed rows are rescheduled with a backoff capped at 30 seconds, and the row fails a few times while Redis is stopped, so publication after restart can legitimately lag by up to about 30 seconds.

This covers one relay crash window: termination after the PostgreSQL commit and before any publication. The other window, termination after Redis has accepted the event but before the transaction recording `published_at` commits, is the one that produces the documented at-least-once duplicate and is not tested. A `dispatch-service` restart while a consumed request is still unacknowledged is not tested either: consumers read with `>` and never reclaim pending entries, so that entry would stay pending by design (see [reliability.md](reliability.md)).

### Ride-to-Fare-Settlement Flow

```bash
bash scripts/fare-e2e-test.sh
```

Prerequisites: Docker with Compose v2 and free host ports 5432, 6379, 8080–8083
and 8087. Go is optional; without it the test runs from a `golang:1.22`
container against the published ports. The script refuses to start while those
ports are in use, so stop a running MetroRide stack first (`docker compose
--profile fare down` keeps its volumes; the flow never touches them because it
uses a Compose project name unique to the run).

The script builds and starts rider, driver, routing, dispatch and fare-service
with PostgreSQL and Redis, waits for every `/readyz`, and runs one Go test,
`TestRideToFareSettlement` in `tests/fareintegration`. Business actions use only
the public HTTP APIs; PostgreSQL and Redis are read to check results and never
written to skip a step. Stage by stage:

1. Create a ride with a unique `rider_id` through rider-service (`202`, a UUID
   `ride_id`).
2. Poll the ride until it is `assigned`; read the single `ride_assignments` row
   and the `ride_assigned` envelope on `events.ride.assignments` to learn this
   run's driver, assignment ID and assignment event ID.
3. Poll the fare ledger until the `quote_hold` exists, and require that it is the
   only entry: `rider_receivable +quote`, `fare_hold -quote`, quote positive,
   `source_event_id` equal to the assignment envelope. The hold is observed
   before the completion is requested, so the settlement is a reaction to it.
4. Complete the ride through rider-service (`202` with the `ride_completed`
   envelope ID).
5. Poll the ledger until it holds exactly one `quote_hold`, one `hold_reversal`
   and one `settlement`. Every entry sums to zero, the reversal is the hold
   negated posting for posting, the settlement debits the held quote, credits the
   driver `quote × share` rounded half up once and the platform the remainder
   (a zero share is omitted), and both new entries carry the completion envelope
   as `source_event_id`. The share is `FARE_DRIVER_SHARE`, which the script passes
   both to fare-service (through the test overlay
   `tests/fareintegration/compose.fare-e2e.yml`) and to the test.
6. Wait for `fare_settled` on `events.ride.fares`: envelope type, source and
   correlation ID; ride, rider, driver and assignment IDs; `settlement_event_id`
   equal to the completion envelope; quote, driver and platform amounts equal to
   the ledger; `driver_share` equal to the configured value; the amounts and the
   share are JSON strings and `settled_at` parses as RFC 3339. `fare.event_outbox`
   holds exactly one `fare_settled` row for the ride, `published_at` set, whose
   stored envelope equals the published one (compared as decoded JSON, because
   `jsonb` does not keep key order). Several stream copies are accepted only if
   they are the same envelope ID and content: the relay is at-least-once.
7. Complete the ride again: `409 {"error":"ride is completed"}`, the ledger's
   entry IDs are unchanged, there is still one `ride_completed` outbox row and one
   distinct `ride_completed` envelope, and still one `fare_settled` row and one
   distinct `fare_settled` envelope.

Amounts are compared as integer cents parsed from the two-decimal strings, never
as floating point. Nothing is asserted from a demo value such as `2.85`: the quote
is whatever fare-service held for the driver dispatch chose, and the split is
computed from that quote and the configured share. Every wait is bounded polling
(45 s per stage, 5 s per request), and a failure prints the stage, the ride,
rider, driver, assignment and event IDs, and the last state observed.

What the flow does not claim: the quote is still fare-service's current model,
base fare plus the assigned driver's distance and ETA to the pickup point, not a
metered passenger trip; and the flow does not exercise fault injection, load, or
any payment.

## Running Everything Locally

```bash
gofmt -l .                     # must print nothing
go vet ./...
go test -race ./...
docker compose config
docker compose build
docker compose up -d
bash scripts/smoke-test.sh
go test -race -count=1 -tags=integration ./tests/integration
bash scripts/outbox-recovery-test.sh
bash scripts/process-kill-recovery-test.sh
bash scripts/failure-integration-test.sh
docker compose down -v
bash scripts/fare-e2e-test.sh   # after the stack is down: it needs the same host ports
```

To also reproduce the Kubernetes deployment validation locally, see the
step-by-step commands in [cicd.md](cicd.md#running-it-locally).

If local ports are unavailable, stop the conflicting process or adjust the Compose port mappings before running the stack.

## Current Coverage Boundary

The automated suite covers the happy path, duplicate-event idempotency, Redis outage and recovery, a `SIGKILL` of `rider-service` between the PostgreSQL commit and Redis publication, outbox progress across full batches of poison rows, routing outage, retry exhaustion, dead-letter publication, preservation of unassigned PostgreSQL state, and one ride settled end to end through the Java fare-service (hold, reversal, settlement, `fare_settled` and its outbox row, refused second completion). It does not claim to cover PostgreSQL outages, a relay crash after Redis has accepted an event but before `published_at` is recorded (the at-least-once duplicate window), a `dispatch-service` restart while a consumed request is still unacknowledged, abandoned Redis pending-entry claiming, dead-letter replay, or every malformed event.

## Future Testing Improvements

- Add focused tests for the seven remaining packages where their behavior warrants isolation.
- Add dead-letter replay tests.
- Add stream lag assertions and pending-entry claiming tests.
- Add Redis-backed publish/consume contract tests for event envelopes.
- Add GitHub Actions matrix testing across Go versions.
- Add Kubernetes failure-path validation (dependency outage inside the cluster).
