# MetroRide Testing and CI

MetroRide uses automated validation to keep the local distributed system reliable as the codebase evolves. The test strategy is intentionally backend-focused: it validates Go packages, Docker Compose configuration, service readiness, the ride assignment workflow, duplicate-event idempotency, and a real routing-outage dead-letter path.

## CI Pipeline

GitHub Actions runs on `push` and `pull_request`.

The pipeline performs:

1. Checkout repository.
2. Set up Go.
3. Run package tests with `go test ./...`.
4. Validate Docker Compose with `docker compose config`.
5. Build all service images with `docker compose build`.
6. Start the stack with `docker compose up -d`.
7. Run `bash scripts/smoke-test.sh`.
8. Run integration tests with `go test -tags=integration ./tests/integration`.
9. Stop `routing-service` and run `bash scripts/failure-integration-test.sh`.
10. Verify retry exhaustion, the real Redis dead-letter entry, and unchanged PostgreSQL ride state.
11. Print focused dispatch, routing, and Redis logs if the failure-path test fails, plus full Compose logs for any CI failure.
12. Shut down the stack with `docker compose down -v`, even when an earlier step fails.

## Unit Tests vs Smoke Tests vs Integration Tests

### Unit and Package Tests

```bash
go test ./...
```

These tests compile all Go packages and run normal package-level tests. They do not require Docker Compose or external services.

### Smoke Test

```bash
bash scripts/smoke-test.sh
```

The smoke test assumes the Compose stack is already running. It validates:

- `/healthz` for every Go service.
- `/readyz` for every Go service.
- `/metrics` for key services.
- Ride creation through `rider-service`.
- Event-driven dispatch through Redis Streams.
- Final ride state becomes `assigned`.
- Assigned ride includes a non-empty `driver_id`.

The script waits for services before asserting behavior, so it works both locally and in GitHub Actions.

### Integration Tests

```bash
go test -tags=integration ./tests/integration
```

Integration tests require the Docker Compose stack to be running. They validate the backend workflow through real service boundaries:

- Happy path ride assignment.
- Duplicate `ride_requested` event handling.
- Idempotency: a duplicated event must not create a second assignment for the same ride.

The tests use the public rider API, Redis Streams, and PostgreSQL state to verify distributed behavior.

### Routing-Outage Failure Integration Test

```bash
bash scripts/failure-integration-test.sh
```

The script requires the default Compose stack to be running. It stops `routing-service`, waits with a bounded deadline until the service is unreachable, runs the `failureintegration` Go test, and restores routing on exit.

The Go test:

1. Confirms routing is unavailable.
2. Records the current end of `events.dead_letter` so old failures cannot satisfy the test.
3. Creates a ride through the public rider API, which persists the ride in PostgreSQL and publishes a real `ride_requested` event to Redis Streams.
4. Lets the running dispatch consumer exhaust its production bounded-retry path.
5. Polls only new dead-letter records and matches the exact ride and original event ID.
6. Validates the dead-letter event type, dispatch source, routing error context, and failure timestamp.
7. Confirms the ride remains `requested`, has no driver, and has zero rows in `ride_assignments`.

The test uses a 30-second context deadline and Redis blocking reads with short polling intervals. It does not rely on a fixed delay or a mock transport.

## Running Everything Locally

```bash
go test ./...
docker compose config
docker compose build
docker compose up -d
bash scripts/smoke-test.sh
go test -tags=integration ./tests/integration
bash scripts/failure-integration-test.sh
docker compose down -v
```

If local ports are unavailable, stop the conflicting process or adjust the Compose port mappings before running the stack.

## Current Coverage Boundary

The automated suite covers the happy path, duplicate-event idempotency, routing outage, retry exhaustion, dead-letter publication, and preservation of unassigned PostgreSQL state. It does not claim to cover Redis outages, PostgreSQL outages, dispatch crash recovery, dead-letter replay, every malformed event, or every partial-failure window.

## Future Testing Improvements

- Add service-level unit tests for retry and readiness helpers.
- Add dead-letter replay tests.
- Add stream lag assertions.
- Add contract tests for event envelopes.
- Add GitHub Actions matrix testing across Go versions.
- Add race detector runs for selected packages.
