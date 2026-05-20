# MetroRide Testing and CI

MetroRide uses automated validation to keep the local distributed system reliable as the codebase evolves. The test strategy is intentionally backend-focused: it validates Go packages, Docker Compose configuration, service readiness, the ride assignment workflow, and idempotent event handling.

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
9. Print Docker Compose logs on failure.
10. Shut down the stack with `docker compose down -v`.

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

## Running Everything Locally

```bash
go test ./...
docker compose config
docker compose build
docker compose up -d
bash scripts/smoke-test.sh
go test -tags=integration ./tests/integration
docker compose down -v
```

If local ports are unavailable, stop the conflicting process or adjust the Compose port mappings before running the stack.

## Routing Failure Test Coverage

The current automated integration suite covers happy path assignment and duplicate-event idempotency. Routing failure behavior is documented in `docs/reliability.md`, but full automated simulation is intentionally deferred because it would require orchestration control during the test, such as stopping `routing-service`, injecting a bad `ROUTING_SERVICE_URL`, or running a separate failure-mode Compose profile.

Future work should add a dedicated failure-mode test that:

1. Starts dispatch with an unreachable routing endpoint.
2. Publishes a `ride_requested` event.
3. Waits for retry exhaustion.
4. Verifies an event appears in `events.dead_letter`.

## Future Testing Improvements

- Add service-level unit tests for retry and readiness helpers.
- Add a Docker Compose failure profile for routing outages.
- Add dead-letter replay tests.
- Add stream lag assertions.
- Add contract tests for event envelopes.
- Add GitHub Actions matrix testing across Go versions.
- Add race detector runs for selected packages.
