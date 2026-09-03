# MetroRide Testing and CI

MetroRide uses automated validation to keep the local distributed system reliable as the codebase evolves. The test strategy is intentionally backend-focused: it validates Go packages, Docker Compose configuration, service readiness, the ride assignment workflow, duplicate-event idempotency, and a real routing-outage dead-letter path.

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
5. Run package tests with `go test ./...`.
6. Validate Docker Compose with `docker compose config`.
7. Build all service images with `docker compose build`.
8. Start the stack with `docker compose up -d`.
9. Run `bash scripts/smoke-test.sh`.
10. Run integration tests with `go test -tags=integration ./tests/integration`.
11. Stop `routing-service` and run `bash scripts/failure-integration-test.sh`.
12. Verify retry exhaustion, the real Redis dead-letter entry, and unchanged
    PostgreSQL ride state.
13. Print focused dispatch, routing, and Redis logs if the failure-path test
    fails, plus full Compose logs for any CI failure.
14. Shut down the stack with `docker compose down -v`, even when an earlier
    step fails.

Nothing is published and nothing is deployed unless this job passes.

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
go test ./...
```

This command compiles all Go packages and would run any untagged package tests. The current repository has no untagged `*_test.go` files, so behavioral evidence comes from the tagged integration tests and smoke scripts below. This gate does not require Docker Compose or external services.

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
gofmt -l .                     # must print nothing
go vet ./...
go test ./...
docker compose config
docker compose build
docker compose up -d
bash scripts/smoke-test.sh
go test -tags=integration ./tests/integration
bash scripts/failure-integration-test.sh
docker compose down -v
```

To also reproduce the Kubernetes deployment validation locally, see the
step-by-step commands in [cicd.md](cicd.md#running-it-locally).

If local ports are unavailable, stop the conflicting process or adjust the Compose port mappings before running the stack.

## Current Coverage Boundary

The automated suite covers the happy path, duplicate-event idempotency, routing outage, retry exhaustion, dead-letter publication, and preservation of unassigned PostgreSQL state. It does not claim to cover Redis outages, PostgreSQL outages, dispatch crash recovery, dead-letter replay, every malformed event, or every partial-failure window.

## Future Testing Improvements

- Add service-level unit tests for retry and readiness helpers.
- Add dead-letter replay tests.
- Add stream lag assertions.
- Add contract tests for event envelopes.
- Add GitHub Actions matrix testing across Go versions.
- Add Kubernetes failure-path validation (dependency outage inside the cluster).
- Add race detector runs for selected packages.
