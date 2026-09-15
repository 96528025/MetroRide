# MetroRide — Event-Driven Dispatch and Fare Settlement

[![CI](https://github.com/96528025/MetroRide/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/96528025/MetroRide/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.22-00ADD8?logo=go&logoColor=white)
![Java](https://img.shields.io/badge/Java-21-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis%20Streams-7-DC382D?logo=redis&logoColor=white)

MetroRide is an event-driven ride-dispatch backend built with Go, PostgreSQL, and Redis Streams, with an optional Java/Spring Boot fare service. It uses road-route estimates for passenger-trip quotes, reserves drivers during active assignments, and recovers unfinished stream deliveries after a consumer restart.

The application models ride acceptance, assignment, completion, cancellation, and a double-entry fare ledger. Driver locations are simulated, and fare settlement records accounting entries without moving money. There is no rider or driver frontend; Grafana provides operational dashboards. The optional Kafka extension carries driver-location telemetry separately from the ride workflow.

## Engineering highlights

| Problem | Implemented approach | Evidence |
| --- | --- | --- |
| Database commits but event publishing fails | Domain changes and outbox rows commit in one PostgreSQL transaction; a relay retries publishing to Redis | [`shared/pkg/outbox`](shared/pkg/outbox), Redis-outage and process-kill recovery scripts |
| A ride request is delivered again | Conditional `requested → assigned` update and assignment insert in one transaction | [`dispatch-service`](services/dispatch-service/cmd/main.go), duplicate-event integration test |
| Completion or settlement is attempted twice | Conditional `assigned → completed` update; fare event-ID deduplication, persistent per-ride row lock, per-ride unique indexes | [`rider-service`](services/rider-service/cmd/main.go), [`fare-service`](services/fare-service/README.md), concurrent completion/settlement tests |
| A consumer crashes or receives unusable data | Go and Java consumers reclaim pending entries and dead-letter failed work before acknowledging | `RideEventConsumer`, `FailureHandler`, Testcontainers recovery tests |
| Images build but fail when deployed | CI installs the six core images through Helm into KinD and drives a ride through the deployed stack | [Deployment workflow](.github/workflows/deploy-validation.yml) |

## Run a ride through fare settlement

Prerequisites: Docker with Compose, Bash, curl, and network access for image downloads and the configured route provider. Start fare-service before creating the ride:

```bash
docker compose --profile fare up --build -d
```

Create a new ride:

```bash
curl -X POST http://localhost:8080/v1/rides \
  -H 'Content-Type: application/json' \
  -d '{"rider_id":"rider-42","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}'
```

Copy this response's `ride_id`, then poll until the ride is assigned and its quote hold exists:

```bash
RIDE_ID='paste-the-new-ride-id-here'
curl "http://localhost:8080/v1/rides/$RIDE_ID"
curl "http://localhost:8087/v1/rides/$RIDE_ID/ledger"
```

Complete the ride and poll the ledger until it contains the hold reversal and settlement:

```bash
curl -X POST "http://localhost:8080/v1/rides/$RIDE_ID/complete"
curl "http://localhost:8087/v1/rides/$RIDE_ID/ledger"
```

HTTP `202` confirms the ride state change; fare processing is asynchronous. To cancel instead, call `/cancel` before completing the ride. Inspect the ride and ledger to confirm the result.

```bash
docker compose --profile fare down
```

This stops the stack and preserves its data volumes.

For the automated flow, run `bash scripts/fare-e2e-test.sh` with the application's ports free. It creates and removes its own Compose project, uses an explicit local route fixture, and checks the ledger and outgoing settlement event. Go is optional for this script because it can run the test client in a Go container.

## Architecture

```mermaid
flowchart LR
    C[API client] -->|Create, complete, or cancel ride| Rider[rider-service / Go]
    Rider -->|Ride state + outbox in one transaction| DB[(PostgreSQL)]
    DB -->|Rider relay| Requests[events.ride.requests]
    Requests --> Dispatch[dispatch-service / Go]
    Driver[driver-service / Go] --> Locations[events.driver.locations]
    Locations --> Routing[routing-service / Go]
    Routing -->|Shared driver positions| DB
    Routing --> Provider[Configured Valhalla provider]
    Dispatch -->|Passenger and driver routes| Routing
    Dispatch -->|Assignment + outbox in one transaction| DB
    DB -->|Dispatch relay| Assignments[events.ride.assignments]
    DB -->|Dispatch relay| Notifications[events.ride.notifications]
    Notifications --> Notify[notification-service / Go]
    DB -->|Rider relay| Completions[events.ride.completions]
    Assignments --> Fare[Optional fare-service / Java]
    Completions --> Fare
    Fare -->|Event deduplication + ledger + outbox in one transaction| DB
    DB -->|Fare relay| Fares[events.ride.fares]
    Dispatch -.-> DLQ[events.dead_letter]
    Fare -.-> DLQ
```

The core data path uses HTTP/JSON, PostgreSQL, and Redis Streams. The repository's `shared/proto` directory is not evidence of an implemented gRPC transport. Traffic simulation and optional Kafka telemetry are separate from the dispatch path shown above.

| Service | Port | Responsibility | Default stack |
| --- | ---: | --- | --- |
| `rider-service` | 8080 | Create/read rides, complete assigned rides, publish through its outbox relay | Yes |
| `driver-service` | 8081 | Publish locations for four simulated drivers every two seconds | Yes |
| `dispatch-service` | 8082 | Consume requests, call routing, persist assignment and two outbox destinations | Yes |
| `routing-service` | 8083 | Share driver positions in PostgreSQL; compare road approaches | Yes |
| `traffic-service` | 8084 | Publish simulated congestion every ten seconds; currently no consumer | Yes |
| `notification-service` | 8085 | Log and count notification deliveries | Yes |
| `analytics-service` | 8086 | Expose latest driver locations consumed from Kafka | Optional `kafka` profile |
| `fare-service` | 8087 | Consume assignments/completions; quote, hold, settle, expose the ledger, and publish `fare_settled` through its own outbox relay | Optional `fare` profile |

## A ride from request to settlement

Driver locations and active reservations are shared through PostgreSQL. A location update can refresh a driver's position without clearing an active reservation. Stale location reports are excluded from selection, and an older event cannot overwrite a newer position.

Dispatch considers available drivers with recent locations. It ranks a bounded shortlist using estimated road travel time to the pickup; the shortlist is not a global optimization across every driver. The assignment, exclusive driver reservation, ride state change, and outgoing events commit in one database transaction. If another ride reserves the candidate first, dispatch retries selection.

Completing or canceling an assigned ride releases its driver in the same transaction as the ride state change and outgoing event. Conditional state changes prevent completion and cancellation from both succeeding for the same ride. A delayed duplicate request cannot reserve a driver again for an ended ride.

A fare quote uses the estimated road distance and duration from the passenger's pickup point to the drop-off point. The driver's approach to the pickup is recorded separately and is not used as the passenger-trip distance.

Routing requests use Valhalla's `auto` costing model through a configurable endpoint. The route provider, calculation time, distance, and duration are stored with the assignment. A routing failure leaves the ride unassigned for retry; the service does not substitute a straight-line distance and label it as a road route.

Fare-service stores the resulting quote and pricing inputs when it creates the hold. Completion settles that stored quote, so a later route or rate change does not reprice an existing hold. These are upfront route-based estimates using configurable demonstration rates. The application does not measure the passenger's actual driven path, apply live traffic pricing, or collect payments.
The default driver share is 0.80. The quote, rate version, and driver share are fixed when the hold is created. Money is represented with decimal arithmetic and rounded to cents at the documented calculation boundaries.

## Fare ledger and pricing boundary

```text
quote = 2.50 + 1.20 * trip_distance_km + 0.30 * trip_duration_seconds / 60
driver share = round_to_cents(quote * stored_driver_share)
platform share = quote - driver share
```

### Cancel a ride

`POST /v1/rides/{ride_id}/cancel`

A requested or assigned ride can be canceled. A successful transition returns `202` with `ride_id`, `status: "cancelled"`, and the cancellation event's `event_id`. An unknown ride returns `404`; a completed or already canceled ride returns `409`.

Cancellation releases any active driver reservation. If fare-service is enabled, it releases an existing quote hold without creating a settlement or cancellation fee. A cancellation received before the assignment event is recorded so that a late assignment cannot create a new hold for the canceled ride. Ledger changes and event deduplication commit together.

Completion remains `POST /v1/rides/{ride_id}/complete` and requires an assigned ride. Completion and cancellation compete for the same guarded state transition; at most one succeeds.

New assignment payloads identify their schema version and separate driver-approach fields from passenger-trip fields. Missing trip fields are not interpreted as zero, and legacy pickup-distance fields are not silently treated as passenger-trip measurements.

Existing ledger entries are preserved. Legacy holds settle using their stored quote under the documented legacy policy; they are not recalculated from a new route. Legacy assignment events that have not yet produced a hold are sent for review instead of creating a quote from unverified passenger-trip inputs. Database migrations preserve existing rows and reject conflicting active assignments rather than choosing which ride owns a driver.

See [fare-service](services/fare-service/README.md) for decimal arithmetic and legacy policy.

## Reliability and operational limits

The Go consumers for dispatch, driver locations, and notifications read new deliveries and periodically reclaim eligible pending entries with `XAUTOCLAIM`. Each instance has its own consumer name, and reclaim scanning retains its cursor. Successful processing is acknowledged only after the relevant state change has completed.

Reclaiming does not make delivery exactly-once. Dispatch uses guarded database transitions and unique reservations; location updates reject older timestamps. Notification logs and delivery counters can repeat after redelivery and are not presented as unique notifications.

Retryable failures remain pending. Malformed messages and exhausted delivery attempts are copied to the dead-letter stream before acknowledgment. If dead-letter publication fails, the original remains pending. Reclaim idle time must exceed the bounded processing budget; repeated delivery remains possible and is handled by the state guards.

PostgreSQL domain state and outgoing events commit together in a transactional outbox. Relays publish at least once; Redis Streams persistence, retention, and backups are separate operational responsibilities. There is no client-create request deduplication, authentication, payment collection, or production availability guarantee.

See [routing and migration](docs/routing.md), [reliability](docs/reliability.md), and [API](docs/api.md).

## Verification

Local package checks require Go 1.22; Maven checks require Java 21 and Docker.

Go tests cover HTTP and event contracts, route-response validation, driver selection, consumer recovery, and guarded state transitions. Integration tests use PostgreSQL and Redis to exercise concurrent reservations, completion/cancellation races, transaction rollback, and restart recovery. The fare flow follows a ride through the Go services and Java ledger and verifies publication of the resulting event. Test output is the source of truth for case counts.

The Go race detector instruments the Go test processes. It does not instrument service binaries running in separately built containers. Fixture-based routing tests do not measure real travel times or prove the availability of a public routing service.

```bash
go test -race ./...
go vet ./...
(cd services/fare-service && ./mvnw verify)
bash scripts/fare-e2e-test.sh
```

Real Redis and PostgreSQL checks in the routing and shared consumer packages run when `TEST_REDIS_ADDR` and `TEST_POSTGRES_DSN` are set. See [testing and CI](docs/testing-and-ci.md) for the isolated fixture stack, outage checks, and KinD validation.

## Delivery pipeline and optional Kafka

[CI](.github/workflows/ci.yml) validates Go formatting, vet/tests, Compose, smoke/integration, and outage recovery. A separate Java job runs Maven `verify`, and a third job runs the ride-to-fare-settlement flow against a Compose stack with the `fare` profile. Six core images are built as distroless, non-root containers.

- **Pull requests:** build images in the runner, load them into KinD, install the Helm release, and run deployment smoke checks without publishing service images.
- **Main, release tags, manual runs:** publish core images tagged with the full commit SHA to GHCR, pull those artifacts into the deployment-validation job, and run the same KinD checks. No `latest` tag is used.
- **Scope:** the publish job depends on Go backend validation; fare validation and the fare flow job are separate jobs and not dependencies of image publication. Fare and analytics are outside the six-image release/Helm smoke path: the flow job proves the Go-to-Java chain works in Compose, it does not put fare-service into GHCR, the Helm chart or KinD. KinD clusters are removed after validation.

The [Helm chart](infrastructure/helm/metro-ride) includes probes, resource settings, bounded dependency waits, and optional `ServiceMonitor` rendering. Default dependencies are external PostgreSQL/Redis; KinD profiles use disposable `emptyDir`-backed instances. Local deployment validation needs Docker, kind, kubectl, Helm, and Bash 4+; see [CI/CD documentation](docs/cicd.md).

The optional Kafka profile runs a non-persistent single KRaft broker, a three-partition driver-location topic keyed by `driver_id`, a separate telemetry producer, and the analytics consumer:

```bash
docker compose --profile kafka up --build -d
ENABLE_KAFKA_SMOKE=true bash scripts/smoke-test.sh
```

Kafka is a telemetry extension; Redis Streams remains the ride workflow transport.

## Repository guide

[Architecture](docs/architecture.md) · [API](docs/api.md) · [Reliability](docs/reliability.md) · [Fare service](services/fare-service/README.md) · [Observability](docs/observability.md) · [Testing](docs/testing-and-ci.md) · [Deployment](docs/cicd.md) · [Kafka extension](docs/kafka-lightweight-extension.md)

The root README describes the combined current workflow. Service-specific source, migrations, and tests provide the detailed behavioral contracts.

## License

MIT. See [LICENSE](LICENSE).
