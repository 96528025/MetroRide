# Road Routing and Quote Inputs

MetroRide uses a configurable Valhalla endpoint for road-route estimates. `VALHALLA_BASE_URL` defaults to `https://valhalla1.openstreetmap.de` for low-volume manual demonstrations. This public endpoint is operated by FOSSGIS and is subject to its usage policy and availability. Set the variable to an instance you operate for sustained use.

Passenger quotes use a pickup-to-drop-off route. Driver selection uses a shortlist of up to five candidates, ordered initially by straight-line distance, then compares estimated road travel times to the pickup. Ties use driver ID. A selected driver's availability is rechecked when the reservation commits.

Requests use `costing: "auto"` and kilometer units. Route metadata records the provider and calculation time. Estimates depend on the provider's map and routing model; they are not observed trip times.

The shared route cache lasts one minute; duplicate requests coordinate through Redis. Request starts are spaced by at least one second per endpoint across replicas. Each provider call has a ten-second limit; the nearest-driver operation has a sixty-second budget. The public provider can return no route, reject requests, or be unavailable; no fixture or straight-line fallback is selected automatically. Routes are computed before the assignment transaction, so no driver reservation is held during the provider call.

`DRIVER_LOCATION_MAX_AGE_SECONDS` defaults to 15. An old location is excluded even if the driver has no active reservation. Simulator updates do not release reservations.

Automated suites and CI use a local HTTP route fixture through an explicit test configuration. The fixture checks request coordinates and units; the surrounding suites validate response schemas, timeout handling, and cross-service behavior; it is not evidence of live-provider availability. Manual route checks use the configured provider and record their own outcomes. Test configuration is never selected automatically after a live routing failure.

See [Valhalla's route documentation](https://valhalla.github.io/valhalla/api/route/overview/) and [public server information](https://github.com/valhalla/valhalla#demo-server).

## Event version and existing databases

The [assignment event contract](../shared/events/README.md#assignment-version-2-and-cancellation) defines version 2's separate approach and passenger fields. Fare-service owns input validation, quote freezing, and the [legacy-hold policy](../services/fare-service/README.md#quote-context-and-legacy-holds).

Before upgrading a database with existing rows, stop the Go services that write rides and assignments and make a backup. Apply the core migration using a PostgreSQL client:

```bash
POSTGRES_DSN='postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable' bash scripts/migrate-core.sh
```

The migration runs in one transaction. It preserves rides, assignments, and outbox rows, and restores reservations for active historical rides. Conflicting or missing active drivers cause the migration to fail; resolve those records explicitly before retrying. Fresh Compose databases use the same schema from `init.sql`. Fare-service separately applies Flyway migrations V5 (quote context) and V6 (cancellation state) to the `fare` schema.

## Explicit local test fixture

```bash
export COMPOSE_FILE=docker-compose.yml:tests/routingfixture/compose.yml
docker compose --profile fare up --build -d
bash scripts/smoke-test.sh
docker compose --profile fare down
```

`tests/routingfixture/main.go` returns deterministic contract data. Its standard passenger route is 8.5 km and 920.5 seconds; driver approach fixtures return 1.25 km and 180 seconds. These figures are test inputs, not measured road travel. KinD validation enables the same fixture through `routingFixture.enabled`; the ordinary Helm and Compose defaults use the configured Valhalla endpoint.
