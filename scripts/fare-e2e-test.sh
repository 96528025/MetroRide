#!/usr/bin/env bash
# Runs the ride-to-fare-settlement flow (tests/fareintegration) against a stack this
# script starts itself. It is the single entry point for CI and for local runs.
#
# What it does, in order:
#   1. Refuses to run if the ports the stack publishes are already in use, or if a Compose
#      project with this run's name already has containers or volumes. The project name is
#      unique per run by default, so a stack you keep running under the default project
#      name (`docker compose up`) is never touched, and neither are its data volumes.
#   2. Builds and starts only the services the flow needs, with the `fare` profile and the
#      test overlay tests/fareintegration/compose.fare-e2e.yml, which passes FARE_DRIVER_SHARE
#      to fare-service. The Go test reads the same variable.
#   3. Waits for readiness, runs the Go test, and on failure writes every service log to
#      FARE_E2E_LOG_DIR.
#   4. Removes this run's containers, volumes and built images, whether the test passed or
#      failed (FARE_E2E_KEEP=true skips that and prints the teardown command instead).
#
# Environment:
#   FARE_DRIVER_SHARE          driver share configured on fare-service and checked by the
#                              test (default 0.80, the value in application.yml)
#   FARE_E2E_PROJECT           Compose project name (default fare-e2e-<timestamp>-<pid>)
#   FARE_E2E_LOG_DIR           directory for service logs on failure (default: a temp dir)
#   FARE_E2E_READY_TIMEOUT_SECONDS  readiness wait per service (default 120)
#   FARE_E2E_KEEP              true = leave the stack running after the test
#   FARE_E2E_GO_IMAGE          image used to run `go test` when `go` is not on PATH
#                              (default golang:1.22)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

export FARE_DRIVER_SHARE="${FARE_DRIVER_SHARE:-0.80}"
PROJECT="${FARE_E2E_PROJECT:-fare-e2e-$(date +%Y%m%d%H%M%S)-$$}"
LOG_DIR="${FARE_E2E_LOG_DIR:-}"
READY_TIMEOUT="${FARE_E2E_READY_TIMEOUT_SECONDS:-120}"
KEEP="${FARE_E2E_KEEP:-false}"
GO_IMAGE="${FARE_E2E_GO_IMAGE:-golang:1.22}"
BASE_HOST="${BASE_HOST:-localhost}"

# The services the chain runs through, and the host ports docker-compose.yml publishes for
# them and their dependencies. Prometheus, Grafana, traffic and notification are not started.
SERVICES=(rider-service driver-service routing-service dispatch-service fare-service)
READY_PORTS=(8080 8081 8083 8082 8087)
ALL_PORTS=(5432 6379 8080 8081 8082 8083 8087)

COMPOSE=(docker compose -p "${PROJECT}"
  -f docker-compose.yml -f tests/fareintegration/compose.fare-e2e.yml
  --profile fare)

log() { printf '[fare-e2e] %s\n' "$*"; }
fail() { printf '[fare-e2e] failed: %s\n' "$*" >&2; exit 1; }

port_in_use() {
  # bash opens a TCP connection; success means something is listening.
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

dump_logs() {
  if [[ -z "${LOG_DIR}" ]]; then
    LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/fare-e2e-logs.XXXXXX")"
  fi
  mkdir -p "${LOG_DIR}"
  "${COMPOSE[@]}" ps -a >"${LOG_DIR}/compose-ps.txt" 2>&1 || true
  for service in postgres redis "${SERVICES[@]}"; do
    "${COMPOSE[@]}" logs --no-color --timestamps "${service}" >"${LOG_DIR}/${service}.log" 2>&1 || true
  done
  log "service logs written to ${LOG_DIR}"
}

teardown() {
  local status=$?
  trap - EXIT
  if (( status != 0 )); then
    dump_logs
  fi
  if [[ "${KEEP}" == "true" ]]; then
    log "FARE_E2E_KEEP=true: stack left running; remove it with:"
    log "  FARE_DRIVER_SHARE=${FARE_DRIVER_SHARE} docker compose -p ${PROJECT} -f docker-compose.yml -f tests/fareintegration/compose.fare-e2e.yml --profile fare down -v --remove-orphans --rmi local"
  else
    log "removing project ${PROJECT} (containers, volumes, built images)..."
    "${COMPOSE[@]}" down -v --remove-orphans --rmi local >/dev/null 2>&1 || true
  fi
  log "finished in ${SECONDS}s with status ${status}"
  exit "${status}"
}

# --- preflight ---------------------------------------------------------------------------
if [[ -n "$("${COMPOSE[@]}" ps -aq 2>/dev/null)" ]] \
  || [[ -n "$(docker volume ls -q --filter "label=com.docker.compose.project=${PROJECT}" 2>/dev/null)" ]]; then
  fail "Compose project ${PROJECT} already has containers or volumes; pick another FARE_E2E_PROJECT or remove it first"
fi
busy=()
for port in "${ALL_PORTS[@]}"; do
  if port_in_use "${port}"; then
    busy+=("${port}")
  fi
done
if (( ${#busy[@]} > 0 )); then
  fail "port(s) ${busy[*]} are already in use on this host; stop whatever listens there (for example a running MetroRide stack) and retry"
fi

trap teardown EXIT
log "project ${PROJECT}, driver share ${FARE_DRIVER_SHARE}"

# --- build and start -----------------------------------------------------------------
phase_start=${SECONDS}
"${COMPOSE[@]}" config --quiet
"${COMPOSE[@]}" build "${SERVICES[@]}"
log "build took $((SECONDS - phase_start))s"

phase_start=${SECONDS}
"${COMPOSE[@]}" up -d "${SERVICES[@]}"
for i in "${!SERVICES[@]}"; do
  service="${SERVICES[$i]}"
  url="http://${BASE_HOST}:${READY_PORTS[$i]}/readyz"
  deadline=$((SECONDS + READY_TIMEOUT))
  until curl -fsS --max-time 2 "${url}" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      fail "${service} did not become ready at ${url} within ${READY_TIMEOUT}s"
    fi
    sleep 1
  done
  log "ready: ${service}"
done
log "startup took $((SECONDS - phase_start))s"

# --- test ----------------------------------------------------------------------------
phase_start=${SECONDS}
GO_TEST=(go test -race -count=1 -timeout 8m -v -tags=fareintegration ./tests/fareintegration)
if command -v go >/dev/null 2>&1; then
  INTEGRATION_HOST="${BASE_HOST}" "${GO_TEST[@]}"
else
  # No Go toolchain on this machine: run the test from a Go container against the ports
  # published on the host. The two named volumes are module and build caches only.
  log "go not found on PATH; running the test in ${GO_IMAGE}"
  docker run --rm \
    -v "${REPO_ROOT}:/src" -w /src \
    -v fare-e2e-gomod:/go/pkg/mod -v fare-e2e-gocache:/root/.cache/go-build \
    --add-host=host.docker.internal:host-gateway \
    -e FARE_DRIVER_SHARE \
    -e INTEGRATION_HOST=host.docker.internal \
    -e INTEGRATION_POSTGRES_DSN='postgres://metroride:metroride@host.docker.internal:5432/metroride?sslmode=disable' \
    -e INTEGRATION_REDIS_ADDR=host.docker.internal:6379 \
    "${GO_IMAGE}" "${GO_TEST[@]}"
fi
log "test took $((SECONDS - phase_start))s"
