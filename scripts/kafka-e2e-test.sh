#!/usr/bin/env bash
# Runs the Kafka telemetry flow (tests/kafkaintegration) against a stack this script
# starts itself. It is the single entry point for CI and for local runs.
#
# What it does, in order:
#   1. Refuses to run if a Compose project with this run's name already has containers or
#      volumes. The project name is unique per run by default, so a stack you keep running
#      under another project name is never touched.
#   2. Builds and starts only the `kafka` profile's path: Kafka, the one-shot topic job, the
#      driver telemetry producer and analytics-service (plus Redis, which the producer's
#      readiness check needs). The core ride services are not started. The test overlay
#      tests/kafkaintegration/compose.kafka-e2e.yml drops every host port mapping, so the
#      run can share a machine with a stack that already uses 6379, 8086 or 9092.
#   3. Builds a small runner image (Go toolchain + Docker CLI) and runs the Go test in it on
#      the project's Compose network, because the broker advertises only kafka:9092. The
#      test waits for service readiness itself. The Docker socket is mounted so the test can
#      stop and start the producer and analytics-service. It selects containers by this
#      run's project label, which prevents mistakes but is not a security boundary: the
#      socket gives the runner container full control of the Docker daemon.
#   4. On failure writes every service log to KAFKA_E2E_LOG_DIR, then removes this run's
#      containers, volumes and built images whether the test passed or failed
#      (KAFKA_E2E_KEEP=true skips that and prints the teardown command instead).
#
# Requires Docker Compose v2.24 or newer for the overlay's `!reset` tag.
#
# Environment:
#   KAFKA_E2E_PROJECT  Compose project name (default kafka-e2e-<timestamp>-<pid>)
#   KAFKA_E2E_LOG_DIR  directory for service logs on failure (default: a temp dir)
#   KAFKA_E2E_KEEP     true = leave the stack running after the test
#   DOCKER_SOCKET      host Docker socket to mount (default /var/run/docker.sock)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

PROJECT="${KAFKA_E2E_PROJECT:-kafka-e2e-$(date +%Y%m%d%H%M%S)-$$}"
LOG_DIR="${KAFKA_E2E_LOG_DIR:-}"
KEEP="${KAFKA_E2E_KEEP:-false}"
DOCKER_SOCKET="${DOCKER_SOCKET:-/var/run/docker.sock}"
RUNNER_IMAGE="${PROJECT}-test-runner"

SERVICES=(driver-kafka-producer analytics-service)
LOGGED=(kafka kafka-init redis driver-kafka-producer analytics-service)

COMPOSE=(docker compose -p "${PROJECT}"
  -f docker-compose.yml -f tests/kafkaintegration/compose.kafka-e2e.yml
  --profile kafka)

log() { printf '[kafka-e2e] %s\n' "$*"; }
fail() { printf '[kafka-e2e] failed: %s\n' "$*" >&2; exit 1; }

dump_logs() {
  if [[ -z "${LOG_DIR}" ]]; then
    LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kafka-e2e-logs.XXXXXX")"
  fi
  mkdir -p "${LOG_DIR}"
  "${COMPOSE[@]}" ps -a >"${LOG_DIR}/compose-ps.txt" 2>&1 || true
  for service in "${LOGGED[@]}"; do
    "${COMPOSE[@]}" logs --no-color --timestamps "${service}" >"${LOG_DIR}/${service}.log" 2>&1 || true
  done
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group metroride-analytics-service \
    >"${LOG_DIR}/consumer-group.txt" 2>&1 || true
  log "service logs written to ${LOG_DIR}"
}

teardown() {
  local status=$?
  trap - EXIT
  if (( status != 0 )); then
    dump_logs
  fi
  if [[ "${KEEP}" == "true" ]]; then
    log "KAFKA_E2E_KEEP=true: stack left running; remove it with:"
    log "  docker compose -p ${PROJECT} -f docker-compose.yml -f tests/kafkaintegration/compose.kafka-e2e.yml --profile kafka down -v --remove-orphans --rmi local && docker image rm ${RUNNER_IMAGE}"
  else
    log "removing project ${PROJECT} (containers, volumes, built images)..."
    "${COMPOSE[@]}" down -v --remove-orphans --rmi local >/dev/null 2>&1 || true
    docker image rm "${RUNNER_IMAGE}" >/dev/null 2>&1 || true
  fi
  log "finished in ${SECONDS}s with status ${status}"
  exit "${status}"
}

# --- preflight ---------------------------------------------------------------------------
if [[ ! -S "${DOCKER_SOCKET}" ]]; then
  fail "Docker socket ${DOCKER_SOCKET} not found; set DOCKER_SOCKET to the host daemon's socket"
fi
if [[ -n "$("${COMPOSE[@]}" ps -aq 2>/dev/null)" ]] \
  || [[ -n "$(docker volume ls -q --filter "label=com.docker.compose.project=${PROJECT}" 2>/dev/null)" ]]; then
  fail "Compose project ${PROJECT} already has containers or volumes; pick another KAFKA_E2E_PROJECT or remove it first"
fi

trap teardown EXIT
log "project ${PROJECT}"

# --- build and start -----------------------------------------------------------------
phase_start=${SECONDS}
"${COMPOSE[@]}" config --quiet
"${COMPOSE[@]}" build "${SERVICES[@]}"
docker build -q -t "${RUNNER_IMAGE}" -f tests/kafkaintegration/Dockerfile.runner tests/kafkaintegration >/dev/null
log "build took $((SECONDS - phase_start))s"

phase_start=${SECONDS}
# depends_on brings up Redis and Kafka (both healthy) and the topic job (completed) first.
"${COMPOSE[@]}" up -d "${SERVICES[@]}"
log "startup took $((SECONDS - phase_start))s"

# --- test ----------------------------------------------------------------------------
phase_start=${SECONDS}
# The module and build caches are named volumes so repeat local runs are fast.
docker run --rm \
  --network "${PROJECT}_default" \
  -v "${DOCKER_SOCKET}:/var/run/docker.sock" \
  -v "${REPO_ROOT}:/src" -w /src \
  -v kafka-e2e-gomod:/go/pkg/mod -v kafka-e2e-gocache:/root/.cache/go-build \
  -e KAFKA_E2E_PROJECT="${PROJECT}" \
  "${RUNNER_IMAGE}" \
  go test -race -count=1 -timeout 6m -v -tags=kafkaintegration ./tests/kafkaintegration
log "test took $((SECONDS - phase_start))s"
