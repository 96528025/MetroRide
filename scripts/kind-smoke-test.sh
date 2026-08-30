#!/usr/bin/env bash
# Post-deployment validation against the release running in the ephemeral KinD
# cluster. Identical for the pull-request and trusted-delivery paths.
#
# Contract
# --------
# 1. Every required Deployment reports Available (checked in kind-deploy.sh and
#    re-asserted here before any traffic is sent).
# 2. GET /healthz and GET /readyz return 2xx for all six core services.
# 3. POST http://rider-service:8080/v1/rides with
#      {"rider_id":"smoke-rider","pickup_lat":37.775,"pickup_lng":-122.419,
#       "dropoff_lat":37.789,"dropoff_lng":-122.401}
#    returns HTTP 202 and a ride_id.
# 4. GET /v1/rides/<ride_id> reaches HTTP 200 with status "assigned" and a
#    non-empty driver_id, which only happens if rider-service persisted the
#    ride, published ride_requested to Redis Streams, dispatch-service consumed
#    it through its consumer group, called routing-service, and committed the
#    assignment to PostgreSQL.
# 5. The same final state is confirmed independently in PostgreSQL: the rides
#    row is 'assigned' with a driver, and exactly one ride_assignments row
#    exists for that ride.
# 6. notification-service reports at least one processed assignment event via
#    its existing GET /v1/notifications/stats endpoint.
#
# Steps 2-4 are the existing scripts/smoke-test.sh, reached over kubectl
# port-forward, so the Compose stack and the Kubernetes release are validated by
# exactly the same assertions. Every wait is bounded polling with a deadline;
# there are no fixed sleeps standing in for synchronisation.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"
# shellcheck source=scripts/lib/kind.sh
source "${REPO_ROOT}/scripts/lib/kind.sh"

READY_TIMEOUT_SECONDS="${KIND_SMOKE_READY_TIMEOUT_SECONDS:-90}"
STATE_TIMEOUT_SECONDS="${KIND_SMOKE_STATE_TIMEOUT_SECONDS:-90}"

SERVICE_PORTS=(
  "rider-service:8080"
  "driver-service:8081"
  "dispatch-service:8082"
  "routing-service:8083"
  "traffic-service:8084"
  "notification-service:8085"
)

declare -a PORT_FORWARD_PIDS=()
WORK_DIR="$(mktemp -d)"

cleanup() {
  local status=$?
  for pid in "${PORT_FORWARD_PIDS[@]+"${PORT_FORWARD_PIDS[@]}"}"; do
    kill "${pid}" 2>/dev/null || true
  done
  if (( status != 0 )); then
    echo "--- port-forward logs ---" >&2
    for log in "${WORK_DIR}"/port-forward-*.log; do
      [[ -e "${log}" ]] || continue
      echo "== ${log}" >&2
      cat "${log}" >&2
    done
  fi
  rm -rf "${WORK_DIR}"
  exit "${status}"
}
trap cleanup EXIT

kubectl_ns() {
  kubectl --namespace "${HELM_NAMESPACE}" "$@"
}

echo "== asserting required deployments are available"
for deployment in "${METRORIDE_REQUIRED_DEPLOYMENTS[@]}"; do
  kubectl_ns wait --for=condition=Available "deployment/${deployment}" --timeout=120s
done

echo "== opening port-forwards"
for entry in "${SERVICE_PORTS[@]}"; do
  name="${entry%%:*}"
  port="${entry##*:}"
  kubectl_ns port-forward --address 127.0.0.1 "service/${name}" "${port}:${port}" \
    >"${WORK_DIR}/port-forward-${name}.log" 2>&1 &
  PORT_FORWARD_PIDS+=($!)
done

for entry in "${SERVICE_PORTS[@]}"; do
  name="${entry%%:*}"
  port="${entry##*:}"
  deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
  until curl -fsS --max-time 3 "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "failed: port-forward to ${name} did not serve /healthz within ${READY_TIMEOUT_SECONDS}s" >&2
      exit 1
    fi
    sleep 1
  done
  echo "ok: ${name} reachable on 127.0.0.1:${port}"
done

echo "== running the shared smoke test against the Kubernetes release"
RIDE_ID_FILE="${WORK_DIR}/ride-id"
BASE_HOST=127.0.0.1 \
  ENABLE_KAFKA_SMOKE=false \
  SMOKE_RIDE_ID_FILE="${RIDE_ID_FILE}" \
  bash scripts/smoke-test.sh

RIDE_ID="$(cat "${RIDE_ID_FILE}")"
if ! [[ "${RIDE_ID}" =~ ^[0-9a-fA-F-]{36}$ ]]; then
  echo "failed: smoke test produced an unexpected ride id: '${RIDE_ID}'" >&2
  exit 1
fi

echo "== confirming final ride state directly in PostgreSQL (ride ${RIDE_ID})"
# Queries run over the container's local unix socket, which the official
# PostgreSQL image trusts, so no password has to be passed around. The ride id
# is a server-generated UUID and is shape-checked above before interpolation.
psql_query() {
  kubectl_ns exec deploy/postgres -- \
    psql -U metroride -d metroride -tAc "$1"
}

ride_row="$(psql_query "select status || '|' || coalesce(driver_id, '') from rides where id = '${RIDE_ID}'")"
ride_row="${ride_row//[$'\r\n ']/}"
ride_status="${ride_row%%|*}"
ride_driver="${ride_row##*|}"
if [[ "${ride_status}" != "assigned" ]]; then
  echo "failed: PostgreSQL reports ride ${RIDE_ID} as '${ride_status}', expected 'assigned'" >&2
  exit 1
fi
if [[ -z "${ride_driver}" ]]; then
  echo "failed: PostgreSQL ride ${RIDE_ID} is assigned but has no driver_id" >&2
  exit 1
fi
echo "ok: rides row is assigned to ${ride_driver}"

assignment_count="$(psql_query "select count(*) from ride_assignments where ride_id = '${RIDE_ID}'")"
assignment_count="${assignment_count//[$'\r\n ']/}"
if [[ "${assignment_count}" != "1" ]]; then
  echo "failed: expected exactly 1 ride_assignments row for ${RIDE_ID}, found '${assignment_count}'" >&2
  exit 1
fi
echo "ok: exactly one ride_assignments row exists"

echo "== confirming notification-service consumed the assignment event"
deadline=$((SECONDS + STATE_TIMEOUT_SECONDS))
while true; do
  stats="$(curl -fsS --max-time 3 "http://127.0.0.1:8085/v1/notifications/stats" || true)"
  processed="$(printf '%s' "${stats}" | sed -n 's/.*"processed":\([0-9][0-9]*\).*/\1/p')"
  if [[ -n "${processed}" && "${processed}" -gt 0 ]]; then
    echo "ok: notification-service processed ${processed} assignment event(s)"
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "failed: notification-service reported no processed events within ${STATE_TIMEOUT_SECONDS}s: ${stats}" >&2
    exit 1
  fi
  sleep 2
done

echo "ok: post-deployment smoke test passed against the KinD release"
