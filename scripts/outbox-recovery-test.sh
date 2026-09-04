#!/usr/bin/env bash
set -euo pipefail

BASE_HOST="${BASE_HOST:-localhost}"
TIMEOUT_SECONDS="${OUTBOX_RECOVERY_TIMEOUT_SECONDS:-45}"
RIDE_REQUEST='{"rider_id":"outbox-recovery","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}'
response_file=""

extract_json_string() {
  local key="$1"
  sed -n "s/.*\"${key}\":\"\\([^\"]*\\)\".*/\\1/p"
}

restore_redis() {
  local test_status=$?
  trap - EXIT
  if [[ -n "${response_file}" ]]; then
    rm -f "${response_file}"
  fi
  docker compose up -d redis >/dev/null || true
  exit "${test_status}"
}

trap restore_redis EXIT

if ! docker compose ps --status running --services | grep -qx 'redis'; then
  echo "failed: redis must be running before the outbox recovery test" >&2
  exit 1
fi

echo "stopping Redis to open the database/event-bus failure window..."
docker compose stop -t 5 redis >/dev/null

if ! readiness_response="$(curl -fsS --max-time 3 "http://${BASE_HOST}:8080/readyz")"; then
  echo "failed: rider-service left the ready pool even though PostgreSQL can still accept durable requests" >&2
  exit 1
fi
readiness_status="$(printf '%s' "${readiness_response}" | extract_json_string status)"
if [[ "${readiness_status}" != "ready" ]]; then
  echo "failed: rider-service reported unexpected readiness during Redis outage: ${readiness_response}" >&2
  exit 1
fi
echo "ok: rider-service remains ready while Redis delivery is degraded"

response_file="$(mktemp)"
status_code="$(curl -sS -o "${response_file}" -w '%{http_code}' -X POST "http://${BASE_HOST}:8080/v1/rides" \
  -H 'Content-Type: application/json' \
  -d "${RIDE_REQUEST}")"
response="$(cat "${response_file}")"
rm -f "${response_file}"
response_file=""

if [[ "${status_code}" != "202" ]]; then
  echo "failed: rider-service did not accept the durable request during Redis outage: HTTP ${status_code}: ${response}" >&2
  exit 1
fi

ride_id="$(printf '%s' "${response}" | extract_json_string ride_id)"
event_id="$(printf '%s' "${response}" | extract_json_string event_id)"
if [[ -z "${ride_id}" || -z "${event_id}" ]]; then
  echo "failed: response missing ride_id or event_id: ${response}" >&2
  exit 1
fi

pending="$(docker compose exec -T postgres psql -U metroride -d metroride -Atqc \
  "select count(*) from event_outbox where id = '${event_id}' and stream = 'events.ride.requests' and published_at is null")"
if [[ "${pending}" != "1" ]]; then
  echo "failed: expected one durable unpublished outbox event, got ${pending}" >&2
  exit 1
fi
echo "ok: ride ${ride_id} and event ${event_id} committed atomically while Redis was unavailable"

echo "restarting Redis and waiting for automatic relay recovery..."
docker compose up -d redis >/dev/null

deadline=$((SECONDS + TIMEOUT_SECONDS))
ride=""
while true; do
  ride="$(curl -fsS "http://${BASE_HOST}:8080/v1/rides/${ride_id}")"
  ride_status="$(printf '%s' "${ride}" | extract_json_string status)"
  unpublished="$(docker compose exec -T postgres psql -U metroride -d metroride -Atqc \
    "select count(*) from event_outbox where aggregate_id = '${ride_id}' and published_at is null")"
  if [[ "${ride_status}" == "assigned" && "${unpublished}" == "0" ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "failed: outbox relay did not recover within timeout; ride=${ride}; unpublished=${unpublished}" >&2
    exit 1
  fi
  sleep 1
done

trap - EXIT
echo "ok: Redis recovery relayed every event and assigned the ride without client retry"
