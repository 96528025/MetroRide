#!/usr/bin/env bash
# Proves that a ride request committed to PostgreSQL survives a SIGKILL of the
# process that owns its outbox relay (rider-service), and is published and
# assigned after restart without a client retry.
#
# The kill window is made deterministic instead of racing the relay: Redis is
# stopped first so publication cannot happen, the ride is accepted and its
# outbox row is verified unpublished, and only then is rider-service killed.
#
# Redis is stopped, never removed: the dispatch consumer group lives in Redis
# and is only recreated at dispatch-service startup, so wiping Redis state
# would leave the consumer loop failing with NOGROUP. dispatch-service stays
# running throughout because it exits at startup when Redis is unreachable.
# This test covers one relay crash window only: termination after the
# PostgreSQL commit and before any publication. Termination after Redis has
# accepted the event but before the transaction recording published_at
# commits (the window that produces the documented at-least-once duplicate)
# is not exercised, and neither is a consumer killed mid-message (pending
# entries are never reclaimed).
set -euo pipefail

BASE_HOST="${BASE_HOST:-localhost}"
# The relay reschedules failed rows with a backoff capped at 30 s, and the row
# fails a few times while Redis is stopped, so publication after restart can
# legitimately lag by up to ~30 s plus the 250 ms poll. The deadline starts
# before the restart, so the container start-up counts against the 90 s too.
TIMEOUT_SECONDS="${PROCESS_KILL_RECOVERY_TIMEOUT_SECONDS:-90}"
RIDE_REQUEST='{"rider_id":"process-kill-recovery","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}'
response_file=""

extract_json_string() {
  local key="$1"
  sed -n "s/.*\"${key}\":\"\\([^\"]*\\)\".*/\\1/p"
}

psql_scalar() {
  # Bounded like every other blocking call: connect within 2 s and abort the
  # statement after 2 s instead of hanging past the recovery deadline.
  docker compose exec -T -e PGCONNECT_TIMEOUT=2 -e PGOPTIONS='-c statement_timeout=2000' \
    postgres psql -U metroride -d metroride -Atqc "$1"
}

# Seconds left before ${deadline}, never below 1, so every blocking call in the
# recovery phase is capped by the time that is actually left.
remaining_seconds() {
  local remaining=$((deadline - SECONDS))
  if (( remaining < 1 )); then
    remaining=1
  fi
  printf '%s' "${remaining}"
}

restore_services() {
  local test_status=$?
  trap - EXIT
  if [[ -n "${response_file}" ]]; then
    rm -f "${response_file}"
  fi
  # Both names are explicit: rider-service's depends_on lists only postgres, so
  # `up -d rider-service` alone would leave Redis stopped.
  docker compose up -d redis rider-service >/dev/null || true
  exit "${test_status}"
}

trap restore_services EXIT

# routing-service must be up as well: if it is down when dispatch consumes the
# request, dispatch dead-letters it after 3 retries and the ride never leaves
# `requested`, which would look like a relay failure.
running_services="$(docker compose ps --status running --services)"
for service in redis rider-service dispatch-service routing-service; do
  if ! grep -qx "${service}" <<<"${running_services}"; then
    echo "failed: ${service} must be running before the process-kill recovery test" >&2
    exit 1
  fi
done

echo "stopping Redis so the outbox relay cannot publish..."
docker compose stop -t 5 redis >/dev/null

response_file="$(mktemp)"
status_code="$(curl -sS -o "${response_file}" -w '%{http_code}' -X POST "http://${BASE_HOST}:8080/v1/rides" \
  -H 'Content-Type: application/json' \
  -d "${RIDE_REQUEST}")"
response="$(cat "${response_file}")"
rm -f "${response_file}"
response_file=""

if [[ "${status_code}" != "202" ]]; then
  echo "failed: rider-service did not accept the durable request while Redis was stopped: HTTP ${status_code}: ${response}" >&2
  exit 1
fi

ride_id="$(printf '%s' "${response}" | extract_json_string ride_id)"
event_id="$(printf '%s' "${response}" | extract_json_string event_id)"
if [[ -z "${ride_id}" || -z "${event_id}" ]]; then
  echo "failed: response missing ride_id or event_id: ${response}" >&2
  exit 1
fi

pending="$(psql_scalar \
  "select count(*) from event_outbox where id = '${event_id}' and stream = 'events.ride.requests' and published_at is null")"
if [[ "${pending}" != "1" ]]; then
  echo "failed: expected one durable unpublished outbox event, got ${pending}" >&2
  exit 1
fi
echo "ok: ride ${ride_id} and event ${event_id} committed while publication was blocked"

echo "killing rider-service with SIGKILL while its outbox row is unpublished..."
docker compose kill -s SIGKILL rider-service >/dev/null

deadline=$((SECONDS + 10))
while docker compose ps --status running --services | grep -qx 'rider-service'; do
  if (( SECONDS >= deadline )); then
    echo "failed: rider-service is still running after SIGKILL" >&2
    exit 1
  fi
  sleep 1
done
echo "ok: rider-service exited without a graceful shutdown"

echo "restarting Redis and rider-service and waiting for automatic relay recovery..."
deadline=$((SECONDS + TIMEOUT_SECONDS))
docker compose up -d redis rider-service >/dev/null

# Each loop checks the deadline before it blocks and caps the call by the time
# left, so a hung request can neither stall the run nor be accepted late.
while true; do
  if (( SECONDS >= deadline )); then
    echo "failed: rider-service did not report ready within ${TIMEOUT_SECONDS}s after restart" >&2
    exit 1
  fi
  readiness_status="$(curl -fsS --max-time "$(remaining_seconds)" "http://${BASE_HOST}:8080/readyz" 2>/dev/null | extract_json_string status || true)"
  if [[ "${readiness_status}" == "ready" ]]; then
    break
  fi
  sleep 1
done
echo "ok: rider-service is ready again"

ride=""
unpublished=""
while true; do
  if (( SECONDS >= deadline )); then
    echo "failed: restarted relay did not recover within ${TIMEOUT_SECONDS}s; ride=${ride}; unpublished=${unpublished}" >&2
    exit 1
  fi
  ride="$(curl -fsS --max-time "$(remaining_seconds)" "http://${BASE_HOST}:8080/v1/rides/${ride_id}" || true)"
  ride_status="$(printf '%s' "${ride}" | extract_json_string status)"
  unpublished="$(psql_scalar \
    "select count(*) from event_outbox where aggregate_id = '${ride_id}' and published_at is null" || true)"
  if [[ "${ride_status}" == "assigned" && "${unpublished}" == "0" ]]; then
    break
  fi
  sleep 1
done
echo "ok: restarted relay published the committed event and dispatch assigned the ride without client retry"

# The event must appear on the stream exactly once: the kill happened before
# any publication, so there is no window for the documented at-least-once
# duplicate (a crash between XADD and recording published_at).
published_entries="$(docker compose exec -T redis redis-cli XRANGE events.ride.requests - + | grep -c "${event_id}" || true)"
if [[ "${published_entries}" != "1" ]]; then
  echo "failed: expected event ${event_id} exactly once on events.ride.requests, found ${published_entries:-none}" >&2
  exit 1
fi
echo "ok: event ${event_id} was published exactly once"

trap - EXIT
echo "ok: process kill after commit lost nothing; the restarted relay delivered the event and the ride was assigned"
