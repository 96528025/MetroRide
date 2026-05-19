#!/usr/bin/env bash
set -euo pipefail

services=(
  "rider-service:http://localhost:8080"
  "driver-service:http://localhost:8081"
  "dispatch-service:http://localhost:8082"
  "routing-service:http://localhost:8083"
  "traffic-service:http://localhost:8084"
  "notification-service:http://localhost:8085"
)

for entry in "${services[@]}"; do
  name="${entry%%:*}"
  base="${entry#*:}"
  curl -fsS "${base}/healthz" >/dev/null
  curl -fsS "${base}/readyz" >/dev/null
  echo "ok: ${name} health/readiness"
done

curl -fsS http://localhost:8082/metrics | grep -q "metroride_dispatch_latency_seconds"
echo "ok: dispatch-service metrics"

response="$(curl -fsS -X POST http://localhost:8080/v1/rides \
  -H 'Content-Type: application/json' \
  -d '{"rider_id":"rider-42","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}')"

ride_id="$(printf '%s' "$response" | sed -n 's/.*"ride_id":"\([^"]*\)".*/\1/p')"
if [[ -z "${ride_id}" ]]; then
  echo "failed to parse ride_id from response: ${response}" >&2
  exit 1
fi

echo "created ride: ${ride_id}"

status=""
ride=""
for _ in {1..15}; do
  ride="$(curl -fsS "http://localhost:8080/v1/rides/${ride_id}")"
  status="$(printf '%s' "$ride" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')"
  if [[ "${status}" == "assigned" ]]; then
    break
  fi
  sleep 1
done

if [[ "${status}" != "assigned" ]]; then
  echo "ride was not assigned within timeout: ${ride}" >&2
  exit 1
fi

driver_id="$(printf '%s' "$ride" | sed -n 's/.*"driver_id":"\([^"]*\)".*/\1/p')"
if [[ -z "${driver_id}" ]]; then
  echo "assigned ride missing driver_id: ${ride}" >&2
  exit 1
fi

echo "ok: ride assigned to ${driver_id}"
echo "${ride}"
