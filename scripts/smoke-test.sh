#!/usr/bin/env bash
set -euo pipefail

BASE_HOST="${BASE_HOST:-localhost}"
TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-60}"

service_url() {
  local port="$1"
  printf 'http://%s:%s' "${BASE_HOST}" "${port}"
}

wait_for_endpoint() {
  local name="$1"
  local url="$2"
  local deadline=$((SECONDS + TIMEOUT_SECONDS))

  until curl -fsS "${url}" >/dev/null; do
    if (( SECONDS >= deadline )); then
      echo "failed: ${name} did not become ready at ${url}" >&2
      return 1
    fi
    sleep 2
  done
}

check_endpoint() {
  local name="$1"
  local url="$2"
  curl -fsS "${url}" >/dev/null
  echo "ok: ${name}"
}

wait_for_kafka_event() {
  local analytics_url
  local deadline
  local response
  local consumed

  analytics_url="$(service_url 8086)/v1/analytics/drivers"
  deadline=$((SECONDS + TIMEOUT_SECONDS))
  until false; do
    response="$(curl -fsS "${analytics_url}")"
    consumed="$(printf '%s' "${response}" | sed -n 's/.*"total_consumed":\([0-9][0-9]*\).*/\1/p')"
    if [[ -n "${consumed}" && "${consumed}" -gt 0 && "${response}" == *'"drivers":['* ]]; then
      echo "ok: analytics-service consumed ${consumed} Kafka driver location event(s)"
      echo "${response}"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "failed: analytics-service did not consume Kafka driver locations within timeout: ${response}" >&2
      return 1
    fi
    sleep 2
  done
}

extract_json_string() {
  local key="$1"
  sed -n "s/.*\"${key}\":\"\\([^\"]*\\)\".*/\\1/p"
}

services=(
  "rider-service:8080"
  "driver-service:8081"
  "dispatch-service:8082"
  "routing-service:8083"
  "traffic-service:8084"
  "notification-service:8085"
)

echo "waiting for MetroRide services..."
for entry in "${services[@]}"; do
  name="${entry%%:*}"
  port="${entry#*:}"
  base="$(service_url "${port}")"
  wait_for_endpoint "${name} /healthz" "${base}/healthz"
  wait_for_endpoint "${name} /readyz" "${base}/readyz"
done

echo "checking health and readiness..."
for entry in "${services[@]}"; do
  name="${entry%%:*}"
  port="${entry#*:}"
  base="$(service_url "${port}")"
  check_endpoint "${name} /healthz" "${base}/healthz"
  check_endpoint "${name} /readyz" "${base}/readyz"
done

echo "checking metrics endpoints..."
check_endpoint "rider-service /metrics" "$(service_url 8080)/metrics"
curl -fsS "$(service_url 8082)/metrics" | grep -q "metroride_dispatch_latency_seconds"
echo "ok: dispatch-service exposes dispatch latency metric"
curl -fsS "$(service_url 8083)/metrics" | grep -q "metroride_routing_computation_seconds"
echo "ok: routing-service exposes routing computation metric"

echo "creating ride request..."
response="$(curl -fsS -X POST "$(service_url 8080)/v1/rides" \
  -H 'Content-Type: application/json' \
  -d '{"rider_id":"smoke-rider","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}')"

ride_id="$(printf '%s' "${response}" | extract_json_string "ride_id")"
if [[ -z "${ride_id}" ]]; then
  echo "failed: could not parse ride_id from response: ${response}" >&2
  exit 1
fi

echo "created ride: ${ride_id}"

status=""
ride=""
deadline=$((SECONDS + TIMEOUT_SECONDS))
until [[ "${status}" == "assigned" ]]; do
  ride="$(curl -fsS "$(service_url 8080)/v1/rides/${ride_id}")"
  status="$(printf '%s' "${ride}" | extract_json_string "status")"
  if (( SECONDS >= deadline )); then
    echo "failed: ride was not assigned within timeout: ${ride}" >&2
    exit 1
  fi
  sleep 1
done

driver_id="$(printf '%s' "${ride}" | extract_json_string "driver_id")"
if [[ -z "${driver_id}" ]]; then
  echo "failed: assigned ride missing driver_id: ${ride}" >&2
  exit 1
fi

echo "ok: ride assigned to ${driver_id}"
echo "${ride}"

kafka_smoke="${ENABLE_KAFKA_SMOKE:-auto}"
if [[ "${kafka_smoke}" == "true" ]] || {
  [[ "${kafka_smoke}" == "auto" ]] && curl -fsS --max-time 1 "$(service_url 8086)/healthz" >/dev/null 2>&1
}; then
  echo "checking optional Kafka analytics extension..."
  wait_for_endpoint "analytics-service /healthz" "$(service_url 8086)/healthz"
  wait_for_endpoint "analytics-service /readyz" "$(service_url 8086)/readyz"
  check_endpoint "analytics-service /metrics" "$(service_url 8086)/metrics"
  curl -fsS "$(service_url 8086)/metrics" | grep -q "metroride_kafka_driver_location_events_total"
  echo "ok: analytics-service exposes Kafka metrics"
  wait_for_kafka_event
else
  echo "skipping optional Kafka analytics checks"
fi
