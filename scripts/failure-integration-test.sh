#!/usr/bin/env bash
set -euo pipefail

BASE_HOST="${BASE_HOST:-localhost}"
OUTAGE_TIMEOUT_SECONDS="${FAILURE_OUTAGE_TIMEOUT_SECONDS:-15}"
export INTEGRATION_HOST="${INTEGRATION_HOST:-${BASE_HOST}}"

restore_routing() {
  local test_status=$?
  local deadline

  trap - EXIT
  if ! docker compose up -d routing-service >/dev/null; then
    echo "failed: could not restore routing-service" >&2
    exit 1
  fi

  deadline=$((SECONDS + OUTAGE_TIMEOUT_SECONDS))
  until curl -fsS --max-time 1 "http://${BASE_HOST}:8083/readyz" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "failed: routing-service did not become ready after restoration" >&2
      exit 1
    fi
    sleep 1
  done

  exit "${test_status}"
}

if ! docker compose ps --status running --services | grep -qx 'routing-service'; then
  echo "failed: routing-service must be running before the failure integration test" >&2
  exit 1
fi

trap restore_routing EXIT

echo "stopping routing-service..."
docker compose stop -t 5 routing-service

deadline=$((SECONDS + OUTAGE_TIMEOUT_SECONDS))
while curl -fsS --max-time 1 "http://${BASE_HOST}:8083/healthz" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    echo "failed: routing-service remained reachable after stop" >&2
    exit 1
  fi
  sleep 1
done

echo "routing-service is unavailable; running dead-letter failure integration test..."
go test -count=1 -timeout 45s -tags=failureintegration ./tests/failureintegration
