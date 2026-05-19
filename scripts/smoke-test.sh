#!/usr/bin/env bash
set -euo pipefail

curl -fsS http://localhost:8080/healthz >/dev/null
curl -fsS http://localhost:8082/healthz >/dev/null
curl -fsS http://localhost:8083/healthz >/dev/null

response="$(curl -fsS -X POST http://localhost:8080/v1/rides \
  -H 'Content-Type: application/json' \
  -d '{"rider_id":"rider-42","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}')"

ride_id="$(printf '%s' "$response" | sed -n 's/.*"ride_id":"\([^"]*\)".*/\1/p')"
echo "created ride: ${ride_id}"

sleep 3
curl -fsS "http://localhost:8080/v1/rides/${ride_id}"
