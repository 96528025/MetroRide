#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
: "${POSTGRES_DSN:?Set POSTGRES_DSN for the database to migrate}"
psql "${POSTGRES_DSN}" -v ON_ERROR_STOP=1 --single-transaction -f infrastructure/docker/postgres/migrations/001__driver_state_and_trip_routes.sql
