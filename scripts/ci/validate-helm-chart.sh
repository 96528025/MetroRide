#!/usr/bin/env bash
# Lint and render the Helm chart in every configuration it is actually
# installed in: the published-image defaults, and both ephemeral KinD profiles.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

CHART="infrastructure/helm/metro-ride"
INIT_SQL="--set-file=dependencies.postgres.initSql=infrastructure/docker/postgres/init.sql"
TAG="${IMAGE_TAG:-0000000000000000000000000000000000000000}"

echo "==> helm lint (chart defaults)"
helm lint "${CHART}"

for profile in pr ghcr; do
  echo "==> helm lint (ephemeral KinD profile: ${profile})"
  helm lint "${CHART}" \
    -f "${CHART}/values-kind.yaml" \
    -f "${CHART}/values-kind-${profile}.yaml" \
    "${INIT_SQL}"

  echo "==> helm template (ephemeral KinD profile: ${profile})"
  helm template metro-ride "${CHART}" \
    -f "${CHART}/values-kind.yaml" \
    -f "${CHART}/values-kind-${profile}.yaml" \
    "${INIT_SQL}" \
    --set "image.tag=${TAG}" >/dev/null
done

echo "==> helm template (chart defaults)"
helm template metro-ride "${CHART}" --set "image.tag=${TAG}" >/dev/null

echo "ok: helm lint and helm template pass for every installed configuration"
