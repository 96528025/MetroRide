#!/usr/bin/env bash
# Shared settings for the ephemeral KinD deployment-validation environment.

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-metroride}"
# Pinned so cluster creation is reproducible and never silently changes
# Kubernetes version between runs.
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"
KIND_CONFIG="${KIND_CONFIG:-infrastructure/kind/cluster.yaml}"

HELM_RELEASE="${HELM_RELEASE:-metro-ride}"
HELM_NAMESPACE="${HELM_NAMESPACE:-metroride}"
HELM_CHART="${HELM_CHART:-infrastructure/helm/metro-ride}"

# Image acquisition profile: pr (built locally) or ghcr (pulled from GHCR).
IMAGE_SOURCE="${IMAGE_SOURCE:-pr}"

# The values files that define the ephemeral profile. Both deployment paths
# share values-kind.yaml; only the image-acquisition profile differs.
# Callers must invoke this before reading value args through a process
# substitution, where a non-zero return would otherwise be swallowed.
metroride_require_valid_image_source() {
  case "${IMAGE_SOURCE}" in
    pr | ghcr) return 0 ;;
    *)
      echo "failed: IMAGE_SOURCE must be 'pr' or 'ghcr', got '${IMAGE_SOURCE}'" >&2
      return 1
      ;;
  esac
}

metroride_helm_value_args() {
  metroride_require_valid_image_source || return 1
  local args=(-f "${HELM_CHART}/values-kind.yaml")
  case "${IMAGE_SOURCE}" in
    pr) args+=(-f "${HELM_CHART}/values-kind-pr.yaml") ;;
    ghcr) args+=(-f "${HELM_CHART}/values-kind-ghcr.yaml") ;;
  esac
  # The test database schema has a single source of truth, shared with the
  # Compose stack, rather than being duplicated inside the chart.
  args+=(--set-file "dependencies.postgres.initSql=infrastructure/docker/postgres/init.sql")
  args+=(--set "image.tag=${IMAGE_TAG}")
  printf '%s\n' "${args[@]}"
}

# Deployments that must become Available before the smoke test runs.
# shellcheck disable=SC2034  # consumed by the scripts that source this file.
METRORIDE_REQUIRED_DEPLOYMENTS=(
  postgres
  redis
  rider-service
  driver-service
  dispatch-service
  routing-service
  traffic-service
  notification-service
)
