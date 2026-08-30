#!/usr/bin/env bash
# Shared image naming for MetroRide container builds.
#
# This file is sourced by the build/pull scripts and by CI. It is the single
# source of truth for which services are built and how their images are named,
# so GitHub Actions, the Helm chart and the documentation cannot drift apart.
#
# Canonical image reference:
#
#   ghcr.io/96528025/metroride-<service>:<immutable-tag>
#
# GHCR rejects uppercase path components, so the registry, namespace and name
# prefix are normalised to lowercase here rather than relying on callers.

# The six core services that make up the default (non-Kafka) runtime profile.
# analytics-service is deliberately excluded: it only runs under the optional
# `kafka` Compose profile and is not part of the released core set.
METRORIDE_SERVICES=(
  rider-service
  driver-service
  dispatch-service
  routing-service
  traffic-service
  notification-service
)

metroride_lowercase() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

IMAGE_REGISTRY="$(metroride_lowercase "${IMAGE_REGISTRY:-ghcr.io}")"
IMAGE_NAMESPACE="$(metroride_lowercase "${IMAGE_NAMESPACE:-96528025}")"
IMAGE_NAME_PREFIX="$(metroride_lowercase "${IMAGE_NAME_PREFIX:-metroride-}")"

# Immutable tag. CI always passes the full commit SHA; locally we default to
# the checked-out commit so local builds match what CI would produce.
if [[ -z "${IMAGE_TAG:-}" ]]; then
  IMAGE_TAG="$(git rev-parse HEAD 2>/dev/null || true)"
fi
if [[ -z "${IMAGE_TAG}" ]]; then
  echo "failed: IMAGE_TAG is not set and the current directory is not a git checkout" >&2
  exit 1
fi

# Optional additional human-readable tags (for example a v1.2.3 release tag).
# Deployments always use the immutable tag above; these are conveniences only.
# shellcheck disable=SC2034  # consumed by the scripts that source this file.
read -r -a IMAGE_EXTRA_TAGS_ARRAY <<< "${IMAGE_EXTRA_TAGS:-}" || true

metroride_image_repository() {
  printf '%s/%s/%s%s' "${IMAGE_REGISTRY}" "${IMAGE_NAMESPACE}" "${IMAGE_NAME_PREFIX}" "$1"
}

metroride_image_ref() {
  printf '%s:%s' "$(metroride_image_repository "$1")" "${2:-${IMAGE_TAG}}"
}

# Resolve the service list from positional arguments, defaulting to all six.
metroride_resolve_services() {
  local requested=("$@")
  if [[ ${#requested[@]} -eq 0 ]]; then
    printf '%s\n' "${METRORIDE_SERVICES[@]}"
    return 0
  fi
  local service known
  for service in "${requested[@]}"; do
    known=false
    local candidate
    for candidate in "${METRORIDE_SERVICES[@]}"; do
      if [[ "${candidate}" == "${service}" ]]; then
        known=true
        break
      fi
    done
    if [[ "${known}" != true ]]; then
      echo "failed: unknown service '${service}' (known: ${METRORIDE_SERVICES[*]})" >&2
      return 1
    fi
    printf '%s\n' "${service}"
  done
}
