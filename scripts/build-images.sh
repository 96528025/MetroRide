#!/usr/bin/env bash
# Build one container image per MetroRide core service from the single shared
# Dockerfile (infrastructure/docker/Dockerfile.service). There is deliberately
# no per-service Dockerfile: the service name is a build argument.
#
# Usage:
#   scripts/build-images.sh                     # build all six, do not push
#   scripts/build-images.sh rider-service       # build one service
#   scripts/build-images.sh --push              # build and push all six
#   IMAGE_TAG=<sha> scripts/build-images.sh     # override the immutable tag
#
# Pushing requires the caller to already be authenticated to the registry.
# Pull requests never push: only the trusted delivery path passes --push.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

PUSH=false
declare -a REQUESTED=()
for arg in "$@"; do
  case "${arg}" in
    --push) PUSH=true ;;
    -*) echo "failed: unknown flag '${arg}'" >&2; exit 1 ;;
    *) REQUESTED+=("${arg}") ;;
  esac
done

# shellcheck source=scripts/lib/images.sh
source "${REPO_ROOT}/scripts/lib/images.sh"

mapfile -t SERVICES < <(metroride_resolve_services "${REQUESTED[@]+"${REQUESTED[@]}"}")

echo "building ${#SERVICES[@]} image(s) at tag ${IMAGE_TAG} (push=${PUSH})"

for service in "${SERVICES[@]}"; do
  primary="$(metroride_image_ref "${service}")"
  declare -a tag_args=(--tag "${primary}")
  for extra in "${IMAGE_EXTRA_TAGS_ARRAY[@]+"${IMAGE_EXTRA_TAGS_ARRAY[@]}"}"; do
    [[ -z "${extra}" ]] && continue
    tag_args+=(--tag "$(metroride_image_ref "${service}" "${extra}")")
  done

  echo "==> building ${primary}"
  docker build \
    --file infrastructure/docker/Dockerfile.service \
    --build-arg "SERVICE=${service}" \
    "${tag_args[@]}" \
    .

  if [[ "${PUSH}" == true ]]; then
    echo "==> pushing ${primary}"
    docker push "${primary}"
    for extra in "${IMAGE_EXTRA_TAGS_ARRAY[@]+"${IMAGE_EXTRA_TAGS_ARRAY[@]}"}"; do
      [[ -z "${extra}" ]] && continue
      docker push "$(metroride_image_ref "${service}" "${extra}")"
    done
  fi
done

echo "ok: built ${SERVICES[*]}"
