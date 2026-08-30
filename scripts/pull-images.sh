#!/usr/bin/env bash
# Pull the exact immutable, SHA-tagged MetroRide images back from the registry
# onto the machine running this script.
#
# This is the trusted-delivery verification step: it proves the images were
# actually published and that the caller's credentials can authenticate and
# retrieve them. The pulled images are then loaded into the ephemeral KinD
# cluster, so no registry credential is ever placed inside Kubernetes.
#
# Usage:
#   IMAGE_TAG=<sha> scripts/pull-images.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# shellcheck source=scripts/lib/images.sh
source "${REPO_ROOT}/scripts/lib/images.sh"

mapfile -t SERVICES < <(metroride_resolve_services "$@")

echo "pulling ${#SERVICES[@]} image(s) at tag ${IMAGE_TAG}"
for service in "${SERVICES[@]}"; do
  ref="$(metroride_image_ref "${service}")"
  echo "==> pulling ${ref}"
  docker pull --quiet "${ref}"
  # Print the resolved digest so the run log records exactly which artifact
  # was deployed, not just the tag it was requested under.
  docker image inspect --format '{{ .RepoDigests }}' "${ref}"
done

echo "ok: pulled ${SERVICES[*]}"
