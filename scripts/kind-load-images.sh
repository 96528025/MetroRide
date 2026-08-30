#!/usr/bin/env bash
# Side-load every image the release needs into the KinD node.
#
# The image list is derived from the rendered Helm manifests, not from a second
# hand-maintained list, so the chart and the loader cannot drift: whatever the
# cluster is about to run is exactly what gets loaded.
#
# Service images must already be present locally - built from this commit (pull
# request path) or pulled back from GHCR (trusted delivery path). A missing one
# is a hard error, never a silent registry pull. Test-only dependency images
# are fetched here if absent.
#
# After this runs the cluster needs no registry access at all, which is why no
# imagePullSecret exists anywhere in this project.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"
# shellcheck source=scripts/lib/images.sh
source "${REPO_ROOT}/scripts/lib/images.sh"
# shellcheck source=scripts/lib/kind.sh
source "${REPO_ROOT}/scripts/lib/kind.sh"

metroride_require_valid_image_source
mapfile -t VALUE_ARGS < <(metroride_helm_value_args)

mapfile -t IMAGES < <(
  helm template "${HELM_RELEASE}" "${HELM_CHART}" "${VALUE_ARGS[@]}" \
    | grep -oE '^[[:space:]]*image:[[:space:]]*"?[^"[:space:]]+"?' \
    | sed -E 's/.*image:[[:space:]]*//; s/"//g' \
    | sort -u
)

if [[ ${#IMAGES[@]} -eq 0 ]]; then
  echo "failed: rendered manifests contain no images" >&2
  exit 1
fi

SERVICE_IMAGE_PREFIX="$(metroride_image_repository "")"

echo "loading ${#IMAGES[@]} image(s) into KinD cluster '${KIND_CLUSTER_NAME}'"
for ref in "${IMAGES[@]}"; do
  if ! docker image inspect "${ref}" >/dev/null 2>&1; then
    if [[ "${ref}" == "${SERVICE_IMAGE_PREFIX}"* ]]; then
      echo "failed: service image ${ref} is not present locally." >&2
      echo "  Build it (scripts/build-images.sh) or pull it (scripts/pull-images.sh) first." >&2
      exit 1
    fi
    echo "==> pulling dependency image ${ref}"
    docker pull --quiet "${ref}"
  fi
  echo "==> kind load docker-image ${ref}"
  kind load docker-image "${ref}" --name "${KIND_CLUSTER_NAME}"
done

echo "ok: loaded ${IMAGES[*]}"
