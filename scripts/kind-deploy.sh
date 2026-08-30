#!/usr/bin/env bash
# Install the MetroRide Helm release into the ephemeral KinD cluster and wait
# for every required workload to become Available.
#
# Both deployment paths run this identical script; only IMAGE_SOURCE differs,
# which selects the values file recording how the images were acquired.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"
# shellcheck source=scripts/lib/images.sh
source "${REPO_ROOT}/scripts/lib/images.sh"
# shellcheck source=scripts/lib/kind.sh
source "${REPO_ROOT}/scripts/lib/kind.sh"

metroride_require_valid_image_source
mapfile -t VALUE_ARGS < <(metroride_helm_value_args)

echo "installing release '${HELM_RELEASE}' (image source: ${IMAGE_SOURCE}, tag: ${IMAGE_TAG})"
helm upgrade --install "${HELM_RELEASE}" "${HELM_CHART}" \
  "${VALUE_ARGS[@]}" \
  --namespace "${HELM_NAMESPACE}" \
  --create-namespace \
  --wait \
  --timeout "${HELM_TIMEOUT:-6m}"

for deployment in "${METRORIDE_REQUIRED_DEPLOYMENTS[@]}"; do
  echo "==> waiting for deployment/${deployment}"
  kubectl --namespace "${HELM_NAMESPACE}" rollout status "deployment/${deployment}" \
    --timeout "${ROLLOUT_TIMEOUT:-300s}"
done

kubectl --namespace "${HELM_NAMESPACE}" get pods -o wide
echo "ok: all required deployments are available"
