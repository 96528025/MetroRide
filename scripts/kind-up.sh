#!/usr/bin/env bash
# Create the ephemeral KinD cluster used for deployment validation.
#
# Cluster creation is the one genuinely transient setup step here (image
# unpack, control-plane start), so it gets exactly one retry. Everything after
# it is deterministic and is never retried: a repeated failure is a real
# failure and must surface as one.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"
# shellcheck source=scripts/lib/kind.sh
source "${REPO_ROOT}/scripts/lib/kind.sh"

create_cluster() {
  kind create cluster \
    --name "${KIND_CLUSTER_NAME}" \
    --image "${KIND_NODE_IMAGE}" \
    --config "${KIND_CONFIG}" \
    --wait 180s
}

if ! create_cluster; then
  echo "warning: KinD cluster creation failed; deleting any partial cluster and retrying once" >&2
  kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
  create_cluster
fi

kubectl cluster-info --context "kind-${KIND_CLUSTER_NAME}"
kubectl wait --for=condition=Ready nodes --all --timeout=180s
echo "ok: KinD cluster '${KIND_CLUSTER_NAME}' is ready"
