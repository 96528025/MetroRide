#!/usr/bin/env bash
# Delete the ephemeral KinD cluster. Always safe to run, including when the
# cluster was never created, so it can be wired to an unconditional CI step.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"
# shellcheck source=scripts/lib/kind.sh
source "${REPO_ROOT}/scripts/lib/kind.sh"

kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
echo "ok: KinD cluster '${KIND_CLUSTER_NAME}' deleted"
