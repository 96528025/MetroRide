#!/usr/bin/env bash
# Collect diagnostics from the ephemeral KinD cluster after a failure.
#
# This script is intentionally best-effort: individual commands may fail (the
# cluster may not exist, a resource may be missing) without stopping the rest of
# the collection. It always exits 0 so that it reports on a failure rather than
# adding a second, less informative one - the original failing step is what
# fails the job, and this step never runs on success.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}" || exit 1
# shellcheck source=scripts/lib/kind.sh
source "${REPO_ROOT}/scripts/lib/kind.sh"

section() {
  echo
  echo "==================== $* ===================="
}

run() {
  echo "\$ $*"
  "$@" 2>&1 || echo "(command failed: $*)"
  echo
}

section "cluster-wide state"
run kubectl get pods -A -o wide
run kubectl get deployments -A
run kubectl get services -A
run kubectl get events -A --sort-by=.metadata.creationTimestamp
run kubectl get nodes -o wide
run kubectl top nodes
run kubectl top pods -A

section "helm release"
run helm status "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}"
run helm get values "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}"

section "workload detail"
# Describe every deployment that is not fully available, plus every pod that is
# not Running/Succeeded - those are the ones worth reading.
while read -r deployment; do
  [[ -z "${deployment}" ]] && continue
  run kubectl --namespace "${HELM_NAMESPACE}" describe "deployment/${deployment}"
done < <(
  kubectl --namespace "${HELM_NAMESPACE}" get deployments \
    -o jsonpath='{range .items[?(@.status.availableReplicas!=@.status.replicas)]}{.metadata.name}{"\n"}{end}' 2>/dev/null
)

while read -r pod; do
  [[ -z "${pod}" ]] && continue
  run kubectl --namespace "${HELM_NAMESPACE}" describe "pod/${pod}"
done < <(
  kubectl --namespace "${HELM_NAMESPACE}" get pods \
    --field-selector=status.phase!=Running,status.phase!=Succeeded \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null
)

section "logs"
# Current logs for every pod in the release namespace, including the test-only
# PostgreSQL and Redis, plus previous logs where a container has restarted -
# that is where a crash loop's real cause is recorded.
while read -r pod; do
  [[ -z "${pod}" ]] && continue
  run kubectl --namespace "${HELM_NAMESPACE}" logs "pod/${pod}" --all-containers --tail=200
  restarts="$(
    kubectl --namespace "${HELM_NAMESPACE}" get "pod/${pod}" \
      -o jsonpath='{range .status.containerStatuses[*]}{.restartCount}{"\n"}{end}' 2>/dev/null \
      | sort -rn | head -1
  )"
  if [[ -n "${restarts}" && "${restarts}" != "0" ]]; then
    echo "(pod ${pod} has restarted ${restarts} time(s); previous logs follow)"
    run kubectl --namespace "${HELM_NAMESPACE}" logs "pod/${pod}" --all-containers --previous --tail=200
  fi
done < <(
  kubectl --namespace "${HELM_NAMESPACE}" get pods \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null
)

echo "diagnostics collection complete"
exit 0
