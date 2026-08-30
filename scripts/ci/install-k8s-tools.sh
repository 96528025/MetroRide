#!/usr/bin/env bash
# Install pinned kubectl, Helm and KinD for deployment validation.
#
# Versions are pinned rather than taken from whatever the runner image happens
# to ship, so a cluster created today is the same one created next month and a
# runner-image change cannot silently alter the Kubernetes version under test.
set -euo pipefail

KIND_VERSION="${KIND_VERSION:-v0.30.0}"
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.34.1}"
HELM_VERSION="${HELM_VERSION:-v3.19.0}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

# GitHub-hosted runners own /usr/local/bin as root, so installing there needs
# sudo; a root container (or a writable INSTALL_DIR) does not. Resolve it once
# rather than assuming either.
SUDO=()
if [[ ! -w "${INSTALL_DIR}" ]]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO=(sudo)
  else
    echo "failed: ${INSTALL_DIR} is not writable and sudo is unavailable" >&2
    exit 1
  fi
fi

install_binary() {
  "${SUDO[@]+"${SUDO[@]}"}" install -m 0755 "$1" "${INSTALL_DIR}/$2"
}

echo "==> installing kubectl ${KUBECTL_VERSION}"
curl -fsSLo "${tmp}/kubectl" "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
install_binary "${tmp}/kubectl" kubectl

echo "==> installing helm ${HELM_VERSION}"
curl -fsSLo "${tmp}/helm.tar.gz" "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz"
tar -xzf "${tmp}/helm.tar.gz" -C "${tmp}" linux-amd64/helm
install_binary "${tmp}/linux-amd64/helm" helm

echo "==> installing kind ${KIND_VERSION}"
curl -fsSLo "${tmp}/kind" "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64"
install_binary "${tmp}/kind" kind

kubectl version --client
helm version --short
kind --version
