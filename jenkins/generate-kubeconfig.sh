#!/usr/bin/env bash
set -euo pipefail

SERVICE_ACCOUNT_NAMESPACE="${SERVICE_ACCOUNT_NAMESPACE:-leaderboard}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-jenkins-deployer}"
OUTPUT_PATH="${OUTPUT_PATH:-./jenkins/kubeconfig}"

SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
CA_DATA="$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
TOKEN="$(kubectl -n "${SERVICE_ACCOUNT_NAMESPACE}" create token "${SERVICE_ACCOUNT_NAME}")"

mkdir -p "$(dirname "${OUTPUT_PATH}")"

cat > "${OUTPUT_PATH}" <<EOF
apiVersion: v1
kind: Config
clusters:
  - cluster:
      certificate-authority-data: ${CA_DATA}
      server: ${SERVER}
    name: local-cluster
contexts:
  - context:
      cluster: local-cluster
      namespace: ${SERVICE_ACCOUNT_NAMESPACE}
      user: ${SERVICE_ACCOUNT_NAME}
    name: jenkins-context
current-context: jenkins-context
users:
  - name: ${SERVICE_ACCOUNT_NAME}
    user:
      token: ${TOKEN}
EOF

echo "kubeconfig written to ${OUTPUT_PATH}"
