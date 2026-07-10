#!/bin/bash

set -e

KUBECONFIG=/tmp/kubeconfig-kind.yaml

# ------------------------------------------------------------------------------
#                            Bootstrap Kind Cluster
# ------------------------------------------------------------------------------
KINDCONFIG=/tmp/kind-config.yaml

# Create a KinD configuration file.
cat << EOF > $KINDCONFIG
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  image: kindest/node:v1.35.0
EOF

# Create cluster, then delete its config file.
kind delete cluster
kind create cluster --config $KINDCONFIG --kubeconfig $KUBECONFIG
rm $KINDCONFIG

printf "\n\n### KIND cluster now fully deployed (KUBECONF=$KUBECONFIG)\n"
