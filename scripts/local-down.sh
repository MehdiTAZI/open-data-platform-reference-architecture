#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"
kind delete cluster --name "$cluster_name"
rm -rf .local
