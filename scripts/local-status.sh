#!/usr/bin/env bash
set -euo pipefail

printf "\n== control plane ==\n"
kubectl -n odp-system get pods,svc,job -o wide
printf "\n== data plane ==\n"
kubectl -n odp-data get pods,svc -o wide
printf "\n== observability plane ==\n"
kubectl -n odp-observability get pods,svc -o wide 2>/dev/null || true
