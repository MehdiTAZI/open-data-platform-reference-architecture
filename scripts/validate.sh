#!/usr/bin/env bash
set -euo pipefail

fail=0

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck scripts/*.sh || fail=1
else
  echo "INFO shellcheck unavailable; CI installs/runs it"
fi

if command -v python3 >/dev/null 2>&1; then
  python3 - <<'PY' || fail=1
import json
from pathlib import Path
for p in Path('specs').rglob('*.json'):
    json.loads(p.read_text())
    print(f'JSON OK {p}')
PY
else
  echo "MISS python3 required for JSON validation"
  fail=1
fi

if command -v kubectl >/dev/null 2>&1; then
  kubectl apply --dry-run=client -f deployment/kubernetes/base/namespaces.yaml >/dev/null
  kubectl apply --dry-run=client -f deployment/kubernetes/base/default-deny.yaml >/dev/null
  echo "Kubernetes manifests OK"
fi

exit "$fail"
