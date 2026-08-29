#!/usr/bin/env bash
set -euo pipefail

echo "Validating shell scripts..."
shellcheck scripts/*.sh

echo "Validating JSON schemas..."
python3 - <<'PY'
import json
from pathlib import Path
for path in Path("specs").rglob("*.json"):
    json.loads(path.read_text())
    print("OK", path)
PY

echo "Validating YAML syntax..."
python3 - <<'PY'
from pathlib import Path
import yaml
for root in ("config", "deployment", "infrastructure", "specs"):
    for path in Path(root).rglob("*.yaml"):
        list(yaml.safe_load_all(path.read_text()))
        print("OK", path)
PY

echo "Rendering standalone Kustomize configuration..."
kubectl kustomize deployment/kubernetes/standalone >/dev/null

echo "Static validation passed."
