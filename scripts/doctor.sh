#!/usr/bin/env bash
set -euo pipefail

required=(docker kind kubectl openssl python3)
missing=0

for cmd in "${required[@]}"; do
  if command -v "$cmd" >/dev/null 2>&1; then
    printf "OK   %s\n" "$cmd"
  else
    printf "MISS %s\n" "$cmd"
    missing=1
  fi
done

if ! python3 - <<'PY' >/dev/null 2>&1
import yaml
PY
then
  echo "MISS Python package: PyYAML (required by make validate)"
  missing=1
else
  echo "OK   PyYAML"
fi

if (( missing )); then
  echo
  echo "Install missing prerequisites before continuing."
  exit 1
fi

docker info >/dev/null 2>&1 || {
  echo "Docker is installed but the daemon is not reachable." >&2
  exit 1
}

echo "Developer prerequisites are ready."
