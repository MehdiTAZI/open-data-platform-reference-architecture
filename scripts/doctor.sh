#!/usr/bin/env bash
set -euo pipefail

required=(docker kubectl kind)
optional=(tofu helm jq yq)
missing=0

for tool in "${required[@]}"; do
  if command -v "$tool" >/dev/null 2>&1; then
    printf 'OK   %s\n' "$tool"
  else
    printf 'MISS %s (required)\n' "$tool"
    missing=1
  fi
done

for tool in "${optional[@]}"; do
  if command -v "$tool" >/dev/null 2>&1; then
    printf 'OK   %s\n' "$tool"
  else
    printf 'INFO %s not installed yet (optional for current milestone)\n' "$tool"
  fi
done

exit "$missing"
