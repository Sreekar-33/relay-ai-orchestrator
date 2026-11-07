#!/usr/bin/env bash
url=$1; timeout=${2:-60}; start=$(date +%s)
until curl -fsS "$url" >/dev/null 2>&1; do
  if (( $(date +%s) - start > timeout )); then echo "timeout waiting for $url" >&2; exit 1; fi
  sleep 1
done
echo "ready: $url"
