#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python3"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

HOST="0.0.0.0"
PORT="18080"
PCF_CONTAINER="pcf"
PCF_PORT="8000"
PCF_HOST="10.100.200.20"
DEFAULT_TIMEOUT_MS="10000"
FLOW_PROFILE_FILE=""
LATEST_SNAPSHOT_FILE=""
STATE_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --flow-profile-file)
      FLOW_PROFILE_FILE="$2"
      shift 2
      ;;
    --latest-snapshot-file)
      LATEST_SNAPSHOT_FILE="$2"
      shift 2
      ;;
    --state-file)
      STATE_FILE="$2"
      shift 2
      ;;
    --upstream-pcf-container)
      PCF_CONTAINER="$2"
      shift 2
      ;;
    --upstream-pcf-host)
      PCF_HOST="$2"
      shift 2
      ;;
    --upstream-pcf-port)
      PCF_PORT="$2"
      shift 2
      ;;
    --default-timeout-ms)
      DEFAULT_TIMEOUT_MS="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${FLOW_PROFILE_FILE}" || -z "${LATEST_SNAPSHOT_FILE}" || -z "${STATE_FILE}" ]]; then
  echo "flow profile, latest snapshot, and state file are required" >&2
  exit 2
fi

# Keep a short readiness wait when the target PCF container is present locally.
if [[ -n "${PCF_CONTAINER}" ]] && docker inspect "${PCF_CONTAINER}" >/dev/null 2>&1; then
  for _ in $(seq 1 60); do
    if docker inspect "${PCF_CONTAINER}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

exec "${PYTHON_BIN}" -m bridge.policy_acceptor \
  --host "${HOST}" \
  --port "${PORT}" \
  --flow-profile-file "${FLOW_PROFILE_FILE}" \
  --latest-snapshot-file "${LATEST_SNAPSHOT_FILE}" \
  --state-file "${STATE_FILE}" \
  --upstream-pcf-base-url "http://${PCF_HOST}:${PCF_PORT}" \
  --default-timeout-ms "${DEFAULT_TIMEOUT_MS}"
