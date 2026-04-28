#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NS3_ROOT="${NS3_ROOT:-/home/xiezhengyi/workspace/ns-allinone-3.46.1/ns-3.46.1}"

ensure_tap_creator_permissions() {
  local tap_creator
  tap_creator="$(find "$NS3_ROOT/build/src/tap-bridge" -maxdepth 1 -type f -name 'ns*-tap-creator*' | head -n 1 || true)"
  if [[ -z "$tap_creator" || ! -f "$tap_creator" ]]; then
    return 0
  fi

  if [[ -u "$tap_creator" && "$(stat -c '%U' "$tap_creator")" == "root" ]]; then
    return 0
  fi

  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    sudo chown root "$tap_creator"
    sudo chmod u+s "$tap_creator"
    return 0
  fi

  if command -v docker >/dev/null 2>&1; then
    docker run --rm --privileged --pid host --network host -v /:/host free5gc/base:latest \
      chroot /host /bin/bash -lc "chown root '$tap_creator' && chmod u+s '$tap_creator'"
    return 0
  fi

  echo "warning: could not set tap-creator permissions automatically: $tap_creator" >&2
}

cp "$PROJECT_ROOT/sim/ns3/nr_multignb_multiupf.cc" "$NS3_ROOT/scratch/nr_multignb_multiupf.cc"
cd "$NS3_ROOT"
./ns3 configure --enable-examples >/dev/null
./ns3 build >/dev/null
ensure_tap_creator_permissions
