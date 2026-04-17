#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NS3_ROOT="${NS3_ROOT:-/home/xiezhengyi/workspace/ns-allinone-3.46.1/ns-3.46.1}"

cp "$PROJECT_ROOT/sim/ns3/nr_multignb_multiupf.cc" "$NS3_ROOT/scratch/nr_multignb_multiupf.cc"
cd "$NS3_ROOT"
./ns3 configure --enable-examples >/dev/null
./ns3 build >/dev/null