#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NS3_ROOT="${NS3_ROOT:-/home/xiezhengyi/workspace/ns-allinone-3.46.1/ns-3.46.1}"

if [[ ! -d "$NS3_ROOT" ]]; then
  echo "NS3_ROOT does not exist: $NS3_ROOT" >&2
  exit 1
fi

# Use the exact 5G-LENA source locations when available.  The recursive search
# retains compatibility with a non-standard contrib layout, but does not hide
# which prerequisite is missing.
QOS_FLOW_HEADER="$NS3_ROOT/contrib/nr/model/nr-qos-flow.h"
QOS_SCHEDULER_HEADER="$NS3_ROOT/contrib/nr/model/nr-mac-scheduler-ofdma-qos.h"
NR_HELPER_HEADER="$NS3_ROOT/contrib/nr/helper/nr-helper.h"
[[ -f "$QOS_FLOW_HEADER" ]] || QOS_FLOW_HEADER="$(find "$NS3_ROOT" -type f -name nr-qos-flow.h -print -quit || true)"
[[ -f "$QOS_SCHEDULER_HEADER" ]] || QOS_SCHEDULER_HEADER="$(find "$NS3_ROOT" -type f -name nr-mac-scheduler-ofdma-qos.h -print -quit || true)"
[[ -f "$NR_HELPER_HEADER" ]] || NR_HELPER_HEADER="$(find "$NS3_ROOT" -type f -name nr-helper.h -print -quit || true)"

missing=()
[[ -n "$QOS_FLOW_HEADER" && -f "$QOS_FLOW_HEADER" ]] || missing+=("NrQosFlow header (nr-qos-flow.h)")
[[ -n "$QOS_SCHEDULER_HEADER" && -f "$QOS_SCHEDULER_HEADER" ]] || missing+=("NrMacSchedulerOfdmaQos header (nr-mac-scheduler-ofdma-qos.h)")
[[ -n "$NR_HELPER_HEADER" && -f "$NR_HELPER_HEADER" ]] || missing+=("NrHelper header (nr-helper.h)")
if [[ -n "$NR_HELPER_HEADER" && -f "$NR_HELPER_HEADER" ]] && ! grep -Fq 'ActivateDedicatedQosFlow' "$NR_HELPER_HEADER"; then
  missing+=("NrHelper::ActivateDedicatedQosFlow API")
fi
if (( ${#missing[@]} > 0 )); then
  echo "NS-3 QoS API preflight failed for: $NS3_ROOT" >&2
  printf 'Missing: %s\n' "${missing[*]}" >&2
  exit 1
fi

echo "[build] NS-3 QoS API verified: $NS3_ROOT" >&2

cp "$PROJECT_ROOT/sim/ns3/nr_multignb_multiupf_split.cc" "$NS3_ROOT/scratch/nr_multignb_multiupf_split.cc"
cd "$NS3_ROOT"
./ns3 configure --enable-examples >/dev/null
./ns3 build >/dev/null
