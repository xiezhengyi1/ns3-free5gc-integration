#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NS3_ROOT="${NS3_ROOT:-/home/xiezhengyi/workspace/ns-allinone-3.46.1/ns-3.46.1}"

RUN_ID=""
SCENARIO_ID=""
GNB_NUM="1"
UE_NUM=""
UE_NUM_PER_GNB="1"
TICK_MS="1000"
SIM_TIME_MS="30000"
SIMULATOR="RealtimeSimulatorImpl"
OUTPUT_FILE=""
CLOCK_FILE=""
FLOW_PROFILE_FILE=""
SLICE_RESOURCE_FILE=""
POLICY_RELOAD_MS="1000"
UPF_NAMES="upf"
SLICE_SDS="010203"
UE_SUPIS=""
UE_GNB_MAP=""
GNB_UPF_LINKS=""
GNB_POSITIONS=""
UE_POSITIONS=""
NR_NUMEROLOGY="1"
NR_BANDWIDTH_HZ="100000000"
NR_CENTRAL_FREQUENCY_HZ="3500000000"
NR_TX_POWER_DB="43"
SCHEDULER_TYPE="qos"
TDD_PATTERN="DL|UL|UL|F|DL|UL|UL|F|"
UE_TX_POWER_DB="23"
GNB_NOISE_FIGURE_DB="5"
UE_NOISE_FIGURE_DB="7"
ENABLE_UPLINK_POWER_CONTROL="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --scenario-id)
      SCENARIO_ID="$2"
      shift 2
      ;;
    --g-nb-num)
      GNB_NUM="$2"
      shift 2
      ;;
    --ue-num)
      UE_NUM="$2"
      shift 2
      ;;
    --ue-num-per-g-nb)
      UE_NUM_PER_GNB="$2"
      shift 2
      ;;
    --tick-ms)
      TICK_MS="$2"
      shift 2
      ;;
    --sim-time-ms)
      SIM_TIME_MS="$2"
      shift 2
      ;;
    --simulator)
      SIMULATOR="$2"
      shift 2
      ;;
    --output-file)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --clock-file)
      CLOCK_FILE="$2"
      shift 2
      ;;
    --flow-profile-file)
      FLOW_PROFILE_FILE="$2"
      shift 2
      ;;
    --slice-resource-file)
      SLICE_RESOURCE_FILE="$2"
      shift 2
      ;;
    --policy-reload-ms)
      POLICY_RELOAD_MS="$2"
      shift 2
      ;;
    --upf-names)
      UPF_NAMES="$2"
      shift 2
      ;;
    --slice-sds)
      SLICE_SDS="$2"
      shift 2
      ;;
    --ue-supis)
      UE_SUPIS="$2"
      shift 2
      ;;
    --ue-gnb-map)
      UE_GNB_MAP="$2"
      shift 2
      ;;
    --gnb-upf-links)
      GNB_UPF_LINKS="$2"
      shift 2
      ;;
    --gnb-positions)
      GNB_POSITIONS="$2"
      shift 2
      ;;
    --ue-positions)
      UE_POSITIONS="$2"
      shift 2
      ;;
    --nr-numerology)
      NR_NUMEROLOGY="$2"
      shift 2
      ;;
    --nr-bandwidth-hz)
      NR_BANDWIDTH_HZ="$2"
      shift 2
      ;;
    --nr-central-frequency-hz)
      NR_CENTRAL_FREQUENCY_HZ="$2"
      shift 2
      ;;
    --nr-tx-power-db)
      NR_TX_POWER_DB="$2"
      shift 2
      ;;
    --scheduler-type)
      SCHEDULER_TYPE="$2"
      shift 2
      ;;
    --tdd-pattern)
      TDD_PATTERN="$2"
      shift 2
      ;;
    --ue-tx-power-db)
      UE_TX_POWER_DB="$2"
      shift 2
      ;;
    --gnb-noise-figure-db)
      GNB_NOISE_FIGURE_DB="$2"
      shift 2
      ;;
    --ue-noise-figure-db)
      UE_NOISE_FIGURE_DB="$2"
      shift 2
      ;;
    --enable-uplink-power-control)
      ENABLE_UPLINK_POWER_CONTROL="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$RUN_ID" || -z "$SCENARIO_ID" || -z "$OUTPUT_FILE" ]]; then
  echo "run id, scenario id and output file are required" >&2
  exit 1
fi

if [[ "$OUTPUT_FILE" != /* ]]; then
  OUTPUT_FILE="$PROJECT_ROOT/$OUTPUT_FILE"
fi

if [[ -n "$FLOW_PROFILE_FILE" && "$FLOW_PROFILE_FILE" != /* ]]; then
  FLOW_PROFILE_FILE="$PROJECT_ROOT/$FLOW_PROFILE_FILE"
fi
if [[ -n "$SLICE_RESOURCE_FILE" && "$SLICE_RESOURCE_FILE" != /* ]]; then
  SLICE_RESOURCE_FILE="$PROJECT_ROOT/$SLICE_RESOURCE_FILE"
fi

if [[ -n "$CLOCK_FILE" && "$CLOCK_FILE" != /* ]]; then
  CLOCK_FILE="$PROJECT_ROOT/$CLOCK_FILE"
fi

mkdir -p "$(dirname "$OUTPUT_FILE")"
if [[ -n "$CLOCK_FILE" ]]; then
  mkdir -p "$(dirname "$CLOCK_FILE")"
fi
if [[ ! -d "$NS3_ROOT" ]]; then
  echo "NS3_ROOT does not exist: $NS3_ROOT" >&2
  exit 1
fi

# Keep this check aligned with build_ns3_split.sh.  The reset controller runs
# this script in a new process, so a generic find-only check made a healthy
# 5G-LENA 4.2 installation look unsupported at simulation launch.
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

echo "[run] NS-3 QoS API verified: $NS3_ROOT" >&2
cp "$PROJECT_ROOT/sim/ns3/nr_multignb_multiupf_split.cc" "$NS3_ROOT/scratch/nr_multignb_multiupf_split.cc"

NS3_ARGS=(
  "--runId=$RUN_ID"
  "--scenarioId=$SCENARIO_ID"
  "--gNbNum=$GNB_NUM"
  "--ueNumPerGnb=$UE_NUM_PER_GNB"
  "--tickMs=$TICK_MS"
  "--simTimeMs=$SIM_TIME_MS"
  "--simulator=$SIMULATOR"
  "--outputFile=$OUTPUT_FILE"
  "--upfNames=$UPF_NAMES"
  "--sliceSds=$SLICE_SDS"
  "--policyReloadMs=$POLICY_RELOAD_MS"
  "--nrNumerology=$NR_NUMEROLOGY"
  "--nrBandwidthHz=$NR_BANDWIDTH_HZ"
  "--nrCentralFrequencyHz=$NR_CENTRAL_FREQUENCY_HZ"
  "--nrTxPowerDb=$NR_TX_POWER_DB"
  "--schedulerType=$SCHEDULER_TYPE"
  "--tddPattern=$TDD_PATTERN"
  "--ueTxPowerDb=$UE_TX_POWER_DB"
  "--gnbNoiseFigureDb=$GNB_NOISE_FIGURE_DB"
  "--ueNoiseFigureDb=$UE_NOISE_FIGURE_DB"
  "--enableUplinkPowerControl=$ENABLE_UPLINK_POWER_CONTROL"
)
if [[ -n "$CLOCK_FILE" ]]; then
  NS3_ARGS+=("--clockFile=$CLOCK_FILE")
fi
if [[ -n "$FLOW_PROFILE_FILE" ]]; then
  NS3_ARGS+=("--flowProfileFile=$FLOW_PROFILE_FILE")
fi
if [[ -n "$SLICE_RESOURCE_FILE" ]]; then
  NS3_ARGS+=("--sliceResourceFile=$SLICE_RESOURCE_FILE")
fi
if [[ -n "$UE_NUM" ]]; then
  NS3_ARGS+=("--ueNum=$UE_NUM")
fi
if [[ -n "$UE_SUPIS" ]]; then
  NS3_ARGS+=("--ueSupis=$UE_SUPIS")
fi
if [[ -n "$UE_GNB_MAP" ]]; then
  NS3_ARGS+=("--ueGnbMap=$UE_GNB_MAP")
fi
if [[ -n "$GNB_UPF_LINKS" ]]; then
  NS3_ARGS+=("--gnbUpfLinks=$GNB_UPF_LINKS")
fi
if [[ -n "$GNB_POSITIONS" ]]; then
  NS3_ARGS+=("--gnbPositions=$GNB_POSITIONS")
fi
if [[ -n "$UE_POSITIONS" ]]; then
  NS3_ARGS+=("--uePositions=$UE_POSITIONS")
fi
cd "$NS3_ROOT"
NS3_BINARY="$(find "$NS3_ROOT/build/scratch" -maxdepth 1 -type f -name 'ns3.*-nr_multignb_multiupf_split-*' | head -n 1 || true)"
if [[ -z "$NS3_BINARY" || ! -x "$NS3_BINARY" ]]; then
  echo "unable to locate built split ns-3 binary under $NS3_ROOT/build/scratch" >&2
  exit 1
fi

exec "$NS3_BINARY" "${NS3_ARGS[@]}"
