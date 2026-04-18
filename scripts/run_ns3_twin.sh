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
OUTPUT_FILE=""
FLOW_PROFILE_FILE=""
UPF_NAMES="upf"
SLICE_SDS="010203"
UE_SUPIS=""
UE_GNB_MAP=""
GNB_UPF_MAP=""
GNB_POSITIONS=""
UE_POSITIONS=""

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
    --output-file)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --flow-profile-file)
      FLOW_PROFILE_FILE="$2"
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
    --gnb-upf-map)
      GNB_UPF_MAP="$2"
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

mkdir -p "$(dirname "$OUTPUT_FILE")"
cp "$PROJECT_ROOT/sim/ns3/nr_multignb_multiupf.cc" "$NS3_ROOT/scratch/nr_multignb_multiupf.cc"

NS3_ARGS="--runId=$RUN_ID --scenarioId=$SCENARIO_ID --gNbNum=$GNB_NUM --ueNumPerGnb=$UE_NUM_PER_GNB --tickMs=$TICK_MS --simTimeMs=$SIM_TIME_MS --outputFile=$OUTPUT_FILE --upfNames=$UPF_NAMES --sliceSds=$SLICE_SDS"
if [[ -n "$FLOW_PROFILE_FILE" ]]; then
  NS3_ARGS="$NS3_ARGS --flowProfileFile=$FLOW_PROFILE_FILE"
fi
if [[ -n "$UE_NUM" ]]; then
  NS3_ARGS="$NS3_ARGS --ueNum=$UE_NUM"
fi
if [[ -n "$UE_SUPIS" ]]; then
  NS3_ARGS="$NS3_ARGS --ueSupis=$UE_SUPIS"
fi
if [[ -n "$UE_GNB_MAP" ]]; then
  NS3_ARGS="$NS3_ARGS --ueGnbMap=$UE_GNB_MAP"
fi
if [[ -n "$GNB_UPF_MAP" ]]; then
  NS3_ARGS="$NS3_ARGS --gnbUpfMap=$GNB_UPF_MAP"
fi
if [[ -n "$GNB_POSITIONS" ]]; then
  NS3_ARGS="$NS3_ARGS --gnbPositions=$GNB_POSITIONS"
fi
if [[ -n "$UE_POSITIONS" ]]; then
  NS3_ARGS="$NS3_ARGS --uePositions=$UE_POSITIONS"
fi

cd "$NS3_ROOT"
./ns3 run "scratch/nr_multignb_multiupf $NS3_ARGS"