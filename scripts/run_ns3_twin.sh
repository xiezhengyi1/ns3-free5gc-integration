#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NS3_ROOT="${NS3_ROOT:-/home/xiezhengyi/workspace/ns-allinone-3.46.1/ns-3.46.1}"

RUN_ID=""
SCENARIO_ID=""
GNB_NUM="1"
UE_NUM_PER_GNB="1"
TICK_MS="1000"
SIM_TIME_MS="30000"
OUTPUT_FILE=""
UPF_NAMES="upf"
SLICE_SDS="010203"

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
    --upf-names)
      UPF_NAMES="$2"
      shift 2
      ;;
    --slice-sds)
      SLICE_SDS="$2"
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

mkdir -p "$(dirname "$OUTPUT_FILE")"
cp "$PROJECT_ROOT/sim/ns3/nr_multignb_multiupf.cc" "$NS3_ROOT/scratch/nr_multignb_multiupf.cc"

cd "$NS3_ROOT"
./ns3 run "scratch/nr_multignb_multiupf --runId=$RUN_ID --scenarioId=$SCENARIO_ID --gNbNum=$GNB_NUM --ueNumPerGnb=$UE_NUM_PER_GNB --tickMs=$TICK_MS --simTimeMs=$SIM_TIME_MS --outputFile=$OUTPUT_FILE --upfNames=$UPF_NAMES --sliceSds=$SLICE_SDS"