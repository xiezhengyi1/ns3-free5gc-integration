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
GNB_UPF_MAP=""
GNB_POSITIONS=""
UE_POSITIONS=""
BRIDGE_GNB_TAPS=""
BRIDGE_UPF_TAPS=""
BRIDGE_LINK_RATE_MBPS="1000"
BRIDGE_LINK_DELAY_MS="1"
BRIDGE_LINK_LOSS_RATE="0"
EXTERNAL_TRAFFIC_ONLY="false"
EXTERNAL_TRAFFIC_TARGET_IP="8.8.8.8"
EXTERNAL_TRAFFIC_SOURCE_BASE_PORT="15000"

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
    --bridge-gnb-taps)
      BRIDGE_GNB_TAPS="$2"
      shift 2
      ;;
    --bridge-upf-taps)
      BRIDGE_UPF_TAPS="$2"
      shift 2
      ;;
    --bridge-link-rate-mbps)
      BRIDGE_LINK_RATE_MBPS="$2"
      shift 2
      ;;
    --bridge-link-delay-ms)
      BRIDGE_LINK_DELAY_MS="$2"
      shift 2
      ;;
    --bridge-link-loss-rate)
      BRIDGE_LINK_LOSS_RATE="$2"
      shift 2
      ;;
    --external-traffic-only)
      EXTERNAL_TRAFFIC_ONLY="true"
      shift
      ;;
    --external-traffic-target-ip)
      EXTERNAL_TRAFFIC_TARGET_IP="$2"
      shift 2
      ;;
    --external-traffic-source-base-port)
      EXTERNAL_TRAFFIC_SOURCE_BASE_PORT="$2"
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
cp "$PROJECT_ROOT/sim/ns3/nr_multignb_multiupf.cc" "$NS3_ROOT/scratch/nr_multignb_multiupf.cc"
ensure_tap_creator_permissions

NS3_ARGS="--runId=$RUN_ID --scenarioId=$SCENARIO_ID --gNbNum=$GNB_NUM --ueNumPerGnb=$UE_NUM_PER_GNB --tickMs=$TICK_MS --simTimeMs=$SIM_TIME_MS --simulator=$SIMULATOR --outputFile=$OUTPUT_FILE --upfNames=$UPF_NAMES --sliceSds=$SLICE_SDS --policyReloadMs=$POLICY_RELOAD_MS --bridgeLinkRateMbps=$BRIDGE_LINK_RATE_MBPS --bridgeLinkDelayMs=$BRIDGE_LINK_DELAY_MS --bridgeLinkLossRate=$BRIDGE_LINK_LOSS_RATE"
if [[ -n "$CLOCK_FILE" ]]; then
  NS3_ARGS="$NS3_ARGS --clockFile=$CLOCK_FILE"
fi
if [[ -n "$FLOW_PROFILE_FILE" ]]; then
  NS3_ARGS="$NS3_ARGS --flowProfileFile=$FLOW_PROFILE_FILE"
fi
if [[ -n "$SLICE_RESOURCE_FILE" ]]; then
  NS3_ARGS="$NS3_ARGS --sliceResourceFile=$SLICE_RESOURCE_FILE"
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
if [[ -n "$BRIDGE_GNB_TAPS" ]]; then
  NS3_ARGS="$NS3_ARGS --bridgeGnbTaps=$BRIDGE_GNB_TAPS"
fi
if [[ -n "$BRIDGE_UPF_TAPS" ]]; then
  NS3_ARGS="$NS3_ARGS --bridgeUpfTaps=$BRIDGE_UPF_TAPS"
fi
if [[ "$EXTERNAL_TRAFFIC_ONLY" == "true" ]]; then
  NS3_ARGS="$NS3_ARGS --externalTrafficOnly=true --externalTrafficTargetIp=$EXTERNAL_TRAFFIC_TARGET_IP --externalTrafficSourceBasePort=$EXTERNAL_TRAFFIC_SOURCE_BASE_PORT"
fi

cd "$NS3_ROOT"
./ns3 run "scratch/nr_multignb_multiupf $NS3_ARGS"
