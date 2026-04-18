// SPDX-License-Identifier: GPL-2.0-only

#include "ns3/antenna-module.h"
#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/internet-apps-module.h"
#include "ns3/internet-module.h"
#include "ns3/mobility-module.h"
#include "ns3/nr-module.h"
#include "ns3/point-to-point-module.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("NrMultiGnbMultiUpfTwin");

namespace
{

std::string
EscapeJson(const std::string& value)
{
    std::ostringstream escaped;
    for (char character : value)
    {
        switch (character)
        {
        case '\\':
            escaped << "\\\\";
            break;
        case '"':
            escaped << "\\\"";
            break;
        case '\n':
            escaped << "\\n";
            break;
        default:
            escaped << character;
            break;
        }
    }
    return escaped.str();
}

std::string
Quote(const std::string& value)
{
    return "\"" + EscapeJson(value) + "\"";
}

std::string
ToString(const Ipv4Address& address)
{
    std::ostringstream stream;
    address.Print(stream);
    return stream.str();
}

std::vector<std::string>
SplitString(const std::string& input, char delimiter, bool keepEmpty = false)
{
    std::vector<std::string> parts;
    std::stringstream stream(input);
    std::string part;
    while (std::getline(stream, part, delimiter))
    {
        if (!part.empty() || keepEmpty)
        {
            parts.push_back(part);
        }
    }
    return parts;
}

std::vector<std::string>
SplitCsv(const std::string& input)
{
    return SplitString(input, ',');
}

std::vector<std::string>
ParseStringList(const std::string& input, uint32_t expectedCount, const std::string& fieldName)
{
    if (input.empty())
    {
        return {};
    }

    auto values = SplitCsv(input);
    if (values.size() != expectedCount)
    {
        NS_FATAL_ERROR(fieldName << " expects " << expectedCount << " values, got " << values.size());
    }
    return values;
}

std::vector<uint32_t>
ParseIndexList(const std::string& input,
               uint32_t expectedCount,
               uint32_t maxValue,
               const std::string& fieldName)
{
    if (input.empty())
    {
        return {};
    }

    auto values = SplitCsv(input);
    if (values.size() != expectedCount)
    {
        NS_FATAL_ERROR(fieldName << " expects " << expectedCount << " values, got " << values.size());
    }

    std::vector<uint32_t> parsed;
    parsed.reserve(values.size());
    for (const auto& value : values)
    {
        const auto parsedValue = static_cast<uint32_t>(std::stoul(value));
        if (parsedValue < 1 || parsedValue > maxValue)
        {
            NS_FATAL_ERROR(fieldName << " value " << parsedValue << " is out of range 1.." << maxValue);
        }
        parsed.push_back(parsedValue);
    }
    return parsed;
}

struct PositionOverrides
{
    std::vector<bool> hasPosition;
    std::vector<Vector> positions;
};

struct FlowProfile
{
    std::string flowId;
    std::string flowName;
    std::string ueName;
    std::string supi;
    std::string appId;
    std::string appName;
    std::string sliceRef;
    std::string sliceSnssai;
    std::string serviceType;
    uint32_t serviceTypeId = 0;
    uint32_t fiveQi = 9;
    double packetSizeBytes = 512.0;
    double arrivalRatePps = 1000.0;
    double latencyMs = 0.0;
    double jitterMs = 0.0;
    double lossRate = 0.0;
    double bandwidthDlMbps = 0.0;
    double bandwidthUlMbps = 0.0;
    double guaranteedBandwidthDlMbps = 0.0;
    double guaranteedBandwidthUlMbps = 0.0;
    uint32_t priority = 0;
    double allocatedBandwidthDlMbps = 0.0;
    double allocatedBandwidthUlMbps = 0.0;
    bool optimizeRequested = false;
};

PositionOverrides
ParsePositionOverrides(const std::string& input,
                       uint32_t expectedCount,
                       const std::string& fieldName)
{
    PositionOverrides overrides;
    overrides.hasPosition.assign(expectedCount, false);
    overrides.positions.assign(expectedCount, Vector(0.0, 0.0, 0.0));
    if (input.empty())
    {
        return overrides;
    }

    auto values = SplitString(input, ';', true);
    if (values.size() != expectedCount)
    {
        NS_FATAL_ERROR(fieldName << " expects " << expectedCount << " values, got " << values.size());
    }

    for (uint32_t index = 0; index < values.size(); ++index)
    {
        if (values[index].empty() || values[index] == "auto")
        {
            continue;
        }
        const auto coordinates = SplitString(values[index], ':');
        if (coordinates.size() != 3)
        {
            NS_FATAL_ERROR(fieldName << " value '" << values[index] << "' must use x:y:z format");
        }
        overrides.positions[index] = Vector(
            std::stod(coordinates[0]),
            std::stod(coordinates[1]),
            std::stod(coordinates[2]));
        overrides.hasPosition[index] = true;
    }
    return overrides;
}

void
ApplyPositionOverrides(const NodeContainer& nodes, const PositionOverrides& overrides)
{
    const auto count = std::min<uint32_t>(nodes.GetN(), overrides.hasPosition.size());
    for (uint32_t index = 0; index < count; ++index)
    {
        if (!overrides.hasPosition[index])
        {
            continue;
        }
        auto mobility = nodes.Get(index)->GetObject<MobilityModel>();
        if (mobility == nullptr)
        {
            NS_FATAL_ERROR("node " << index << " has no MobilityModel");
        }
        mobility->SetPosition(overrides.positions[index]);
    }
}

double
ParseOptionalDouble(const std::string& value, double fallback = 0.0)
{
    if (value.empty())
    {
        return fallback;
    }
    return std::stod(value);
}

uint32_t
ParseOptionalUint(const std::string& value, uint32_t fallback = 0)
{
    if (value.empty())
    {
        return fallback;
    }
    return static_cast<uint32_t>(std::stoul(value));
}

bool
ParseOptionalBool(const std::string& value, bool fallback = false)
{
    if (value.empty())
    {
        return fallback;
    }
    return value == "true" || value == "1" || value == "True";
}

std::string
GetColumnValue(const std::map<std::string, uint32_t>& headerIndex,
               const std::vector<std::string>& columns,
               const std::string& name)
{
    auto it = headerIndex.find(name);
    if (it == headerIndex.end())
    {
        return "";
    }
    return it->second < columns.size() ? columns[it->second] : "";
}

std::vector<FlowProfile>
LoadFlowProfiles(const std::string& path)
{
    if (path.empty())
    {
        return {};
    }

    std::ifstream input(path);
    if (!input.is_open())
    {
        NS_FATAL_ERROR("unable to open flow profile file: " << path);
    }

    std::string headerLine;
    if (!std::getline(input, headerLine))
    {
        return {};
    }

    auto headerColumns = SplitString(headerLine, '\t', true);
    std::map<std::string, uint32_t> headerIndex;
    for (uint32_t index = 0; index < headerColumns.size(); ++index)
    {
        headerIndex[headerColumns[index]] = index;
    }

    std::vector<FlowProfile> profiles;
    std::string line;
    while (std::getline(input, line))
    {
        if (line.empty())
        {
            continue;
        }
        auto columns = SplitString(line, '\t', true);
        if (columns.size() < headerColumns.size())
        {
            columns.resize(headerColumns.size());
        }

        FlowProfile profile;
        profile.flowId = GetColumnValue(headerIndex, columns, "flow_id");
        profile.flowName = GetColumnValue(headerIndex, columns, "flow_name");
        profile.ueName = GetColumnValue(headerIndex, columns, "ue_name");
        profile.supi = GetColumnValue(headerIndex, columns, "supi");
        profile.appId = GetColumnValue(headerIndex, columns, "app_id");
        profile.appName = GetColumnValue(headerIndex, columns, "app_name");
        profile.sliceRef = GetColumnValue(headerIndex, columns, "slice_ref");
        profile.sliceSnssai = GetColumnValue(headerIndex, columns, "slice_snssai");
        profile.serviceType = GetColumnValue(headerIndex, columns, "service_type");
        profile.serviceTypeId = ParseOptionalUint(GetColumnValue(headerIndex, columns, "service_type_id"), 0);
        profile.fiveQi = ParseOptionalUint(GetColumnValue(headerIndex, columns, "five_qi"), 9);
        profile.packetSizeBytes = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "packet_size_bytes"), 512.0);
        profile.arrivalRatePps = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "arrival_rate_pps"), 1000.0);
        profile.latencyMs = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "latency_ms"), 0.0);
        profile.jitterMs = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "jitter_ms"), 0.0);
        profile.lossRate = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "loss_rate"), 0.0);
        profile.bandwidthDlMbps = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "bandwidth_dl_mbps"), 0.0);
        profile.bandwidthUlMbps = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "bandwidth_ul_mbps"), 0.0);
        profile.guaranteedBandwidthDlMbps = ParseOptionalDouble(
            GetColumnValue(headerIndex, columns, "guaranteed_bandwidth_dl_mbps"),
            0.0);
        profile.guaranteedBandwidthUlMbps = ParseOptionalDouble(
            GetColumnValue(headerIndex, columns, "guaranteed_bandwidth_ul_mbps"),
            0.0);
        profile.priority = ParseOptionalUint(GetColumnValue(headerIndex, columns, "priority"), 0);
        profile.allocatedBandwidthDlMbps = ParseOptionalDouble(
            GetColumnValue(headerIndex, columns, "allocated_bandwidth_dl_mbps"),
            profile.bandwidthDlMbps);
        profile.allocatedBandwidthUlMbps = ParseOptionalDouble(
            GetColumnValue(headerIndex, columns, "allocated_bandwidth_ul_mbps"),
            profile.bandwidthUlMbps);
        profile.optimizeRequested = ParseOptionalBool(
            GetColumnValue(headerIndex, columns, "optimize_requested"),
            false);

        if (profile.flowId.empty())
        {
            continue;
        }
        profiles.push_back(profile);
    }

    return profiles;
}

std::string
BuildSupi(uint32_t index)
{
    std::ostringstream supi;
    supi << "imsi-208930000" << std::setw(6) << std::setfill('0') << index;
    return supi.str();
}

struct SnapshotContext
{
    std::string runId;
    std::string scenarioId;
    std::string outputFile;
    uint32_t tickMs;
    uint32_t tickIndex = 0;
    uint32_t gNbNum;
    uint32_t ueNum;
    std::vector<std::string> upfNames;
    std::vector<std::string> sliceSds;
    std::vector<uint32_t> gnbToUpf;
    std::vector<std::string> ueSliceIds;
    std::vector<Ipv4Address> ueIps;
    std::vector<uint32_t> ueToGnb;
    std::vector<std::string> ueSupis;
    std::vector<uint16_t> uePorts;
    std::map<uint16_t, FlowProfile> flowProfileByPort;
    Ptr<FlowMonitor> monitor;
    Ptr<Ipv4FlowClassifier> classifier;
    Time appStartTime;
    Time simTime;
};

void
EmitSnapshot(SnapshotContext* context)
{
    context->monitor->CheckForLostPackets();
    const auto stats = context->monitor->GetFlowStats();
    const double elapsedSeconds =
        std::max(0.001, (Simulator::Now() - context->appStartTime).GetSeconds());

    std::map<std::string, uint32_t> ipToUeIndex;
    for (uint32_t index = 0; index < context->ueIps.size(); ++index)
    {
        ipToUeIndex[ToString(context->ueIps[index])] = index;
    }

    double totalThroughputDl = 0.0;
    double totalDelayMs = 0.0;
    double totalLossRate = 0.0;
    uint32_t activeFlows = 0;

    std::ostringstream json;
    json << "{";
    json << Quote("run_id") << ":" << Quote(context->runId) << ",";
    json << Quote("scenario_id") << ":" << Quote(context->scenarioId) << ",";
    json << Quote("tick_index") << ":" << context->tickIndex << ",";
    json << Quote("sim_time_ms") << ":" << Simulator::Now().GetMilliSeconds() << ",";

    json << Quote("nodes") << ":[";
    bool first = true;
    for (uint32_t gnb = 0; gnb < context->gNbNum; ++gnb)
    {
        if (!first)
        {
            json << ",";
        }
        first = false;
        json << "{" << Quote("id") << ":" << Quote("ran-node-" + std::to_string(gnb + 1)) << ","
             << Quote("type") << ":" << Quote("ran_node") << ","
             << Quote("label") << ":" << Quote("gNB-" + std::to_string(gnb + 1)) << ","
             << Quote("attributes") << ":{" << Quote("alias") << ":"
             << Quote("gnb-" + std::to_string(gnb + 1)) << "}}";
    }
    for (uint32_t ue = 0; ue < context->ueNum; ++ue)
    {
        json << ",{" << Quote("id") << ":" << Quote("ue-node-" + std::to_string(ue + 1)) << ","
             << Quote("type") << ":" << Quote("ue") << ","
             << Quote("label") << ":" << Quote("UE-" + std::to_string(ue + 1)) << ","
             << Quote("attributes") << ":{" << Quote("supi") << ":"
             << Quote(context->ueSupis[ue]) << "}}";
    }
    for (uint32_t upf = 0; upf < context->upfNames.size(); ++upf)
    {
        json << ",{" << Quote("id") << ":" << Quote("core-node-" + std::to_string(upf + 1)) << ","
             << Quote("type") << ":" << Quote("core_node") << ","
             << Quote("label") << ":" << Quote(context->upfNames[upf]) << ","
             << Quote("attributes") << ":{" << Quote("role") << ":"
             << Quote("upf") << "}}";
    }
    json << "],";

    json << Quote("links") << ":[";
    first = true;
    for (uint32_t ue = 0; ue < context->ueNum; ++ue)
    {
        if (!first)
        {
            json << ",";
        }
        first = false;
        const auto gnbIndex = context->ueToGnb[ue] + 1;
        json << "{" << Quote("source") << ":" << Quote("ue-node-" + std::to_string(ue + 1)) << ","
             << Quote("target") << ":" << Quote("ran-node-" + std::to_string(gnbIndex)) << ","
             << Quote("type") << ":" << Quote("attached_to") << ","
             << Quote("attributes") << ":{}}";

        json << ",{" << Quote("source") << ":" << Quote("ran-node-" + std::to_string(gnbIndex)) << ","
             << Quote("target") << ":"
               << Quote("core-node-" + std::to_string(context->gnbToUpf[context->ueToGnb[ue]] + 1))
             << "," << Quote("type") << ":" << Quote("tunneled_via") << ","
             << Quote("attributes") << ":{}}";
    }
    json << "],";

    json << Quote("gnbs") << ":[";
    for (uint32_t gnb = 0; gnb < context->gNbNum; ++gnb)
    {
        if (gnb > 0)
        {
            json << ",";
        }
        json << "{" << Quote("gnb_id") << ":" << Quote("gnb-" + std::to_string(gnb + 1)) << ","
             << Quote("node_id") << ":" << Quote("ran-node-" + std::to_string(gnb + 1)) << ","
             << Quote("alias") << ":" << Quote("gnb-" + std::to_string(gnb + 1)) << ","
             << Quote("attached_ues") << ":[";
        bool firstUe = true;
        for (uint32_t ue = 0; ue < context->ueNum; ++ue)
        {
            if (context->ueToGnb[ue] != gnb)
            {
                continue;
            }
            if (!firstUe)
            {
                json << ",";
            }
            firstUe = false;
            json << Quote("ue-" + std::to_string(ue + 1));
        }
        json << "]," << Quote("dst_upf") << ":"
               << Quote(context->upfNames[context->gnbToUpf[gnb]]) << "}";
    }
    json << "],";

    json << Quote("ues") << ":[";
    for (uint32_t ue = 0; ue < context->ueNum; ++ue)
    {
        if (ue > 0)
        {
            json << ",";
        }
        const auto defaultSliceId = Quote("slice-1-" + context->sliceSds[ue % context->sliceSds.size()]);
        const auto resolvedSliceId =
            ue < context->ueSliceIds.size() && !context->ueSliceIds[ue].empty()
                ? Quote(context->ueSliceIds[ue])
                : defaultSliceId;
        json << "{" << Quote("ue_id") << ":" << Quote("ue-" + std::to_string(ue + 1)) << ","
             << Quote("supi") << ":" << Quote(context->ueSupis[ue]) << ","
             << Quote("gnb_id") << ":" << Quote("gnb-" + std::to_string(context->ueToGnb[ue] + 1)) << ","
             << Quote("slice_id") << ":" << resolvedSliceId << ","
               << Quote("ip_address") << ":" << Quote(ToString(context->ueIps[ue])) << "}";
    }
    json << "],";

    json << Quote("flows") << ":[";
    bool firstFlow = true;
    for (const auto& [flowId, flow] : stats)
    {
        Ipv4FlowClassifier::FiveTuple tuple = context->classifier->FindFlow(flowId);
        auto ueIt = ipToUeIndex.find(ToString(tuple.destinationAddress));
        if (ueIt == ipToUeIndex.end())
        {
            continue;
        }
        const uint32_t ueIndex = ueIt->second;
        const uint32_t gnbIndex = context->ueToGnb[ueIndex];
        const uint32_t upfIndex = context->gnbToUpf[gnbIndex];
        const uint32_t sliceIndex = ueIndex % context->sliceSds.size();
        const auto profileIt = context->flowProfileByPort.find(tuple.destinationPort);
        const FlowProfile* profile = profileIt != context->flowProfileByPort.end() ? &profileIt->second : nullptr;
        const double delayMs = flow.rxPackets > 0 ? 1000.0 * flow.delaySum.GetSeconds() / flow.rxPackets : 0.0;
        const double jitterMs = flow.rxPackets > 0 ? 1000.0 * flow.jitterSum.GetSeconds() / flow.rxPackets : 0.0;
        const double lossRate = flow.txPackets > 0
                                    ? static_cast<double>(flow.txPackets - flow.rxPackets) /
                                          static_cast<double>(flow.txPackets)
                                    : 0.0;
        const double throughputDl = flow.rxBytes * 8.0 / elapsedSeconds / 1e6;
        const std::string flowIdentifier =
            profile != nullptr && !profile->flowId.empty() ? profile->flowId : "flow-" + std::to_string(flowId);
        const std::string flowName =
            profile != nullptr && !profile->flowName.empty() ? profile->flowName : flowIdentifier;
        const std::string appId =
            profile != nullptr && !profile->appId.empty() ? profile->appId : "dl-app-" + std::to_string(ueIndex + 1);
        const std::string appName =
            profile != nullptr && !profile->appName.empty() ? profile->appName : appId;
        const std::string sliceId =
            profile != nullptr && !profile->sliceRef.empty()
                ? profile->sliceRef
                : (ueIndex < context->ueSliceIds.size() && !context->ueSliceIds[ueIndex].empty()
                       ? context->ueSliceIds[ueIndex]
                       : "slice-1-" + context->sliceSds[sliceIndex]);
        const std::string sliceSnssai =
            profile != nullptr && !profile->sliceSnssai.empty()
                ? profile->sliceSnssai
                : "01" + context->sliceSds[sliceIndex];
        const uint32_t fiveQi = profile != nullptr ? profile->fiveQi : 9;
        const double packetSizeBytes = profile != nullptr ? profile->packetSizeBytes : 512.0;
        const double arrivalRatePps = profile != nullptr ? profile->arrivalRatePps : 1000.0;
        const double targetLatencyMs = profile != nullptr ? profile->latencyMs : delayMs;
        const double targetJitterMs = profile != nullptr ? profile->jitterMs : jitterMs;
        const double targetLossRate = profile != nullptr ? profile->lossRate : lossRate;
        const double targetBandwidthDlMbps = profile != nullptr ? profile->bandwidthDlMbps : throughputDl;
        const double targetBandwidthUlMbps = profile != nullptr ? profile->bandwidthUlMbps : 0.0;
        const double guaranteedBandwidthDlMbps =
            profile != nullptr ? profile->guaranteedBandwidthDlMbps : targetBandwidthDlMbps;
        const double guaranteedBandwidthUlMbps =
            profile != nullptr ? profile->guaranteedBandwidthUlMbps : targetBandwidthUlMbps;
        const double allocatedBandwidthDlMbps =
            profile != nullptr ? profile->allocatedBandwidthDlMbps : targetBandwidthDlMbps;
        const double allocatedBandwidthUlMbps =
            profile != nullptr ? profile->allocatedBandwidthUlMbps : targetBandwidthUlMbps;
        const bool optimizeRequested = profile != nullptr ? profile->optimizeRequested : false;
        const std::string serviceType =
            profile != nullptr && !profile->serviceType.empty() ? profile->serviceType : "eMBB";
        const uint32_t serviceTypeId = profile != nullptr ? profile->serviceTypeId : 1;
        const uint32_t priority = profile != nullptr ? profile->priority : 0;

        totalThroughputDl += throughputDl;
        totalDelayMs += delayMs;
        totalLossRate += lossRate;
        activeFlows++;

        if (!firstFlow)
        {
            json << ",";
        }
        firstFlow = false;
        json << "{" << Quote("flow_id") << ":" << Quote(flowIdentifier) << ","
               << Quote("name") << ":" << Quote(flowName) << ","
             << Quote("supi") << ":" << Quote(context->ueSupis[ueIndex]) << ","
               << Quote("app_id") << ":" << Quote(appId) << ","
               << Quote("app_name") << ":" << Quote(appName) << ","
             << Quote("src_gnb") << ":" << Quote("gnb-" + std::to_string(gnbIndex + 1)) << ","
             << Quote("dst_upf") << ":" << Quote(context->upfNames[upfIndex]) << ","
               << Quote("slice_id") << ":" << Quote(sliceId) << ","
               << Quote("5qi") << ":" << fiveQi << ","
             << Quote("delay_ms") << ":" << delayMs << ","
             << Quote("jitter_ms") << ":" << jitterMs << ","
             << Quote("loss_rate") << ":" << lossRate << ","
             << Quote("throughput_ul_mbps") << ":0,"
             << Quote("throughput_dl_mbps") << ":" << throughputDl << ","
             << Quote("queue_bytes") << ":0,"
               << Quote("rlc_buffer_bytes") << ":0,"
               << Quote("service") << ":{" << Quote("service_type") << ":" << Quote(serviceType) << ","
               << Quote("service_type_id") << ":" << serviceTypeId << "},"
               << Quote("traffic") << ":{" << Quote("five_tuple") << ":{" << Quote("protocol") << ":"
               << tuple.protocol << "," << Quote("source_ip") << ":" << Quote(ToString(tuple.sourceAddress)) << ","
               << Quote("source_port") << ":" << tuple.sourcePort << "," << Quote("destination_ip") << ":"
               << Quote(ToString(tuple.destinationAddress)) << "," << Quote("destination_port") << ":"
               << tuple.destinationPort << "}," << Quote("packet_size") << ":" << packetSizeBytes << ","
               << Quote("arrival_rate") << ":" << arrivalRatePps << "},"
               << Quote("sla") << ":{" << Quote("latency") << ":" << targetLatencyMs << ","
               << Quote("jitter") << ":" << targetJitterMs << "," << Quote("priority") << ":"
               << priority << "," << Quote("loss_rate") << ":" << targetLossRate << ","
               << Quote("bandwidth_dl") << ":" << targetBandwidthDlMbps << ","
               << Quote("bandwidth_ul") << ":" << targetBandwidthUlMbps << ","
               << Quote("guaranteed_bandwidth_dl") << ":" << guaranteedBandwidthDlMbps << ","
               << Quote("guaranteed_bandwidth_ul") << ":" << guaranteedBandwidthUlMbps << "},"
               << Quote("telemetry") << ":{" << Quote("latency") << ":" << delayMs << ","
               << Quote("jitter") << ":" << jitterMs << "," << Quote("loss_rate") << ":" << lossRate << ","
               << Quote("packet_sent") << ":" << flow.txPackets << "," << Quote("packet_received") << ":"
               << flow.rxPackets << "," << Quote("throughput_dl") << ":" << throughputDl << ","
               << Quote("throughput_ul") << ":0},"
               << Quote("allocation") << ":{" << Quote("optimize_requested") << ":"
               << (optimizeRequested ? "true" : "false") << ","
               << Quote("current_slice_snssai") << ":" << Quote(sliceSnssai) << ","
               << Quote("allocated_bandwidth_dl") << ":" << allocatedBandwidthDlMbps << ","
               << Quote("allocated_bandwidth_ul") << ":" << allocatedBandwidthUlMbps << "}}"
             ;
    }
    json << "],";

    json << Quote("slices") << ":[";
    for (uint32_t index = 0; index < context->sliceSds.size(); ++index)
    {
        if (index > 0)
        {
            json << ",";
        }
        json << "{" << Quote("slice_id") << ":" << Quote("slice-1-" + context->sliceSds[index]) << ","
             << Quote("sst") << ":1,"
             << Quote("sd") << ":" << Quote(context->sliceSds[index]) << ","
             << Quote("label") << ":" << Quote("slice-" + std::to_string(index + 1)) << "}";
    }
    json << "],";

    const double meanDelay = activeFlows > 0 ? totalDelayMs / activeFlows : 0.0;
    const double meanLoss = activeFlows > 0 ? totalLossRate / activeFlows : 0.0;
    json << Quote("kpis") << ":{" << Quote("active_flows") << ":" << activeFlows << ","
         << Quote("throughput_dl_mbps_total") << ":" << totalThroughputDl << ","
         << Quote("mean_delay_ms") << ":" << meanDelay << ","
         << Quote("mean_loss_rate") << ":" << meanLoss << "},";

    json << Quote("reward_inputs") << ":{" << Quote("throughput_score") << ":" << totalThroughputDl << ","
         << Quote("delay_penalty") << ":" << meanDelay << ","
         << Quote("loss_penalty") << ":" << meanLoss << "}";
    json << "}";

    std::ofstream output(context->outputFile, std::ios::app);
    output << json.str() << std::endl;

    context->tickIndex++;
    const auto nextTick = Simulator::Now() + MilliSeconds(context->tickMs);
    if (nextTick <= context->simTime)
    {
        Simulator::Schedule(MilliSeconds(context->tickMs), &EmitSnapshot, context);
    }
}

} // namespace

int
main(int argc, char* argv[])
{
    uint16_t gNbNum = 1;
    uint16_t ueNum = 0;
    uint16_t ueNumPerGnb = 1;
    uint32_t tickMs = 1000;
    uint32_t simTimeMs = 30000;
    std::string runId = "run-local";
    std::string scenarioId = "scenario-local";
    std::string outputFile = "./tick-snapshots.jsonl";
    std::string flowProfileFile;
    std::string upfNamesCsv = "upf";
    std::string sliceSdsCsv = "010203";
    std::string ueSupisCsv;
    std::string ueGnbMapCsv;
    std::string gnbUpfMapCsv;
    std::string gnbPositionsArg;
    std::string uePositionsArg;

    CommandLine cmd(__FILE__);
    cmd.AddValue("gNbNum", "Number of gNBs", gNbNum);
    cmd.AddValue("ueNum", "Total number of UEs", ueNum);
    cmd.AddValue("ueNumPerGnb", "Number of UEs per gNB", ueNumPerGnb);
    cmd.AddValue("tickMs", "Tick interval in milliseconds", tickMs);
    cmd.AddValue("simTimeMs", "Simulation time in milliseconds", simTimeMs);
    cmd.AddValue("runId", "Run identifier", runId);
    cmd.AddValue("scenarioId", "Scenario identifier", scenarioId);
    cmd.AddValue("outputFile", "Snapshot JSONL output path", outputFile);
    cmd.AddValue("flowProfileFile", "TSV file describing scenario app/flow profiles", flowProfileFile);
    cmd.AddValue("upfNames", "Comma separated UPF names", upfNamesCsv);
    cmd.AddValue("sliceSds", "Comma separated slice SD list", sliceSdsCsv);
    cmd.AddValue("ueSupis", "Comma separated UE SUPI list", ueSupisCsv);
    cmd.AddValue("ueGnbMap", "Comma separated 1-based gNB index for each UE", ueGnbMapCsv);
    cmd.AddValue("gnbUpfMap", "Comma separated 1-based UPF index for each gNB", gnbUpfMapCsv);
    cmd.AddValue("gnbPositions", "Semicolon separated x:y:z gNB positions or auto", gnbPositionsArg);
    cmd.AddValue("uePositions", "Semicolon separated x:y:z UE positions or auto", uePositionsArg);
    cmd.Parse(argc, argv);

    std::filesystem::create_directories(std::filesystem::path(outputFile).parent_path());
    std::ofstream(outputFile, std::ios::trunc).close();

    const uint32_t resolvedUeNum = ueNum > 0 ? ueNum : gNbNum * ueNumPerGnb;
    const auto upfNames = SplitCsv(upfNamesCsv);
    const auto sliceSds = SplitCsv(sliceSdsCsv);
    auto ueSupis = ParseStringList(ueSupisCsv, resolvedUeNum, "ueSupis");
    if (ueSupis.empty())
    {
        for (uint32_t index = 0; index < resolvedUeNum; ++index)
        {
            ueSupis.push_back(BuildSupi(index + 1));
        }
    }

    auto ueToGnb = ParseIndexList(ueGnbMapCsv, resolvedUeNum, gNbNum, "ueGnbMap");
    if (ueToGnb.empty())
    {
        for (uint32_t index = 0; index < resolvedUeNum; ++index)
        {
            ueToGnb.push_back((index % gNbNum) + 1);
        }
    }

    auto gnbToUpf = ParseIndexList(gnbUpfMapCsv, gNbNum, upfNames.size(), "gnbUpfMap");
    if (gnbToUpf.empty())
    {
        for (uint32_t index = 0; index < gNbNum; ++index)
        {
            gnbToUpf.push_back((index % upfNames.size()) + 1);
        }
    }

    const auto gnbPositionOverrides = ParsePositionOverrides(gnbPositionsArg, gNbNum, "gnbPositions");
    const auto uePositionOverrides = ParsePositionOverrides(uePositionsArg, resolvedUeNum, "uePositions");
    const auto flowProfiles = LoadFlowProfiles(flowProfileFile);
    std::map<std::string, uint32_t> ueIndexBySupi;
    std::vector<std::string> ueSliceIds(resolvedUeNum);
    for (uint32_t index = 0; index < resolvedUeNum; ++index)
    {
        ueIndexBySupi[ueSupis[index]] = index;
        ueSliceIds[index] = "slice-1-" + sliceSds[index % sliceSds.size()];
    }
    for (const auto& profile : flowProfiles)
    {
        auto it = ueIndexBySupi.find(profile.supi);
        if (it == ueIndexBySupi.end())
        {
            NS_FATAL_ERROR("flow profile references unknown SUPI " << profile.supi);
        }
        if (!profile.sliceRef.empty())
        {
            ueSliceIds[it->second] = profile.sliceRef;
        }
    }

    Time simTime = MilliSeconds(simTimeMs);
    Time appStartTime = MilliSeconds(400);
    uint16_t numerology = 0;
    double centralFrequency = 4e9;
    double bandwidth = 20e6;
    double totalTxPower = 35;
    uint32_t udpPacketSize = 512;
    uint32_t lambda = 1000;

    Config::SetDefault("ns3::NrRlcUm::MaxTxBufferSize", UintegerValue(999999999));

    GridScenarioHelper gridScenario;
    gridScenario.SetRows(1);
    gridScenario.SetColumns(gNbNum);
    gridScenario.SetHorizontalBsDistance(10.0);
    gridScenario.SetVerticalBsDistance(10.0);
    gridScenario.SetBsHeight(10.0);
    gridScenario.SetUtHeight(1.5);
    gridScenario.SetSectorization(GridScenarioHelper::SINGLE);
    gridScenario.SetBsNumber(gNbNum);
    gridScenario.SetUtNumber(resolvedUeNum);
    gridScenario.SetScenarioHeight(3);
    gridScenario.SetScenarioLength(3);
    gridScenario.CreateScenario();
    ApplyPositionOverrides(gridScenario.GetBaseStations(), gnbPositionOverrides);
    ApplyPositionOverrides(gridScenario.GetUserTerminals(), uePositionOverrides);

    Ptr<NrPointToPointEpcHelper> nrEpcHelper = CreateObject<NrPointToPointEpcHelper>();
    Ptr<IdealBeamformingHelper> beamformingHelper = CreateObject<IdealBeamformingHelper>();
    Ptr<NrHelper> nrHelper = CreateObject<NrHelper>();
    nrHelper->SetBeamformingHelper(beamformingHelper);
    nrHelper->SetEpcHelper(nrEpcHelper);
    nrEpcHelper->SetAttribute("S1uLinkDelay", TimeValue(MilliSeconds(0)));

    beamformingHelper->SetAttribute("BeamformingMethod",
                                    TypeIdValue(DirectPathBeamforming::GetTypeId()));
    nrHelper->SetUeAntennaAttribute("NumRows", UintegerValue(1));
    nrHelper->SetUeAntennaAttribute("NumColumns", UintegerValue(1));
    nrHelper->SetUeAntennaAttribute("AntennaElement",
                                    PointerValue(CreateObject<IsotropicAntennaModel>()));
    nrHelper->SetGnbAntennaAttribute("NumRows", UintegerValue(2));
    nrHelper->SetGnbAntennaAttribute("NumColumns", UintegerValue(2));
    nrHelper->SetGnbAntennaAttribute("AntennaElement",
                                     PointerValue(CreateObject<IsotropicAntennaModel>()));

    BandwidthPartInfoPtrVector allBwps;
    CcBwpCreator ccBwpCreator;
    CcBwpCreator::SimpleOperationBandConf bandConf(centralFrequency, bandwidth, 1);
    bandConf.m_numBwp = 1;
    auto band = ccBwpCreator.CreateOperationBandContiguousCc(bandConf);
    Ptr<NrChannelHelper> channelHelper = CreateObject<NrChannelHelper>();
    channelHelper->ConfigureFactories("UMi", "Default", "ThreeGpp");
    channelHelper->SetPathlossAttribute("ShadowingEnabled", BooleanValue(false));
    channelHelper->SetChannelConditionModelAttribute("UpdatePeriod", TimeValue(MilliSeconds(0)));
    channelHelper->AssignChannelsToBands({band});
    allBwps = CcBwpCreator::GetAllBwps({band});

    Packet::EnableChecking();
    Packet::EnablePrinting();

    NetDeviceContainer gnbNetDev =
        nrHelper->InstallGnbDevice(gridScenario.GetBaseStations(), allBwps);
    NetDeviceContainer ueNetDev =
        nrHelper->InstallUeDevice(gridScenario.GetUserTerminals(), allBwps);

    double x = std::pow(10, totalTxPower / 10.0);
    for (uint32_t index = 0; index < gnbNetDev.GetN(); ++index)
    {
        NrHelper::GetGnbPhy(gnbNetDev.Get(index), 0)
            ->SetAttribute("Numerology", UintegerValue(numerology));
        NrHelper::GetGnbPhy(gnbNetDev.Get(index), 0)->SetAttribute("TxPower", DoubleValue(10 * std::log10(x)));
    }

    auto [remoteHost, remoteHostAddress] = nrEpcHelper->SetupRemoteHost("100Gb/s", 2500, Seconds(0.000));

    InternetStackHelper internet;
    internet.Install(gridScenario.GetUserTerminals());
    Ipv4InterfaceContainer ueIpIfaces = nrEpcHelper->AssignUeIpv4Address(NetDeviceContainer(ueNetDev));
    for (uint32_t index = 0; index < resolvedUeNum; ++index)
    {
        nrHelper->AttachToGnb(ueNetDev.Get(index), gnbNetDev.Get(ueToGnb[index] - 1));
    }

    ApplicationContainer serverApps;
    ApplicationContainer clientApps;
    std::map<uint16_t, FlowProfile> flowProfileByPort;

    if (!flowProfiles.empty())
    {
        for (uint32_t index = 0; index < flowProfiles.size(); ++index)
        {
            const auto& profile = flowProfiles[index];
            auto ueIt = ueIndexBySupi.find(profile.supi);
            if (ueIt == ueIndexBySupi.end())
            {
                NS_FATAL_ERROR("flow profile references unknown SUPI " << profile.supi);
            }
            const uint32_t ueIndex = ueIt->second;
            const uint16_t dlPort = static_cast<uint16_t>(5000 + index);
            UdpServerHelper packetSink(dlPort);
            serverApps.Add(packetSink.Install(gridScenario.GetUserTerminals().Get(ueIndex)));

            UdpClientHelper client;
            client.SetAttribute("MaxPackets", UintegerValue(0xFFFFFFFF));
            client.SetAttribute(
                "PacketSize",
                UintegerValue(static_cast<uint32_t>(std::max(64.0, profile.packetSizeBytes))));
            const double intervalRate = profile.arrivalRatePps > 0.0 ? profile.arrivalRatePps : lambda;
            client.SetAttribute("Interval", TimeValue(Seconds(1.0 / intervalRate)));
            client.SetAttribute(
                "Remote",
                AddressValue(addressUtils::ConvertToSocketAddress(ueIpIfaces.GetAddress(ueIndex), dlPort)));
            clientApps.Add(client.Install(remoteHost));

            NrEpsBearer bearer(NrEpsBearer::NGBR_LOW_LAT_EMBB);
            Ptr<NrEpcTft> tft = Create<NrEpcTft>();
            NrEpcTft::PacketFilter filter;
            filter.localPortStart = dlPort;
            filter.localPortEnd = dlPort;
            tft->Add(filter);
            nrHelper->ActivateDedicatedEpsBearer(ueNetDev.Get(ueIndex), bearer, tft);
            flowProfileByPort[dlPort] = profile;
        }
    }
    else
    {
        for (uint32_t index = 0; index < resolvedUeNum; ++index)
        {
            const uint16_t dlPort = static_cast<uint16_t>(5000 + index);
            UdpServerHelper packetSink(dlPort);
            serverApps.Add(packetSink.Install(gridScenario.GetUserTerminals().Get(index)));

            UdpClientHelper client;
            client.SetAttribute("MaxPackets", UintegerValue(0xFFFFFFFF));
            client.SetAttribute("PacketSize", UintegerValue(udpPacketSize));
            client.SetAttribute("Interval", TimeValue(Seconds(1.0 / lambda)));
            client.SetAttribute(
                "Remote",
                AddressValue(addressUtils::ConvertToSocketAddress(ueIpIfaces.GetAddress(index), dlPort)));
            clientApps.Add(client.Install(remoteHost));

            NrEpsBearer bearer(NrEpsBearer::NGBR_LOW_LAT_EMBB);
            Ptr<NrEpcTft> tft = Create<NrEpcTft>();
            NrEpcTft::PacketFilter filter;
            filter.localPortStart = dlPort;
            filter.localPortEnd = dlPort;
            tft->Add(filter);
            nrHelper->ActivateDedicatedEpsBearer(ueNetDev.Get(index), bearer, tft);
        }
    }

    serverApps.Start(appStartTime);
    clientApps.Start(appStartTime);
    serverApps.Stop(simTime);
    clientApps.Stop(simTime);

    FlowMonitorHelper flowMonitorHelper;
    NodeContainer monitored;
    monitored.Add(remoteHost);
    monitored.Add(gridScenario.GetUserTerminals());

    Ptr<FlowMonitor> monitor = flowMonitorHelper.Install(monitored);
    monitor->SetAttribute("DelayBinWidth", DoubleValue(0.001));
    monitor->SetAttribute("JitterBinWidth", DoubleValue(0.001));
    monitor->SetAttribute("PacketSizeBinWidth", DoubleValue(20));
    Ptr<Ipv4FlowClassifier> classifier = DynamicCast<Ipv4FlowClassifier>(flowMonitorHelper.GetClassifier());

    SnapshotContext context;
    context.runId = runId;
    context.scenarioId = scenarioId;
    context.outputFile = outputFile;
    context.tickMs = tickMs;
    context.gNbNum = gNbNum;
    context.ueNum = resolvedUeNum;
    context.upfNames = upfNames;
    context.sliceSds = sliceSds;
    context.ueSliceIds = ueSliceIds;
    context.gnbToUpf.reserve(gnbToUpf.size());
    for (const auto value : gnbToUpf)
    {
        context.gnbToUpf.push_back(value - 1);
    }
    context.monitor = monitor;
    context.classifier = classifier;
    context.appStartTime = appStartTime;
    context.simTime = simTime;
    context.flowProfileByPort = flowProfileByPort;

    for (uint32_t index = 0; index < resolvedUeNum; ++index)
    {
        context.ueIps.push_back(ueIpIfaces.GetAddress(index));
        context.ueToGnb.push_back(ueToGnb[index] - 1);
        context.ueSupis.push_back(ueSupis[index]);
        context.uePorts.push_back(static_cast<uint16_t>(5000 + index));
    }

    Simulator::Schedule(MilliSeconds(tickMs), &EmitSnapshot, &context);
    Simulator::Stop(simTime);
    Simulator::Run();
    Simulator::Destroy();
    return 0;
}