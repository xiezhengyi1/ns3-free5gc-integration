// SPDX-License-Identifier: GPL-2.0-only

#include "ns3/antenna-module.h"
#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/csma-module.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/internet-apps-module.h"
#include "ns3/internet-module.h"
#include "ns3/mobility-module.h"
#include "ns3/nr-module.h"
#include "ns3/error-model.h"
#include "ns3/ethernet-header.h"
#include "ns3/point-to-point-module.h"
#include "ns3/tap-bridge-module.h"
#include "ns3/udp-header.h"

#include <algorithm>
#include <cmath>
#include <deque>
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
    std::string sessionRef;
    std::string sliceRef;
    std::string sliceSnssai;
    std::string dnn;
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
    std::string policyFilter;
    uint32_t precedence = 128;
    uint32_t qosRef = 0;
    std::string chargingMethod;
    std::string quota;
    std::string unitCost;
};

struct SliceResourceProfile
{
    std::string sliceId;
    std::string sliceSnssai;
    double capacityDlMbps = 0.0;
    double capacityUlMbps = 0.0;
    double guaranteedDlMbps = 0.0;
    double guaranteedUlMbps = 0.0;
    uint32_t priority = 1;
    double latencyMs = 0.0;
    double jitterMs = 0.0;
    double lossRate = 0.0;
    double processingDelayMs = 0.0;
};

struct SliceRuntimeTelemetry
{
    double capacityDlMbps = 0.0;
    double capacityUlMbps = 0.0;
    double guaranteedDlMbps = 0.0;
    double guaranteedUlMbps = 0.0;
    double demandDlMbps = 0.0;
    double demandUlMbps = 0.0;
    double allocatedDlMbps = 0.0;
    double allocatedUlMbps = 0.0;
    double queueBytes = 0.0;
    double droppedPackets = 0.0;
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
    try
    {
        return static_cast<uint32_t>(std::stoul(value));
    }
    catch (const std::exception&)
    {
        return fallback;
    }
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
        profile.sessionRef = GetColumnValue(headerIndex, columns, "session_ref");
        profile.sliceRef = GetColumnValue(headerIndex, columns, "slice_ref");
        profile.sliceSnssai = GetColumnValue(headerIndex, columns, "slice_snssai");
        profile.dnn = GetColumnValue(headerIndex, columns, "dnn");
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
        profile.policyFilter = GetColumnValue(headerIndex, columns, "policy_filter");
        profile.precedence = ParseOptionalUint(GetColumnValue(headerIndex, columns, "precedence"), 128);
        profile.qosRef = ParseOptionalUint(GetColumnValue(headerIndex, columns, "qos_ref"), 0);
        profile.chargingMethod = GetColumnValue(headerIndex, columns, "charging_method");
        profile.quota = GetColumnValue(headerIndex, columns, "quota");
        profile.unitCost = GetColumnValue(headerIndex, columns, "unit_cost");

        if (profile.flowId.empty())
        {
            continue;
        }
        profiles.push_back(profile);
    }

    return profiles;
}

std::map<std::string, SliceResourceProfile>
LoadSliceResources(const std::string& path)
{
    std::map<std::string, SliceResourceProfile> resources;
    if (path.empty())
    {
        return resources;
    }

    std::ifstream input(path);
    if (!input.is_open())
    {
        NS_FATAL_ERROR("failed to open slice resource file: " << path);
    }

    std::string headerLine;
    if (!std::getline(input, headerLine))
    {
        return resources;
    }
    auto headerColumns = SplitString(headerLine, '\t', true);
    std::map<std::string, uint32_t> headerIndex;
    for (uint32_t index = 0; index < headerColumns.size(); ++index)
    {
        headerIndex[headerColumns[index]] = index;
    }

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

        SliceResourceProfile profile;
        profile.sliceId = GetColumnValue(headerIndex, columns, "slice_ref");
        profile.sliceSnssai = GetColumnValue(headerIndex, columns, "slice_snssai");
        profile.capacityDlMbps = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "capacity_dl_mbps"), 0.0);
        profile.capacityUlMbps = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "capacity_ul_mbps"), 0.0);
        profile.guaranteedDlMbps = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "guaranteed_dl_mbps"), 0.0);
        profile.guaranteedUlMbps = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "guaranteed_ul_mbps"), 0.0);
        profile.priority = ParseOptionalUint(GetColumnValue(headerIndex, columns, "priority"), 1);
        profile.latencyMs = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "latency_ms"), 0.0);
        profile.jitterMs = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "jitter_ms"), 0.0);
        profile.lossRate = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "loss_rate"), 0.0);
        profile.processingDelayMs = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "processing_delay_ms"), 0.0);
        if (profile.sliceId.empty() || profile.capacityDlMbps <= 0.0 || profile.capacityUlMbps <= 0.0)
        {
            NS_FATAL_ERROR("invalid slice resource row in " << path);
        }
        resources[profile.sliceId] = profile;
    }
    return resources;
}

std::string
BuildSupi(uint32_t index)
{
    std::ostringstream supi;
    supi << "imsi-208930000" << std::setw(6) << std::setfill('0') << index;
    return supi.str();
}

bool
ParseSliceId(const std::string& sliceId, uint32_t* sst, std::string* sd)
{
    if (sliceId.rfind("slice-", 0) != 0)
    {
        return false;
    }

    const auto remainder = sliceId.substr(6);
    const auto separator = remainder.find('-');
    if (separator == std::string::npos)
    {
        return false;
    }

    const auto sstValue = remainder.substr(0, separator);
    const auto sdValue = remainder.substr(separator + 1);
    if (sstValue.empty() || sdValue.empty())
    {
        return false;
    }

    try
    {
        *sst = static_cast<uint32_t>(std::stoul(sstValue));
    }
    catch (const std::exception&)
    {
        return false;
    }
    *sd = sdValue;
    return true;
}

std::string
BuildSliceSnssai(const std::string& sliceId, const std::string& fallbackSd)
{
    uint32_t sst = 1;
    std::string sd = fallbackSd;
    if (!ParseSliceId(sliceId, &sst, &sd))
    {
        return "01" + fallbackSd;
    }

    std::ostringstream stream;
    stream << std::setw(2) << std::setfill('0') << sst << sd;
    return stream.str();
}

std::string
BuildDefaultSliceId(const std::vector<std::string>& sliceIds,
                    const std::vector<std::string>& sliceSds,
                    uint32_t index)
{
    if (!sliceIds.empty())
    {
        return sliceIds[index % sliceIds.size()];
    }
    return "slice-1-" + sliceSds[index % sliceSds.size()];
}

void
AppendUniqueString(std::vector<std::string>* values, const std::string& value)
{
    if (value.empty())
    {
        return;
    }
    if (std::find(values->begin(), values->end(), value) == values->end())
    {
        values->push_back(value);
    }
}

struct SnapshotContext
{
    struct ExternalFlowCounters
    {
        uint64_t txPacketsUl = 0;
        uint64_t rxPacketsUl = 0;
        uint64_t dropPacketsUl = 0;
        uint64_t txPacketsDl = 0;
        uint64_t rxPacketsDl = 0;
        uint64_t dropPacketsDl = 0;
        double delaySumMsUl = 0.0;
        double delaySumMsDl = 0.0;
        uint64_t delaySamplesUl = 0;
        uint64_t delaySamplesDl = 0;
        double jitterSumMsUl = 0.0;
        double jitterSumMsDl = 0.0;
        double lastDelayMsUl = 0.0;
        double lastDelayMsDl = 0.0;
        bool hasLastDelayUl = false;
        bool hasLastDelayDl = false;
        std::deque<Time> txTimesUl;
        std::deque<Time> txTimesDl;
    };

    struct FlowRuntimeState
    {
        FlowProfile profile;
        Ptr<UdpClient> client;
        uint16_t port = 0;
        uint32_t ueIndex = 0;
    };

    std::string runId;
    std::string scenarioId;
    std::string outputFile;
    std::string clockFile;
    std::string flowProfileFile;
    std::string sliceResourceFile;
    uint32_t tickMs;
    uint32_t policyReloadMs = 1000;
    uint32_t tickIndex = 0;
    uint32_t gNbNum;
    uint32_t ueNum;
    double bridgeLinkRateMbps = 1000.0;
    double bridgeLinkDelayMs = 1.0;
    double bridgeLinkLossRate = 0.0;
    bool externalTrafficOnly = false;
    std::string externalTrafficTargetIp = "8.8.8.8";
    uint32_t externalTrafficSourceBasePort = 15000;
    std::vector<std::string> upfNames;
    std::vector<std::string> sliceSds;
    std::vector<std::string> sliceIds;
    std::vector<uint32_t> gnbToUpf;
    std::vector<std::string> ueSliceIds;
    std::vector<Ipv4Address> ueIps;
    std::vector<uint32_t> ueToGnb;
    std::vector<std::string> ueSupis;
    std::vector<uint16_t> uePorts;
    std::map<uint16_t, FlowRuntimeState> flowRuntimeByPort;
    std::map<uint16_t, ExternalFlowCounters> externalFlowCountersByPort;
    std::map<std::string, SliceResourceProfile> sliceResources;
    std::map<std::string, SliceRuntimeTelemetry> sliceTelemetry;
    Ptr<FlowMonitor> monitor;
    Ptr<Ipv4FlowClassifier> classifier;
    Time appStartTime;
    Time simTime;
};

bool
ExtractExternalFlowKey(const SnapshotContext* context,
                       Ptr<const Packet> packet,
                       uint16_t* port,
                       bool* uplink)
{
    NS_ASSERT(context != nullptr);
    NS_ASSERT(port != nullptr);
    NS_ASSERT(uplink != nullptr);

    EthernetHeader ethernetHeader;
    Ptr<Packet> packetCopy = packet->Copy();
    if (!packetCopy->RemoveHeader(ethernetHeader))
    {
        return false;
    }
    if (ethernetHeader.GetLengthType() != 0x0800)
    {
        return false;
    }

    Ipv4Header ipv4Header;
    if (!packetCopy->RemoveHeader(ipv4Header))
    {
        return false;
    }
    if (ipv4Header.GetProtocol() != 17)
    {
        return false;
    }

    UdpHeader udpHeader;
    if (!packetCopy->PeekHeader(udpHeader))
    {
        return false;
    }

    const uint16_t destinationPort = udpHeader.GetDestinationPort();
    const uint16_t sourcePort = udpHeader.GetSourcePort();
    if (context->flowRuntimeByPort.find(destinationPort) != context->flowRuntimeByPort.end())
    {
        *port = destinationPort;
        *uplink = true;
        return true;
    }
    if (context->flowRuntimeByPort.find(sourcePort) != context->flowRuntimeByPort.end())
    {
        *port = sourcePort;
        *uplink = false;
        return true;
    }
    return false;
}

void
OnBridgeMacTx(SnapshotContext* context, bool gnbSide, Ptr<const Packet> packet)
{
    uint16_t port = 0;
    bool uplink = false;
    if (!ExtractExternalFlowKey(context, packet, &port, &uplink))
    {
        return;
    }
    auto& counters = context->externalFlowCountersByPort[port];
    if (gnbSide && uplink)
    {
        counters.txPacketsUl++;
        counters.txTimesUl.push_back(Simulator::Now());
    }
    else if (!gnbSide && !uplink)
    {
        counters.txPacketsDl++;
        counters.txTimesDl.push_back(Simulator::Now());
    }
}

void
OnBridgeMacRx(SnapshotContext* context, bool gnbSide, Ptr<const Packet> packet)
{
    uint16_t port = 0;
    bool uplink = false;
    if (!ExtractExternalFlowKey(context, packet, &port, &uplink))
    {
        return;
    }
    auto& counters = context->externalFlowCountersByPort[port];
    if (!gnbSide && uplink)
    {
        counters.rxPacketsUl++;
        if (!counters.txTimesUl.empty())
        {
            const double delayMs = (Simulator::Now() - counters.txTimesUl.front()).GetSeconds() * 1000.0;
            counters.txTimesUl.pop_front();
            counters.delaySumMsUl += delayMs;
            counters.delaySamplesUl++;
            if (counters.hasLastDelayUl)
            {
                counters.jitterSumMsUl += std::abs(delayMs - counters.lastDelayMsUl);
            }
            counters.lastDelayMsUl = delayMs;
            counters.hasLastDelayUl = true;
        }
    }
    else if (gnbSide && !uplink)
    {
        counters.rxPacketsDl++;
        if (!counters.txTimesDl.empty())
        {
            const double delayMs = (Simulator::Now() - counters.txTimesDl.front()).GetSeconds() * 1000.0;
            counters.txTimesDl.pop_front();
            counters.delaySumMsDl += delayMs;
            counters.delaySamplesDl++;
            if (counters.hasLastDelayDl)
            {
                counters.jitterSumMsDl += std::abs(delayMs - counters.lastDelayMsDl);
            }
            counters.lastDelayMsDl = delayMs;
            counters.hasLastDelayDl = true;
        }
    }
}

void
OnBridgeMacRxDrop(SnapshotContext* context, bool gnbSide, Ptr<const Packet> packet)
{
    uint16_t port = 0;
    bool uplink = false;
    if (!ExtractExternalFlowKey(context, packet, &port, &uplink))
    {
        return;
    }
    auto& counters = context->externalFlowCountersByPort[port];
    if (!gnbSide && uplink)
    {
        counters.dropPacketsUl++;
        if (!counters.txTimesUl.empty())
        {
            counters.txTimesUl.pop_front();
        }
    }
    else if (gnbSide && !uplink)
    {
        counters.dropPacketsDl++;
        if (!counters.txTimesDl.empty())
        {
            counters.txTimesDl.pop_front();
        }
    }
}

std::string
NormalizeSimulatorType(const std::string& simulator)
{
    if (simulator.rfind("ns3::", 0) == 0)
    {
        return simulator;
    }
    return "ns3::" + simulator;
}

double
RequestedBandwidthDlMbps(const FlowProfile& profile)
{
    if (profile.bandwidthDlMbps > 0.0)
    {
        return profile.bandwidthDlMbps;
    }
    if (profile.packetSizeBytes <= 0.0 || profile.arrivalRatePps <= 0.0)
    {
        return 0.0;
    }
    return profile.arrivalRatePps * profile.packetSizeBytes * 8.0 / 1e6;
}

double
RequestedBandwidthUlMbps(const FlowProfile& profile)
{
    if (profile.bandwidthUlMbps > 0.0)
    {
        return profile.bandwidthUlMbps;
    }
    if (profile.packetSizeBytes <= 0.0 || profile.arrivalRatePps <= 0.0)
    {
        return 0.0;
    }
    return profile.arrivalRatePps * profile.packetSizeBytes * 8.0 / 1e6;
}

double
GuaranteedBandwidthDlMbps(const FlowProfile& profile)
{
    return std::max(0.0, profile.guaranteedBandwidthDlMbps);
}

double
GuaranteedBandwidthUlMbps(const FlowProfile& profile)
{
    return std::max(0.0, profile.guaranteedBandwidthUlMbps);
}

double
PriorityWeight(const FlowProfile& profile)
{
    return 1.0 / static_cast<double>(std::max<uint32_t>(1, profile.priority == 0 ? 1 : profile.priority));
}

const SliceResourceProfile*
ResolveSliceProfile(const SnapshotContext* context, const FlowProfile& profile)
{
    auto bySliceRef = context->sliceResources.find(profile.sliceRef);
    if (bySliceRef != context->sliceResources.end())
    {
        return &bySliceRef->second;
    }
    if (!profile.sliceSnssai.empty())
    {
        for (const auto& [sliceId, resource] : context->sliceResources)
        {
            if (resource.sliceSnssai == profile.sliceSnssai)
            {
                return &resource;
            }
        }
    }
    return nullptr;
}

double
ClampMetricFactor(double factor)
{
    return std::clamp(factor, 0.9, 1.1);
}

double
ComputeMetricFactor(const SnapshotContext* context,
                    uint16_t port,
                    double congestionRatio,
                    double phaseOffset)
{
    const double wave =
        std::sin((static_cast<double>(context->tickIndex) + static_cast<double>(port % 17)) * 0.55 + phaseOffset);
    const double oscillation = 0.05 * wave;
    const double congestionPenalty = 0.05 * std::clamp(congestionRatio, 0.0, 1.0);
    return ClampMetricFactor(1.0 + oscillation + congestionPenalty);
}

void
ApplyClientRate(const SnapshotContext::FlowRuntimeState& runtime)
{
    if (runtime.client == nullptr)
    {
        return;
    }
    const auto packetSize = static_cast<uint32_t>(std::max(64.0, runtime.profile.packetSizeBytes));
    runtime.client->SetAttribute("PacketSize", UintegerValue(packetSize));

    double ratePps = runtime.profile.arrivalRatePps;
    if (runtime.profile.allocatedBandwidthDlMbps > 0.0 && runtime.profile.packetSizeBytes > 0.0)
    {
        ratePps = runtime.profile.allocatedBandwidthDlMbps * 1e6 / 8.0 / runtime.profile.packetSizeBytes;
    }
    ratePps = std::max(1.0, ratePps);
    runtime.client->SetAttribute("Interval", TimeValue(Seconds(1.0 / ratePps)));
}

void
ReloadFlowProfiles(SnapshotContext* context)
{
    if (context->flowProfileFile.empty())
    {
        return;
    }

    const auto profiles = LoadFlowProfiles(context->flowProfileFile);
    std::map<std::string, FlowProfile> byId;
    for (const auto& profile : profiles)
    {
        byId[profile.flowId] = profile;
    }

    for (auto& [port, runtime] : context->flowRuntimeByPort)
    {
        auto it = byId.find(runtime.profile.flowId);
        if (it == byId.end())
        {
            continue;
        }
        const double currentAllocatedDl = runtime.profile.allocatedBandwidthDlMbps;
        const double currentAllocatedUl = runtime.profile.allocatedBandwidthUlMbps;
        runtime.profile = it->second;
        runtime.profile.allocatedBandwidthDlMbps = currentAllocatedDl;
        runtime.profile.allocatedBandwidthUlMbps = currentAllocatedUl;
    }

    // Rebuild UE slice membership from the latest live flow profiles so AM policies
    // become visible in the emitted UE snapshot after a hot reload.
    std::vector<std::string> reloadedUeSliceIds = context->ueSliceIds;
    if (reloadedUeSliceIds.size() < context->ueNum)
    {
        reloadedUeSliceIds.resize(context->ueNum);
    }
    for (uint32_t ue = 0; ue < context->ueNum; ++ue)
    {
        if (reloadedUeSliceIds[ue].empty())
        {
            reloadedUeSliceIds[ue] = BuildDefaultSliceId(context->sliceIds, context->sliceSds, ue);
        }
    }
    for (const auto& [port, runtime] : context->flowRuntimeByPort)
    {
        if (runtime.ueIndex >= reloadedUeSliceIds.size() || runtime.profile.sliceRef.empty())
        {
            continue;
        }
        reloadedUeSliceIds[runtime.ueIndex] = runtime.profile.sliceRef;
        AppendUniqueString(&context->sliceIds, runtime.profile.sliceRef);
    }
    context->ueSliceIds = std::move(reloadedUeSliceIds);
}

void
ApplySlaDrivenAllocations(SnapshotContext* context)
{
    using GroupKey = std::pair<uint32_t, std::string>;
    std::map<GroupKey, std::vector<SnapshotContext::FlowRuntimeState*>> flowsByGroup;
    context->sliceTelemetry.clear();
    for (auto& [port, runtime] : context->flowRuntimeByPort)
    {
        if (runtime.ueIndex >= context->ueToGnb.size())
        {
            continue;
        }
        flowsByGroup[{context->ueToGnb[runtime.ueIndex], runtime.profile.sliceRef}].push_back(&runtime);
    }

    for (auto& [groupKey, runtimes] : flowsByGroup)
    {
        const std::string& sliceId = groupKey.second;
        const auto resourceIt = context->sliceResources.find(sliceId);
        const double capacityDl = resourceIt != context->sliceResources.end()
                                      ? resourceIt->second.capacityDlMbps
                                      : std::max(1.0, context->bridgeLinkRateMbps);
        const double capacityUl = resourceIt != context->sliceResources.end()
                                      ? resourceIt->second.capacityUlMbps
                                      : std::max(1.0, context->bridgeLinkRateMbps);
        const double guaranteedDl = resourceIt != context->sliceResources.end()
                                        ? resourceIt->second.guaranteedDlMbps
                                        : capacityDl;
        const double guaranteedUl = resourceIt != context->sliceResources.end()
                                        ? resourceIt->second.guaranteedUlMbps
                                        : capacityUl;
        auto& telemetry = context->sliceTelemetry[sliceId];
        telemetry.capacityDlMbps += capacityDl;
        telemetry.capacityUlMbps += capacityUl;
        telemetry.guaranteedDlMbps += guaranteedDl;
        telemetry.guaranteedUlMbps += guaranteedUl;

        auto allocateDirection = [&](bool downlink, double capacity) {
            double guaranteedSum = 0.0;
            for (auto* runtime : runtimes)
            {
                const double requested = downlink ? RequestedBandwidthDlMbps(runtime->profile)
                                                  : RequestedBandwidthUlMbps(runtime->profile);
                const double guaranteed = downlink ? GuaranteedBandwidthDlMbps(runtime->profile)
                                                   : GuaranteedBandwidthUlMbps(runtime->profile);
                const double grant = std::min(requested > 0.0 ? requested : capacity, guaranteed);
                if (downlink)
                {
                    runtime->profile.allocatedBandwidthDlMbps = grant;
                    telemetry.demandDlMbps += requested;
                }
                else
                {
                    runtime->profile.allocatedBandwidthUlMbps = grant;
                    telemetry.demandUlMbps += requested;
                }
                guaranteedSum += grant;
            }

            if (guaranteedSum > capacity && guaranteedSum > 0.0)
            {
                const double scale = capacity / guaranteedSum;
                for (auto* runtime : runtimes)
                {
                    if (downlink)
                    {
                        runtime->profile.allocatedBandwidthDlMbps *= scale;
                    }
                    else
                    {
                        runtime->profile.allocatedBandwidthUlMbps *= scale;
                    }
                }
                return;
            }

            double remaining = std::max(0.0, capacity - guaranteedSum);
            std::vector<SnapshotContext::FlowRuntimeState*> active;
            for (auto* runtime : runtimes)
            {
                const double requested = downlink ? RequestedBandwidthDlMbps(runtime->profile)
                                                  : RequestedBandwidthUlMbps(runtime->profile);
                const double allocated = downlink ? runtime->profile.allocatedBandwidthDlMbps
                                                  : runtime->profile.allocatedBandwidthUlMbps;
                if (requested > allocated)
                {
                    active.push_back(runtime);
                }
            }

            while (remaining > 1e-6 && !active.empty())
            {
                double totalWeight = 0.0;
                for (const auto* runtime : active)
                {
                    totalWeight += PriorityWeight(runtime->profile);
                }
                if (totalWeight <= 0.0)
                {
                    break;
                }

                std::vector<SnapshotContext::FlowRuntimeState*> nextActive;
                double consumed = 0.0;
                for (auto* runtime : active)
                {
                    const double requested = downlink ? RequestedBandwidthDlMbps(runtime->profile)
                                                      : RequestedBandwidthUlMbps(runtime->profile);
                    const double allocated = downlink ? runtime->profile.allocatedBandwidthDlMbps
                                                      : runtime->profile.allocatedBandwidthUlMbps;
                    const double need = std::max(0.0, requested - allocated);
                    if (need <= 1e-6)
                    {
                        continue;
                    }
                    const double share = remaining * PriorityWeight(runtime->profile) / totalWeight;
                    const double grant = std::min(need, share);
                    if (downlink)
                    {
                        runtime->profile.allocatedBandwidthDlMbps += grant;
                    }
                    else
                    {
                        runtime->profile.allocatedBandwidthUlMbps += grant;
                    }
                    consumed += grant;
                    if (need - grant > 1e-6)
                    {
                        nextActive.push_back(runtime);
                    }
                }
                if (consumed <= 1e-6)
                {
                    break;
                }
                remaining = std::max(0.0, remaining - consumed);
                active = nextActive;
            }
        };

        allocateDirection(true, capacityDl);
        allocateDirection(false, capacityUl);

        for (auto* runtime : runtimes)
        {
            telemetry.allocatedDlMbps += runtime->profile.allocatedBandwidthDlMbps;
            telemetry.allocatedUlMbps += runtime->profile.allocatedBandwidthUlMbps;
            const double deficitMbps = std::max(0.0, RequestedBandwidthDlMbps(runtime->profile) -
                                                         runtime->profile.allocatedBandwidthDlMbps) +
                                      std::max(0.0, RequestedBandwidthUlMbps(runtime->profile) -
                                                         runtime->profile.allocatedBandwidthUlMbps);
            const double flowQueueBytes =
                deficitMbps * 1e6 / 8.0 * static_cast<double>(context->tickMs) / 1000.0;
            telemetry.queueBytes += flowQueueBytes;
            telemetry.droppedPackets += runtime->profile.packetSizeBytes > 0.0
                                            ? flowQueueBytes / runtime->profile.packetSizeBytes
                                            : 0.0;
            ApplyClientRate(*runtime);
        }
    }
}

void
WriteClockState(const SnapshotContext* context)
{
    if (context->clockFile.empty())
    {
        return;
    }

    std::filesystem::path clockPath(context->clockFile);
    if (!clockPath.parent_path().empty())
    {
        std::filesystem::create_directories(clockPath.parent_path());
    }

    const auto tempPath = clockPath.string() + ".tmp";
    std::ofstream output(tempPath, std::ios::trunc);
    output << "{" << Quote("run_id") << ":" << Quote(context->runId) << ","
           << Quote("scenario_id") << ":" << Quote(context->scenarioId) << ","
           << Quote("tick_index") << ":" << context->tickIndex << ","
           << Quote("sim_time_ms") << ":" << Simulator::Now().GetMilliSeconds() << ","
           << Quote("flows") << ":[";
    bool first = true;
    for (const auto& [port, runtime] : context->flowRuntimeByPort)
    {
        if (!first)
        {
            output << ",";
        }
        first = false;
        output << "{" << Quote("flow_id") << ":" << Quote(runtime.profile.flowId) << ","
               << Quote("allocated_bandwidth_dl_mbps") << ":" << runtime.profile.allocatedBandwidthDlMbps << ","
               << Quote("allocated_bandwidth_ul_mbps") << ":" << runtime.profile.allocatedBandwidthUlMbps << ","
               << Quote("packet_size_bytes") << ":" << runtime.profile.packetSizeBytes << ","
               << Quote("arrival_rate_pps") << ":" << runtime.profile.arrivalRatePps << "}";
    }
    output << "]}" << std::endl;
    output.close();
    std::error_code errorCode;
    std::filesystem::remove(clockPath, errorCode);
    errorCode.clear();
    std::filesystem::rename(tempPath, clockPath, errorCode);
}

void
EmitSnapshot(SnapshotContext* context)
{
    const uint32_t reloadEveryTicks = std::max<uint32_t>(1, std::max<uint32_t>(1, context->policyReloadMs) /
                                                                std::max<uint32_t>(1, context->tickMs));
    if (!context->flowRuntimeByPort.empty() && context->tickIndex % reloadEveryTicks == 0)
    {
        ReloadFlowProfiles(context);
        ApplySlaDrivenAllocations(context);
    }

    context->monitor->CheckForLostPackets();
    const auto stats = context->monitor->GetFlowStats();
    const double elapsedSeconds =
        context->externalTrafficOnly
            ? std::max(0.001, Simulator::Now().GetSeconds())
            : std::max(0.001, (Simulator::Now() - context->appStartTime).GetSeconds());

    std::map<std::string, uint32_t> ipToUeIndex;
    for (uint32_t index = 0; index < context->ueIps.size(); ++index)
    {
        ipToUeIndex[ToString(context->ueIps[index])] = index;
    }

    double totalThroughputDl = 0.0;
    double totalThroughputUl = 0.0;
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
        const auto defaultSliceId = Quote(BuildDefaultSliceId(context->sliceIds, context->sliceSds, ue));
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
    auto appendFlow = [&](const FlowProfile* profile,
                          uint32_t ueIndex,
                          uint32_t protocol,
                          const std::string& sourceIp,
                          uint32_t sourcePort,
                          const std::string& destinationIp,
                          uint32_t destinationPort,
                          double delayMs,
                          double jitterMs,
                          double lossRate,
                          double throughputUl,
                          double throughputDl,
                          uint64_t txPackets,
                          uint64_t rxPackets,
                          const std::string& direction,
                          const std::string& sourceEntity,
                          const std::string& destinationEntity,
                          const std::string& fallbackFlowId) {
        const uint32_t gnbIndex = context->ueToGnb[ueIndex];
        const uint32_t upfIndex = context->gnbToUpf[gnbIndex];
        const uint32_t sliceIndex = ueIndex % context->sliceSds.size();
        const std::string defaultSliceId = BuildDefaultSliceId(context->sliceIds, context->sliceSds, ueIndex);
        const std::string flowIdentifier =
            profile != nullptr && !profile->flowId.empty() ? profile->flowId : fallbackFlowId;
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
                       : defaultSliceId);
        const std::string sliceSnssai =
            profile != nullptr && !profile->sliceSnssai.empty()
                ? profile->sliceSnssai
                : BuildSliceSnssai(sliceId, context->sliceSds[sliceIndex]);
        const uint32_t fiveQi = profile != nullptr ? profile->fiveQi : 9;
        const double packetSizeBytes = profile != nullptr ? profile->packetSizeBytes : 512.0;
        const double arrivalRatePps = profile != nullptr ? profile->arrivalRatePps : 1000.0;
        const double targetLatencyMs = profile != nullptr ? profile->latencyMs : delayMs;
        const double targetJitterMs = profile != nullptr ? profile->jitterMs : jitterMs;
        const double targetLossRate = profile != nullptr ? profile->lossRate : lossRate;
        const double targetBandwidthDlMbps = profile != nullptr ? profile->bandwidthDlMbps : throughputDl;
        const double targetBandwidthUlMbps = profile != nullptr ? profile->bandwidthUlMbps : throughputUl;
        const double guaranteedBandwidthDlMbps =
            profile != nullptr ? profile->guaranteedBandwidthDlMbps : targetBandwidthDlMbps;
        const double guaranteedBandwidthUlMbps =
            profile != nullptr ? profile->guaranteedBandwidthUlMbps : targetBandwidthUlMbps;
        const double allocatedBandwidthDlMbps =
            profile != nullptr ? profile->allocatedBandwidthDlMbps : targetBandwidthDlMbps;
        const double allocatedBandwidthUlMbps =
            profile != nullptr ? profile->allocatedBandwidthUlMbps : targetBandwidthUlMbps;
        const double flowQueueBytes =
            profile != nullptr
                ? (std::max(0.0, RequestedBandwidthDlMbps(*profile) - allocatedBandwidthDlMbps) +
                   std::max(0.0, RequestedBandwidthUlMbps(*profile) - allocatedBandwidthUlMbps)) *
                      1e6 / 8.0 * static_cast<double>(context->tickMs) / 1000.0
                : 0.0;
        const bool optimizeRequested = profile != nullptr ? profile->optimizeRequested : false;
        const std::string serviceType =
            profile != nullptr && !profile->serviceType.empty() ? profile->serviceType : "eMBB";
        const std::string dnn =
            profile != nullptr && !profile->dnn.empty() ? profile->dnn : "internet";
        const std::string sessionRef =
            profile != nullptr && !profile->sessionRef.empty()
                ? profile->sessionRef
                : context->ueSupis[ueIndex] + ":" + sliceId + ":" + dnn;
        const std::string policyFilter = profile != nullptr ? profile->policyFilter : "";
        const uint32_t serviceTypeId = profile != nullptr ? profile->serviceTypeId : 1;
        const uint32_t priority = profile != nullptr ? profile->priority : 0;
        const uint32_t qosRef = profile != nullptr ? profile->qosRef : 0;
        const bool bidirectional = direction == "bidirectional";

        totalThroughputDl += throughputDl;
        totalThroughputUl += throughputUl;
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
             << Quote("session_ref") << ":" << Quote(sessionRef) << ","
             << Quote("src_gnb") << ":" << Quote("gnb-" + std::to_string(gnbIndex + 1)) << ","
             << Quote("dst_upf") << ":" << Quote(context->upfNames[upfIndex]) << ","
             << Quote("slice_id") << ":" << Quote(sliceId) << ","
             << Quote("5qi") << ":" << fiveQi << ","
             << Quote("delay_ms") << ":" << delayMs << ","
             << Quote("jitter_ms") << ":" << jitterMs << ","
             << Quote("loss_rate") << ":" << lossRate << ","
             << Quote("throughput_ul_mbps") << ":" << throughputUl << ","
             << Quote("throughput_dl_mbps") << ":" << throughputDl << ","
             << Quote("queue_bytes") << ":" << static_cast<uint64_t>(flowQueueBytes) << ","
             << Quote("rlc_buffer_bytes") << ":" << static_cast<uint64_t>(flowQueueBytes / 2.0) << ","
             << Quote("service") << ":{" << Quote("service_type") << ":" << Quote(serviceType) << ","
             << Quote("service_type_id") << ":" << serviceTypeId << ","
             << Quote("dnn") << ":" << Quote(dnn) << "},"
             << Quote("traffic") << ":{" << Quote("five_tuple") << ":{" << Quote("protocol") << ":"
             << protocol << "," << Quote("source_ip") << ":" << Quote(sourceIp) << ","
             << Quote("source_port") << ":" << sourcePort << "," << Quote("destination_ip") << ":"
             << Quote(destinationIp) << "," << Quote("destination_port") << ":"
             << destinationPort << "}";
        if (bidirectional)
        {
            json << "," << Quote("reverse_five_tuple") << ":{" << Quote("protocol") << ":"
                 << protocol << "," << Quote("source_ip") << ":" << Quote(destinationIp) << ","
                 << Quote("source_port") << ":" << destinationPort << "," << Quote("destination_ip") << ":"
                 << Quote(sourceIp) << "," << Quote("destination_port") << ":" << sourcePort << "}";
        }
        json << "," << Quote("direction") << ":" << Quote(direction) << ","
             << Quote("source_entity") << ":" << Quote(sourceEntity) << ","
             << Quote("destination_entity") << ":" << Quote(destinationEntity) << ","
             << Quote("packet_size") << ":" << packetSizeBytes << ","
             << Quote("filter") << ":" << Quote(policyFilter) << ","
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
             << Quote("packet_sent") << ":" << txPackets << "," << Quote("packet_received") << ":"
             << rxPackets << "," << Quote("throughput_dl") << ":" << throughputDl << ","
             << Quote("throughput_ul") << ":" << throughputUl << "},"
             << Quote("allocation") << ":{" << Quote("optimize_requested") << ":"
             << (optimizeRequested ? "true" : "false") << ","
             << Quote("qos_ref") << ":" << qosRef << ","
             << Quote("current_slice_snssai") << ":" << Quote(sliceSnssai) << ","
             << Quote("allocated_bandwidth_dl") << ":" << allocatedBandwidthDlMbps << ","
             << Quote("allocated_bandwidth_ul") << ":" << allocatedBandwidthUlMbps << "}}";
    };

    if (!context->externalTrafficOnly)
    {
        for (const auto& [flowId, flow] : stats)
        {
            Ipv4FlowClassifier::FiveTuple tuple = context->classifier->FindFlow(flowId);
            auto ueIt = ipToUeIndex.find(ToString(tuple.destinationAddress));
            if (ueIt == ipToUeIndex.end())
            {
                continue;
            }
            const auto profileIt = context->flowRuntimeByPort.find(tuple.destinationPort);
            const FlowProfile* profile =
                profileIt != context->flowRuntimeByPort.end() ? &profileIt->second.profile : nullptr;
            const double delayMs = flow.rxPackets > 0 ? 1000.0 * flow.delaySum.GetSeconds() / flow.rxPackets : 0.0;
            const double jitterMs = flow.rxPackets > 0 ? 1000.0 * flow.jitterSum.GetSeconds() / flow.rxPackets : 0.0;
            const double lossRate = flow.txPackets > 0
                                        ? static_cast<double>(flow.txPackets - flow.rxPackets) /
                                              static_cast<double>(flow.txPackets)
                                        : 0.0;
            const double throughputDl = flow.rxBytes * 8.0 / elapsedSeconds / 1e6;
            appendFlow(profile,
                       ueIt->second,
                       static_cast<uint32_t>(tuple.protocol),
                       ToString(tuple.sourceAddress),
                       tuple.sourcePort,
                       ToString(tuple.destinationAddress),
                       tuple.destinationPort,
                       delayMs,
                       jitterMs,
                       lossRate,
                       0.0,
                       throughputDl,
                       flow.txPackets,
                       flow.rxPackets,
                       "downlink",
                       "ns3_remote_host",
                       "ue_pdu_ip",
                       "flow-" + std::to_string(flowId));
        }
    }
    else
    {
        const double tickSeconds = std::max(0.001, static_cast<double>(context->tickMs) / 1000.0);
        for (const auto& [port, runtime] : context->flowRuntimeByPort)
        {
            const auto countersIt = context->externalFlowCountersByPort.find(port);
            const SnapshotContext::ExternalFlowCounters counters =
                countersIt != context->externalFlowCountersByPort.end()
                    ? countersIt->second
                    : SnapshotContext::ExternalFlowCounters{};
            const double offeredMbps =
                runtime.profile.packetSizeBytes * runtime.profile.arrivalRatePps * 8.0 / 1e6;
            const double capacityUl =
                runtime.profile.allocatedBandwidthUlMbps > 0.0
                    ? runtime.profile.allocatedBandwidthUlMbps
                    : (runtime.profile.bandwidthUlMbps > 0.0 ? runtime.profile.bandwidthUlMbps : offeredMbps);
            const double capacityDl =
                runtime.profile.allocatedBandwidthDlMbps > 0.0
                    ? runtime.profile.allocatedBandwidthDlMbps
                    : (runtime.profile.bandwidthDlMbps > 0.0 ? runtime.profile.bandwidthDlMbps : offeredMbps);
            const uint64_t txPacketsUl = counters.txPacketsUl;
            const uint64_t rxPacketsUl = counters.rxPacketsUl;
            const uint64_t txPacketsDl = counters.txPacketsDl;
            const uint64_t rxPacketsDl = counters.rxPacketsDl;
            const uint64_t txPackets = txPacketsUl + txPacketsDl;
            const uint64_t rxPackets = rxPacketsUl + rxPacketsDl;
            const uint64_t delaySamples = counters.delaySamplesUl + counters.delaySamplesDl;
            const uint64_t jitterSamples =
                (counters.hasLastDelayUl ? counters.delaySamplesUl - 1 : 0) +
                (counters.hasLastDelayDl ? counters.delaySamplesDl - 1 : 0);
            const double throughputUl =
                rxPacketsUl * runtime.profile.packetSizeBytes * 8.0 / tickSeconds / 1e6;
            const double throughputDl =
                rxPacketsDl * runtime.profile.packetSizeBytes * 8.0 / tickSeconds / 1e6;
            const double lossRate =
                txPackets > 0 ? static_cast<double>(txPackets - rxPackets) / static_cast<double>(txPackets) : 0.0;
            const double shortfallRatio =
                offeredMbps > 0.0
                    ? std::max(0.0, 1.0 - std::min(throughputUl, throughputDl) / offeredMbps)
                    : 0.0;
            const double measuredDelayMs =
                delaySamples > 0
                    ? (counters.delaySumMsUl + counters.delaySumMsDl) / static_cast<double>(delaySamples)
                    : 0.0;
            const double measuredJitterMs =
                jitterSamples > 0
                    ? (counters.jitterSumMsUl + counters.jitterSumMsDl) / static_cast<double>(jitterSamples)
                    : 0.0;
            const double delayMs = measuredDelayMs > 0.0 ? measuredDelayMs : std::max(0.1, context->bridgeLinkDelayMs);
            const double jitterMs = measuredJitterMs > 0.0 ? measuredJitterMs : 0.0;
            appendFlow(&runtime.profile,
                       runtime.ueIndex,
                       17,
                       ToString(context->ueIps[runtime.ueIndex]),
                       context->externalTrafficSourceBasePort + (port - 5000),
                       context->externalTrafficTargetIp,
                       port,
                       delayMs,
                       jitterMs,
                       lossRate,
                       throughputUl,
                       throughputDl,
                       txPackets,
                       rxPackets,
                       "bidirectional",
                       "ue_pdu_ip",
                       "external_data_network",
                       runtime.profile.flowId);
        }
        context->externalFlowCountersByPort.clear();
    }
    json << "],";

    json << Quote("slices") << ":[";
    for (uint32_t index = 0; index < context->sliceIds.size(); ++index)
    {
        if (index > 0)
        {
            json << ",";
        }
        const auto& sliceId = context->sliceIds[index];
        uint32_t sst = 1;
        std::string sd = context->sliceSds[index % context->sliceSds.size()];
        ParseSliceId(sliceId, &sst, &sd);
        json << "{" << Quote("slice_id") << ":" << Quote(sliceId) << ","
             << Quote("sst") << ":" << sst << ","
             << Quote("sd") << ":" << Quote(sd) << ","
             << Quote("label") << ":" << Quote("slice-" + std::to_string(index + 1));
        const auto resourceIt = context->sliceResources.find(sliceId);
        const auto telemetryIt = context->sliceTelemetry.find(sliceId);
        const double capacityDl = resourceIt != context->sliceResources.end() ? resourceIt->second.capacityDlMbps : 0.0;
        const double capacityUl = resourceIt != context->sliceResources.end() ? resourceIt->second.capacityUlMbps : 0.0;
        const double guaranteedDl = resourceIt != context->sliceResources.end() ? resourceIt->second.guaranteedDlMbps : 0.0;
        const double guaranteedUl = resourceIt != context->sliceResources.end() ? resourceIt->second.guaranteedUlMbps : 0.0;
        const double qosLatency = resourceIt != context->sliceResources.end() ? resourceIt->second.latencyMs : 0.0;
        const double qosJitter = resourceIt != context->sliceResources.end() ? resourceIt->second.jitterMs : 0.0;
        const double qosLoss = resourceIt != context->sliceResources.end() ? resourceIt->second.lossRate : 0.0;
        const double qosProcessingDelay =
            resourceIt != context->sliceResources.end() ? resourceIt->second.processingDelayMs : 0.0;
        const double demandDl = telemetryIt != context->sliceTelemetry.end() ? telemetryIt->second.demandDlMbps : 0.0;
        const double demandUl = telemetryIt != context->sliceTelemetry.end() ? telemetryIt->second.demandUlMbps : 0.0;
        const double allocatedDl = telemetryIt != context->sliceTelemetry.end() ? telemetryIt->second.allocatedDlMbps : 0.0;
        const double allocatedUl = telemetryIt != context->sliceTelemetry.end() ? telemetryIt->second.allocatedUlMbps : 0.0;
        const double queueBytes = telemetryIt != context->sliceTelemetry.end() ? telemetryIt->second.queueBytes : 0.0;
        const double droppedPackets =
            telemetryIt != context->sliceTelemetry.end() ? telemetryIt->second.droppedPackets : 0.0;
        const double utilizationDl = capacityDl > 0.0 ? allocatedDl / capacityDl : 0.0;
        const double utilizationUl = capacityUl > 0.0 ? allocatedUl / capacityUl : 0.0;
        json << "," << Quote("resource") << ":{" << Quote("capacity_dl_mbps") << ":"
             << capacityDl << "," << Quote("capacity_ul_mbps") << ":"
             << capacityUl << "," << Quote("guaranteed_dl_mbps") << ":"
             << guaranteedDl << "," << Quote("guaranteed_ul_mbps") << ":"
             << guaranteedUl << "},"
             << Quote("qos") << ":{"
             << Quote("latency") << ":" << qosLatency << ","
             << Quote("jitter") << ":" << qosJitter << ","
             << Quote("loss_rate") << ":" << qosLoss << ","
             << Quote("processing_delay") << ":" << qosProcessingDelay << "},"
             << Quote("telemetry") << ":{" << Quote("demand_dl_mbps") << ":"
             << demandDl << "," << Quote("demand_ul_mbps") << ":"
             << demandUl << "," << Quote("allocated_dl_mbps") << ":"
             << allocatedDl << "," << Quote("allocated_ul_mbps") << ":"
             << allocatedUl << "," << Quote("utilization_dl") << ":"
             << utilizationDl << "," << Quote("utilization_ul") << ":" << utilizationUl << ","
             << Quote("queue_bytes") << ":" << queueBytes << ","
             << Quote("dropped_packets") << ":" << droppedPackets << "}";
        json << "}";
    }
    json << "],";

    const double meanDelay = activeFlows > 0 ? totalDelayMs / activeFlows : 0.0;
    const double meanLoss = activeFlows > 0 ? totalLossRate / activeFlows : 0.0;
    json << Quote("kpis") << ":{" << Quote("active_flows") << ":" << activeFlows << ","
         << Quote("throughput_dl_mbps_total") << ":" << totalThroughputDl << ","
         << Quote("throughput_ul_mbps_total") << ":" << totalThroughputUl << ","
         << Quote("mean_delay_ms") << ":" << meanDelay << ","
         << Quote("mean_loss_rate") << ":" << meanLoss << "},";

    json << Quote("reward_inputs") << ":{" << Quote("throughput_score") << ":" << (totalThroughputDl + totalThroughputUl) << ","
         << Quote("delay_penalty") << ":" << meanDelay << ","
         << Quote("loss_penalty") << ":" << meanLoss << "}";
    json << "}";

    std::ofstream output(context->outputFile, std::ios::app);
    output << json.str() << std::endl;
    WriteClockState(context);

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
    std::string simulator = "RealtimeSimulatorImpl";
    std::string outputFile = "./tick-snapshots.jsonl";
    std::string clockFile;
    std::string flowProfileFile;
    std::string sliceResourceFile;
    uint32_t policyReloadMs = 1000;
    std::string upfNamesCsv = "upf";
    std::string sliceSdsCsv = "010203";
    std::string ueSupisCsv;
    std::string ueGnbMapCsv;
    std::string gnbUpfMapCsv;
    std::string gnbPositionsArg;
    std::string uePositionsArg;
    std::string bridgeGnbTapsCsv;
    std::string bridgeUpfTapsCsv;
    double bridgeLinkRateMbps = 1000.0;
    double bridgeLinkDelayMs = 1.0;
    double bridgeLinkLossRate = 0.0;
    bool externalTrafficOnly = false;
    std::string externalTrafficTargetIp = "8.8.8.8";
    uint32_t externalTrafficSourceBasePort = 15000;

    CommandLine cmd(__FILE__);
    cmd.AddValue("gNbNum", "Number of gNBs", gNbNum);
    cmd.AddValue("ueNum", "Total number of UEs", ueNum);
    cmd.AddValue("ueNumPerGnb", "Number of UEs per gNB", ueNumPerGnb);
    cmd.AddValue("tickMs", "Tick interval in milliseconds", tickMs);
    cmd.AddValue("simTimeMs", "Simulation time in milliseconds", simTimeMs);
    cmd.AddValue("runId", "Run identifier", runId);
    cmd.AddValue("scenarioId", "Scenario identifier", scenarioId);
    cmd.AddValue("simulator", "Simulator implementation type", simulator);
    cmd.AddValue("outputFile", "Snapshot JSONL output path", outputFile);
    cmd.AddValue("clockFile", "Clock state JSON output path", clockFile);
    cmd.AddValue("flowProfileFile", "TSV file describing scenario app/flow profiles", flowProfileFile);
    cmd.AddValue("sliceResourceFile", "TSV file describing slice resource pools", sliceResourceFile);
    cmd.AddValue("policyReloadMs", "How often to reload the flow profile TSV", policyReloadMs);
    cmd.AddValue("upfNames", "Comma separated UPF names", upfNamesCsv);
    cmd.AddValue("sliceSds", "Comma separated slice SD list", sliceSdsCsv);
    cmd.AddValue("ueSupis", "Comma separated UE SUPI list", ueSupisCsv);
    cmd.AddValue("ueGnbMap", "Comma separated 1-based gNB index for each UE", ueGnbMapCsv);
    cmd.AddValue("gnbUpfMap", "Comma separated 1-based UPF index for each gNB", gnbUpfMapCsv);
    cmd.AddValue("gnbPositions", "Semicolon separated x:y:z gNB positions or auto", gnbPositionsArg);
    cmd.AddValue("uePositions", "Semicolon separated x:y:z UE positions or auto", uePositionsArg);
    cmd.AddValue("bridgeGnbTaps", "Comma separated tap names attached to each gNB bridge", bridgeGnbTapsCsv);
    cmd.AddValue("bridgeUpfTaps", "Comma separated tap names attached to each UPF bridge", bridgeUpfTapsCsv);
    cmd.AddValue("bridgeLinkRateMbps", "Bridge link data rate in Mbps", bridgeLinkRateMbps);
    cmd.AddValue("bridgeLinkDelayMs", "Bridge link delay in milliseconds", bridgeLinkDelayMs);
    cmd.AddValue("bridgeLinkLossRate", "Bridge link packet loss rate applied via ReceiveErrorModel", bridgeLinkLossRate);
    cmd.AddValue("externalTrafficOnly", "Use real UE UDP flows and disable built-in ns-3 UDP apps", externalTrafficOnly);
    cmd.AddValue("externalTrafficTargetIp", "Destination IP used by the real UE UDP generator", externalTrafficTargetIp);
    cmd.AddValue("externalTrafficSourceBasePort", "First source port used by the real UE UDP generator", externalTrafficSourceBasePort);
    cmd.Parse(argc, argv);

    GlobalValue::Bind("SimulatorImplementationType", StringValue(NormalizeSimulatorType(simulator)));
    GlobalValue::Bind("ChecksumEnabled", BooleanValue(true));

    std::filesystem::create_directories(std::filesystem::path(outputFile).parent_path());
    std::ofstream(outputFile, std::ios::trunc).close();
    if (!clockFile.empty())
    {
        std::filesystem::path clockPath(clockFile);
        if (!clockPath.parent_path().empty())
        {
            std::filesystem::create_directories(clockPath.parent_path());
        }
    }

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

    const auto bridgeGnbTaps = ParseStringList(bridgeGnbTapsCsv, gNbNum, "bridgeGnbTaps");
    const auto bridgeUpfTaps = ParseStringList(bridgeUpfTapsCsv, gNbNum, "bridgeUpfTaps");

    const auto gnbPositionOverrides = ParsePositionOverrides(gnbPositionsArg, gNbNum, "gnbPositions");
    const auto uePositionOverrides = ParsePositionOverrides(uePositionsArg, resolvedUeNum, "uePositions");
    const auto flowProfiles = LoadFlowProfiles(flowProfileFile);
    const auto sliceResources = LoadSliceResources(sliceResourceFile);
    std::map<std::string, uint32_t> ueIndexBySupi;
    std::vector<std::string> ueSliceIds(resolvedUeNum);
    std::vector<std::string> sliceIds;
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
            AppendUniqueString(&sliceIds, profile.sliceRef);
        }
    }
    for (const auto& [sliceId, resource] : sliceResources)
    {
        AppendUniqueString(&sliceIds, sliceId);
    }
    for (const auto& sliceId : ueSliceIds)
    {
        AppendUniqueString(&sliceIds, sliceId);
    }
    if (sliceIds.empty())
    {
        for (const auto& sd : sliceSds)
        {
            AppendUniqueString(&sliceIds, "slice-1-" + sd);
        }
    }

    SnapshotContext context;

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

    if (!bridgeGnbTaps.empty() && !bridgeUpfTaps.empty())
    {
        NodeContainer gnbBridgeNodes;
        gnbBridgeNodes.Create(gNbNum);
        NodeContainer upfBridgeNodes;
        upfBridgeNodes.Create(gNbNum);
        CsmaHelper bridgeCsma;
        bridgeCsma.SetChannelAttribute(
            "DataRate",
            DataRateValue(DataRate(static_cast<uint64_t>(std::max(1.0, bridgeLinkRateMbps) * 1e6))));
        bridgeCsma.SetChannelAttribute("Delay",
                                       TimeValue(Seconds(std::max(0.0, bridgeLinkDelayMs) / 1000.0)));
        TapBridgeHelper tapBridge;
        tapBridge.SetAttribute("Mode", StringValue("UseBridge"));
        for (uint32_t index = 0; index < gNbNum; ++index)
        {
            NodeContainer pair;
            pair.Add(gnbBridgeNodes.Get(index));
            pair.Add(upfBridgeNodes.Get(index));
            NetDeviceContainer devices = bridgeCsma.Install(pair);
            if (bridgeLinkLossRate > 0.0)
            {
                for (uint32_t deviceIndex = 0; deviceIndex < devices.GetN(); ++deviceIndex)
                {
                    Ptr<RateErrorModel> errorModel = CreateObject<RateErrorModel>();
                    errorModel->SetUnit(RateErrorModel::ERROR_UNIT_PACKET);
                    errorModel->SetRate(std::min(1.0, std::max(0.0, bridgeLinkLossRate)));
                    devices.Get(deviceIndex)->SetAttribute("ReceiveErrorModel", PointerValue(errorModel));
                }
            }
            devices.Get(0)->TraceConnectWithoutContext("MacTx", MakeBoundCallback(&OnBridgeMacTx, &context, true));
            devices.Get(0)->TraceConnectWithoutContext("MacRx", MakeBoundCallback(&OnBridgeMacRx, &context, true));
            devices.Get(0)->TraceConnectWithoutContext("MacRxDrop",
                                                       MakeBoundCallback(&OnBridgeMacRxDrop, &context, true));
            devices.Get(1)->TraceConnectWithoutContext("MacTx", MakeBoundCallback(&OnBridgeMacTx, &context, false));
            devices.Get(1)->TraceConnectWithoutContext("MacRx", MakeBoundCallback(&OnBridgeMacRx, &context, false));
            devices.Get(1)->TraceConnectWithoutContext("MacRxDrop",
                                                       MakeBoundCallback(&OnBridgeMacRxDrop, &context, false));
            tapBridge.SetAttribute("DeviceName", StringValue(bridgeGnbTaps[index]));
            tapBridge.Install(pair.Get(0), devices.Get(0));
            tapBridge.SetAttribute("DeviceName", StringValue(bridgeUpfTaps[index]));
            tapBridge.Install(pair.Get(1), devices.Get(1));
        }
    }

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
    std::map<uint16_t, SnapshotContext::FlowRuntimeState> flowRuntimeByPort;

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
            Ptr<UdpClient> installedUdpClient;
            if (!externalTrafficOnly)
            {
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
                ApplicationContainer installedClient = client.Install(remoteHost);
                clientApps.Add(installedClient);
                installedUdpClient = DynamicCast<UdpClient>(installedClient.Get(0));

                NrEpsBearer bearer(NrEpsBearer::NGBR_LOW_LAT_EMBB);
                Ptr<NrEpcTft> tft = Create<NrEpcTft>();
                NrEpcTft::PacketFilter filter;
                filter.localPortStart = dlPort;
                filter.localPortEnd = dlPort;
                tft->Add(filter);
                nrHelper->ActivateDedicatedEpsBearer(ueNetDev.Get(ueIndex), bearer, tft);
            }
            flowRuntimeByPort[dlPort] = SnapshotContext::FlowRuntimeState{
                profile,
                installedUdpClient,
                dlPort,
                ueIndex,
            };
        }
    }
    else if (!externalTrafficOnly)
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
            ApplicationContainer installedClient = client.Install(remoteHost);
            clientApps.Add(installedClient);

            NrEpsBearer bearer(NrEpsBearer::NGBR_LOW_LAT_EMBB);
            Ptr<NrEpcTft> tft = Create<NrEpcTft>();
            NrEpcTft::PacketFilter filter;
            filter.localPortStart = dlPort;
            filter.localPortEnd = dlPort;
            tft->Add(filter);
            nrHelper->ActivateDedicatedEpsBearer(ueNetDev.Get(index), bearer, tft);
        }
    }

    if (!externalTrafficOnly)
    {
        serverApps.Start(appStartTime);
        clientApps.Start(appStartTime);
        serverApps.Stop(simTime);
        clientApps.Stop(simTime);
    }

    FlowMonitorHelper flowMonitorHelper;
    NodeContainer monitored;
    monitored.Add(remoteHost);
    monitored.Add(gridScenario.GetUserTerminals());

    Ptr<FlowMonitor> monitor = flowMonitorHelper.Install(monitored);
    monitor->SetAttribute("DelayBinWidth", DoubleValue(0.001));
    monitor->SetAttribute("JitterBinWidth", DoubleValue(0.001));
    monitor->SetAttribute("PacketSizeBinWidth", DoubleValue(20));
    Ptr<Ipv4FlowClassifier> classifier = DynamicCast<Ipv4FlowClassifier>(flowMonitorHelper.GetClassifier());

    context.runId = runId;
    context.scenarioId = scenarioId;
    context.outputFile = outputFile;
    context.clockFile = clockFile;
    context.flowProfileFile = flowProfileFile;
    context.sliceResourceFile = sliceResourceFile;
    context.tickMs = tickMs;
    context.policyReloadMs = policyReloadMs;
    context.gNbNum = gNbNum;
    context.ueNum = resolvedUeNum;
    context.bridgeLinkRateMbps = bridgeLinkRateMbps;
    context.bridgeLinkDelayMs = bridgeLinkDelayMs;
    context.bridgeLinkLossRate = bridgeLinkLossRate;
    context.externalTrafficOnly = externalTrafficOnly;
    context.externalTrafficTargetIp = externalTrafficTargetIp;
    context.externalTrafficSourceBasePort = externalTrafficSourceBasePort;
    context.upfNames = upfNames;
    context.sliceSds = sliceSds;
    context.sliceIds = sliceIds;
    context.sliceResources = sliceResources;
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
    context.flowRuntimeByPort = flowRuntimeByPort;

    for (uint32_t index = 0; index < resolvedUeNum; ++index)
    {
        context.ueIps.push_back(ueIpIfaces.GetAddress(index));
        context.ueToGnb.push_back(ueToGnb[index] - 1);
        context.ueSupis.push_back(ueSupis[index]);
        context.uePorts.push_back(static_cast<uint16_t>(5000 + index));
    }

    ApplySlaDrivenAllocations(&context);

    Simulator::Schedule(MilliSeconds(tickMs), &EmitSnapshot, &context);
    Simulator::Stop(simTime);
    Simulator::Run();
    Simulator::Destroy();
    return 0;
}
