
// SPDX-License-Identifier: GPL-2.0-only

#include "ns3/antenna-module.h"
#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/internet-apps-module.h"
#include "ns3/internet-module.h"
#include "ns3/mobility-module.h"
#include "ns3/nr-module.h"
#include "ns3/epc-gtpu-header.h"
#include "ns3/ethernet-header.h"
#include "ns3/point-to-point-module.h"
#include "ns3/udp-header.h"

#include <algorithm>
#include <cmath>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <set>
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

std::vector<Ipv4Address>
CollectNonLoopbackAddresses(Ptr<Node> node)
{
    NS_ASSERT(node != nullptr);
    Ptr<Ipv4> ipv4 = node->GetObject<Ipv4>();
    NS_ASSERT(ipv4 != nullptr);

    std::vector<Ipv4Address> addresses;
    for (uint32_t ifIndex = 0; ifIndex < ipv4->GetNInterfaces(); ++ifIndex)
    {
        for (uint32_t addrIndex = 0; addrIndex < ipv4->GetNAddresses(ifIndex); ++addrIndex)
        {
            const auto ifAddr = ipv4->GetAddress(ifIndex, addrIndex).GetLocal();
            if (ifAddr == Ipv4Address("127.0.0.1"))
            {
                continue;
            }
            addresses.push_back(ifAddr);
        }
    }

    return addresses;
}

std::string
JoinIpv4Addresses(const std::vector<Ipv4Address>& addresses)
{
    std::ostringstream stream;
    for (size_t index = 0; index < addresses.size(); ++index)
    {
        if (index > 0)
        {
            stream << ",";
        }
        stream << ToString(addresses[index]);
    }
    return stream.str();
}

Ipv4Address
ResolveRemoteHostDataAddress(Ptr<Node> remoteHost, const std::vector<Ipv4Address>& pgwLocalAddresses)
{
    const auto remoteHostAddresses = CollectNonLoopbackAddresses(remoteHost);
    std::vector<Ipv4Address> candidates;
    for (const auto& address : remoteHostAddresses)
    {
        if (std::find(pgwLocalAddresses.begin(), pgwLocalAddresses.end(), address) != pgwLocalAddresses.end())
        {
            continue;
        }
        candidates.push_back(address);
    }

    if (candidates.empty())
    {
        NS_FATAL_ERROR("unable to resolve remote-host data address; remote_host_addrs="
                       << JoinIpv4Addresses(remoteHostAddresses)
                       << " pgw_addrs=" << JoinIpv4Addresses(pgwLocalAddresses));
    }
    if (candidates.size() != 1)
    {
        NS_FATAL_ERROR("remote host has ambiguous data-plane addresses; candidates="
                       << JoinIpv4Addresses(candidates)
                       << " remote_host_addrs=" << JoinIpv4Addresses(remoteHostAddresses)
                       << " pgw_addrs=" << JoinIpv4Addresses(pgwLocalAddresses));
    }

    return candidates.front();
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

struct N3Link
{
    uint32_t gnbIndex;
    uint32_t upfIndex;
};

std::vector<N3Link>
ParseN3Links(const std::string& input, uint32_t gnbCount, uint32_t upfCount)
{
    if (input.empty())
    {
        NS_FATAL_ERROR("gnbUpfLinks must define at least one N3 link");
    }

    std::vector<N3Link> links;
    std::set<std::pair<uint32_t, uint32_t>> seen;
    for (const auto& value : SplitString(input, ';'))
    {
        const auto pair = SplitString(value, ':');
        if (pair.size() != 2)
        {
            NS_FATAL_ERROR("invalid gnbUpfLinks entry: " << value);
        }
        const auto gnb = static_cast<uint32_t>(std::stoul(pair[0]));
        const auto upf = static_cast<uint32_t>(std::stoul(pair[1]));
        if (gnb < 1 || gnb > gnbCount || upf < 1 || upf > upfCount)
        {
            NS_FATAL_ERROR("gnbUpfLinks entry is out of range: " << value);
        }
        const auto zeroBased = std::make_pair(gnb - 1, upf - 1);
        if (seen.insert(zeroBased).second)
        {
            links.push_back(N3Link{zeroBased.first, zeroBased.second});
        }
    }
    for (uint32_t gnb = 0; gnb < gnbCount; ++gnb)
    {
        const bool connected = std::any_of(links.begin(), links.end(), [gnb](const N3Link& link) {
            return link.gnbIndex == gnb;
        });
        if (!connected)
        {
            NS_FATAL_ERROR("gNB " << (gnb + 1) << " has no declared N3 link");
        }
    }
    return links;
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
    std::string upfName;
    std::string serviceType;
    uint32_t serviceTypeId = 0;
    uint32_t fiveQi = 9;
    double packetSizeBytes = 512.0;
    double arrivalRatePps = 1000.0;
    double dlPacketSizeBytes = 512.0;
    double ulPacketSizeBytes = 512.0;
    double dlArrivalRatePps = 1000.0;
    double ulArrivalRatePps = 1000.0;
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
    bool enabled = true;
};

double ResolvePacketSizeBytes(const FlowProfile& profile, bool downlink);

double ResolveArrivalRatePps(const FlowProfile& profile, bool downlink);

double RequestedBandwidthUlMbps(const FlowProfile& profile);

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
        profile.upfName = GetColumnValue(headerIndex, columns, "upf_ref");
        profile.serviceType = GetColumnValue(headerIndex, columns, "service_type");
        profile.serviceTypeId = ParseOptionalUint(GetColumnValue(headerIndex, columns, "service_type_id"), 0);
        profile.fiveQi = ParseOptionalUint(GetColumnValue(headerIndex, columns, "five_qi"), 9);
        profile.packetSizeBytes = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "packet_size_bytes"), 512.0);
        profile.arrivalRatePps = ParseOptionalDouble(GetColumnValue(headerIndex, columns, "arrival_rate_pps"), 1000.0);
        profile.dlPacketSizeBytes = ParseOptionalDouble(
            GetColumnValue(headerIndex, columns, "dl_packet_size_bytes"),
            profile.packetSizeBytes);
        profile.ulPacketSizeBytes = ParseOptionalDouble(
            GetColumnValue(headerIndex, columns, "ul_packet_size_bytes"),
            profile.packetSizeBytes);
        profile.dlArrivalRatePps = ParseOptionalDouble(
            GetColumnValue(headerIndex, columns, "dl_arrival_rate_pps"),
            profile.arrivalRatePps);
        profile.ulArrivalRatePps = ParseOptionalDouble(
            GetColumnValue(headerIndex, columns, "ul_arrival_rate_pps"),
            profile.arrivalRatePps);
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
        profile.enabled = ParseOptionalBool(GetColumnValue(headerIndex, columns, "enabled"), true);

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

double
DbToLinear(double valueDb)
{
    return std::pow(10.0, valueDb / 10.0);
}

std::string
NormalizeSchedulerType(const std::string& schedulerType)
{
    std::string normalized = schedulerType;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (normalized == "pf")
    {
        return "ns3::NrMacSchedulerOfdmaPF";
    }
    if (normalized == "rr")
    {
        return "ns3::NrMacSchedulerOfdmaRR";
    }
    if (normalized == "ofdma_pf" || normalized == "ns3::nrmacschedulerofdmapf")
    {
        return "ns3::NrMacSchedulerOfdmaPF";
    }
    if (normalized == "ofdma_rr" || normalized == "ns3::nrmacschedulerofdmarrr" ||
        normalized == "ns3::nrmacschedulerofdmarr")
    {
        return "ns3::NrMacSchedulerOfdmaRR";
    }
    NS_FATAL_ERROR("unsupported split-mode scheduler_type: " << schedulerType);
}

std::string
NormalizeTddPattern(const std::string& rawPattern)
{
    if (rawPattern.empty())
    {
        NS_FATAL_ERROR("split-mode tddPattern must be non-empty");
    }
    for (char token : rawPattern)
    {
        if (token != 'D' && token != 'L' && token != 'U' && token != 'F' && token != '|')
        {
            NS_FATAL_ERROR("unsupported character in split-mode tddPattern: " << rawPattern);
        }
    }
    return rawPattern;
}

struct SnapshotContext
{
    struct TraceParseCounters
    {
        uint64_t extractCalls = 0;
        uint64_t ethernetNonIpv4 = 0;
        uint64_t parseNoIpv4 = 0;
        uint64_t parseNonUdpOuter = 0;
        uint64_t parseNoOuterUdp = 0;
        uint64_t parseGtpuNoHeader = 0;
        uint64_t parseGtpuNoInnerIpv4 = 0;
        uint64_t parseGtpuInnerNonUdp = 0;
        uint64_t parseGtpuNoInnerUdp = 0;
        uint64_t parseOkOuterUdp = 0;
        uint64_t parseOkGtpuInnerUdp = 0;
        uint64_t matchDestinationPort = 0;
        uint64_t matchSourcePort = 0;
        uint64_t unmatchedPorts = 0;
        std::map<uint16_t, uint64_t> ethernetTypes;
    };

    struct FlowRuntimeState
    {
        FlowProfile profile;
        Ptr<PacketSink> downlinkSink;
        Ptr<PacketSink> uplinkSink;
        uint16_t port = 0;
        uint16_t uplinkPort = 0;
        uint16_t downlinkSourcePort = 0;
        uint16_t uplinkSourcePort = 0;
        uint32_t ueIndex = 0;
        uint64_t appTxPacketsDl = 0;
        uint64_t appTxPacketsUl = 0;
        uint64_t appTxBytesDl = 0;
        uint64_t appTxBytesUl = 0;
        uint64_t appSendErrorsDl = 0;
        uint64_t appSendErrorsUl = 0;
        uint64_t ipObservedUeTxUl = 0;
        uint64_t ipObservedRemoteRxUl = 0;
        uint64_t ipObservedRemoteTxDl = 0;
        uint64_t ipObservedUeRxDl = 0;
        uint64_t ipObservedPgwRxUl = 0;
        uint64_t ipObservedPgwTxUl = 0;
        uint64_t ipObservedPgwLocalDeliverUl = 0;
        uint64_t ipObservedPgwForwardUl = 0;
        uint64_t ipObservedPgwDropUl = 0;
        uint32_t detailedTraceLogs = 0;
        uint64_t lastLoggedUeTxUl = 0;
        uint64_t lastLoggedPgwForwardUl = 0;
        uint32_t consecutiveUplinkStallTicks = 0;
        uint32_t uplinkStallLogs = 0;
        uint64_t lastSnapshotPacketSent = 0;
        uint64_t lastSnapshotPacketReceived = 0;
        uint64_t lastSnapshotPacketSentDl = 0;
        uint64_t lastSnapshotPacketSentUl = 0;
        uint64_t lastSnapshotPacketReceivedDl = 0;
        uint64_t lastSnapshotPacketReceivedUl = 0;
    };

    struct RadioConfig
    {
        std::string schedulerType = "pf";
        std::string tddPattern = "DL|UL|UL|F|DL|UL|UL|F|";
        double gnbTxPowerDbm = 43.0;
        double ueTxPowerDbm = 23.0;
        double gnbNoiseFigureDb = 5.0;
        double ueNoiseFigureDb = 7.0;
        bool enableUplinkPowerControl = true;
        uint16_t numerology = 1;
        double centralFrequencyHz = 3.5e9;
        double bandwidthHz = 100e6;
    };

    struct UeRadioTelemetry
    {
        double dlDataSinrDb = 0.0;
        bool hasDlDataSinr = false;
        double rsrpDbm = 0.0;
        double rsrqDb = 0.0;
        bool hasMeasurement = false;
        uint32_t servingGnbIndex = 0;
        uint64_t ulBufferBytes = 0;
        uint64_t dlBufferBytes = 0;
    };

    struct GnbRuntimeTelemetry
    {
        uint32_t activeUeCount = 0;
        uint64_t ulScheduledBytes = 0;
        uint64_t dlScheduledBytes = 0;
        double ulPrbUtilization = 0.0;
        double dlPrbUtilization = 0.0;
        double radioCapacityUlMbps = 0.0;
        double radioCapacityDlMbps = 0.0;
        bool radioCapacityUnknown = true;
        std::string radioCapacitySource = "uninitialized";
        std::deque<double> recentUlThroughputMbps;
        std::deque<double> recentDlThroughputMbps;
        uint64_t ulUsedReg = 0;
        uint64_t dlUsedReg = 0;
        uint64_t ulAvailableReg = 0;
        uint64_t dlAvailableReg = 0;
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
    std::vector<std::string> upfNames;
    std::vector<std::string> sliceSds;
    std::vector<std::string> sliceIds;
    std::vector<N3Link> n3Links;
    std::vector<std::string> ueSliceIds;
    std::vector<Ipv4Address> ueIps;
    std::vector<uint32_t> ueToGnb;
    std::vector<std::string> ueSupis;
    std::vector<uint16_t> uePorts;
    std::vector<Vector> gnbPositions;
    std::vector<Vector> uePositions;
    std::vector<Ipv4Address> pgwLocalAddresses;
    std::vector<Ipv4Address> remoteHostAddresses;
    Ipv4Address ueDefaultGateway;
    Ipv4Address remoteHostAddress;
    Ipv4Address remoteHostGatewayAddress;
    RadioConfig radioConfig;
    std::vector<UeRadioTelemetry> ueRadioTelemetry;
    std::vector<GnbRuntimeTelemetry> gnbRadioTelemetry;
    std::map<uint64_t, uint32_t> ueIndexByImsi;
    std::map<uint16_t, uint32_t> ueIndexByRnti;
    std::map<uint16_t, uint32_t> gnbIndexByCellId;
    std::map<uint16_t, FlowRuntimeState> flowRuntimeByPort;
    uint64_t unmatchedIpv4TraceLogs = 0;
    uint64_t unmatchedIpv4DecisionLogs = 0;
    std::map<std::string, SliceResourceProfile> sliceResources;
    std::map<std::string, SliceRuntimeTelemetry> sliceTelemetry;
    Ptr<FlowMonitor> monitor;
    Ptr<Ipv4FlowClassifier> classifier;
    Time appStartTime;
    Time simTime;
};

uint32_t
ResolveFlowUpfIndex(const SnapshotContext* context, uint32_t gnbIndex, const FlowProfile* profile)
{
    if (profile == nullptr || profile->upfName.empty())
    {
        NS_FATAL_ERROR("split flow is missing upf_ref");
    }
    const auto upfIt = std::find(context->upfNames.begin(), context->upfNames.end(), profile->upfName);
    if (upfIt == context->upfNames.end())
    {
        NS_FATAL_ERROR("flow " << profile->flowId << " references unknown UPF " << profile->upfName);
    }
    const auto upfIndex = static_cast<uint32_t>(std::distance(context->upfNames.begin(), upfIt));
    const bool linked = std::any_of(context->n3Links.begin(), context->n3Links.end(), [gnbIndex, upfIndex](const N3Link& link) {
        return link.gnbIndex == gnbIndex && link.upfIndex == upfIndex;
    });
    if (!linked)
    {
        NS_FATAL_ERROR("flow " << profile->flowId << " selects an UPF without a declared N3 link");
    }
    return upfIndex;
}

class SplitFlowUdpApp : public Application
{
  public:
    static TypeId GetTypeId();

    void Configure(Address peer,
                   SnapshotContext::FlowRuntimeState* runtime,
                   Time inactivePollInterval,
                   uint16_t localPort = 0,
                   bool downlink = true);

  private:
    void StartApplication() override;
    void StopApplication() override;
    void ScheduleNext(Time delay);
    void SendOrPoll();
    Time ResolveInterval() const;
    uint32_t ResolvePacketSize() const;
    std::string ResolvePeerString() const;

    Ptr<Socket> m_socket;
    Address m_peer;
    SnapshotContext::FlowRuntimeState* m_runtime = nullptr;
    Time m_inactivePollInterval = MilliSeconds(100);
    EventId m_event;
    bool m_running = false;
    uint16_t m_localPort = 0;
    bool m_downlink = true;
    uint64_t m_sendErrors = 0;
};

NS_OBJECT_ENSURE_REGISTERED(SplitFlowUdpApp);

TypeId
SplitFlowUdpApp::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::SplitFlowUdpApp").SetParent<Application>().AddConstructor<SplitFlowUdpApp>();
    return tid;
}

void
SplitFlowUdpApp::Configure(Address peer,
                           SnapshotContext::FlowRuntimeState* runtime,
                           Time inactivePollInterval,
                           uint16_t localPort,
                           bool downlink)
{
    m_peer = peer;
    m_runtime = runtime;
    m_inactivePollInterval = inactivePollInterval;
    m_localPort = localPort;
    m_downlink = downlink;
}

void
SplitFlowUdpApp::StartApplication()
{
    m_running = true;
    if (m_socket == nullptr)
    {
        m_socket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
        if (m_localPort != 0)
        {
            m_socket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_localPort));
        }
        m_socket->Connect(m_peer);
    }
    ScheduleNext(Seconds(0));
}

void
SplitFlowUdpApp::StopApplication()
{
    m_running = false;
    if (m_event.IsPending())
    {
        Simulator::Cancel(m_event);
    }
    if (m_socket != nullptr)
    {
        m_socket->Close();
        m_socket = nullptr;
    }
}

void
SplitFlowUdpApp::ScheduleNext(Time delay)
{
    if (!m_running)
    {
        return;
    }
    m_event = Simulator::Schedule(delay, &SplitFlowUdpApp::SendOrPoll, this);
}

void
SplitFlowUdpApp::SendOrPoll()
{
    if (!m_running || m_runtime == nullptr)
    {
        return;
    }
    if (!m_runtime->profile.enabled)
    {
        ScheduleNext(m_inactivePollInterval);
        return;
    }

    const auto packetSize = ResolvePacketSize();
    const int sentBytes = m_socket->Send(Create<Packet>(packetSize));
    if (sentBytes < 0)
    {
        ++m_sendErrors;
        if (m_downlink)
        {
            ++m_runtime->appSendErrorsDl;
        }
        else
        {
            ++m_runtime->appSendErrorsUl;
        }
        std::cerr << "[split-ns3] socket-send-failed"
                  << " direction=" << (m_downlink ? "dl" : "ul")
                  << " flow_id=" << m_runtime->profile.flowId
                  << " local_port=" << m_localPort
                  << " peer=" << ResolvePeerString()
                  << " packet_size=" << packetSize
                  << " error_count=" << m_sendErrors
                  << " sim_time_ms=" << Simulator::Now().GetMilliSeconds()
                  << std::endl;
    }
    else
    {
        if (m_downlink)
        {
            ++m_runtime->appTxPacketsDl;
            m_runtime->appTxBytesDl += static_cast<uint64_t>(sentBytes);
        }
        else
        {
            ++m_runtime->appTxPacketsUl;
            m_runtime->appTxBytesUl += static_cast<uint64_t>(sentBytes);
        }
    }
    ScheduleNext(ResolveInterval());
}

Time
SplitFlowUdpApp::ResolveInterval() const
{
    if (m_runtime == nullptr)
    {
        return Seconds(1.0);
    }
    double ratePps = ResolveArrivalRatePps(m_runtime->profile, m_downlink);
    const double packetSizeBytes = ResolvePacketSizeBytes(m_runtime->profile, m_downlink);
    const double allocatedBandwidthMbps =
        m_downlink ? m_runtime->profile.allocatedBandwidthDlMbps : m_runtime->profile.allocatedBandwidthUlMbps;
    if (allocatedBandwidthMbps > 0.0 && packetSizeBytes > 0.0)
    {
        ratePps = allocatedBandwidthMbps * 1e6 / 8.0 / packetSizeBytes;
    }
    ratePps = std::max(1.0, ratePps);
    return Seconds(1.0 / ratePps);
}

uint32_t
SplitFlowUdpApp::ResolvePacketSize() const
{
    if (m_runtime == nullptr)
    {
        return 64;
    }
    return static_cast<uint32_t>(std::max(64.0, ResolvePacketSizeBytes(m_runtime->profile, m_downlink)));
}

std::string
SplitFlowUdpApp::ResolvePeerString() const
{
    InetSocketAddress address = InetSocketAddress::ConvertFrom(m_peer);
    std::ostringstream stream;
    stream << address.GetIpv4() << ":" << address.GetPort();
    return stream.str();
}

bool
ExtractUdpTupleFromPacket(SnapshotContext::TraceParseCounters* debug,
                          Ptr<const Packet> packet,
                          uint16_t* sourcePort,
                          uint16_t* destinationPort)
{
    NS_ASSERT(debug != nullptr);
    NS_ASSERT(sourcePort != nullptr);
    NS_ASSERT(destinationPort != nullptr);

    const uint32_t packetSize = packet->GetSize();
    if (packetSize == 0)
    {
        debug->parseNoIpv4++;
        return false;
    }

    std::vector<uint8_t> bytes(packetSize);
    packet->CopyData(bytes.data(), packetSize);

    auto readU16 = [&bytes](size_t offset) -> uint16_t {
        return static_cast<uint16_t>((static_cast<uint16_t>(bytes[offset]) << 8) |
                                     static_cast<uint16_t>(bytes[offset + 1]));
    };

    size_t offset = 0;
    if (packetSize >= 14)
    {
        const uint16_t etherType = readU16(12);
        if ((bytes[0] & 0xF0) != 0x40)
        {
            debug->ethernetTypes[etherType]++;
            if (etherType != 0x0800)
            {
                debug->ethernetNonIpv4++;
                return false;
            }
            offset = 14;
        }

    }

    if (packetSize < offset + 20)
    {
        debug->parseNoIpv4++;
        return false;
    }

    const uint8_t ipv4Version = bytes[offset] >> 4;
    if (ipv4Version != 4)
    {
        debug->parseNoIpv4++;
        return false;
    }

    const uint8_t ipv4IhlWords = bytes[offset] & 0x0F;
    const size_t ipv4HeaderLength = static_cast<size_t>(ipv4IhlWords) * 4;
    if (ipv4IhlWords < 5 || packetSize < offset + ipv4HeaderLength)
    {
        debug->parseNoIpv4++;
        return false;
    }

    const uint8_t outerProtocol = bytes[offset + 9];
    offset += ipv4HeaderLength;
    if (outerProtocol != 17)
    {
        debug->parseNonUdpOuter++;
        return false;
    }

    if (packetSize < offset + 8)
    {
        debug->parseNoOuterUdp++;
        return false;
    }

    *sourcePort = readU16(offset);
    *destinationPort = readU16(offset + 2);
    offset += 8;

    // The bridged gNB<->UPF N3 link carries GTP-U, so the first UDP header is often the
    // outer transport (typically port 2152) rather than the UE application's inner flow.
    if (*sourcePort == 2152 || *destinationPort == 2152)
    {
        if (packetSize < offset + 8)
        {
            debug->parseGtpuNoHeader++;
            return false;
        }

        const uint8_t gtpuFlags = bytes[offset];
        const uint8_t gtpuMessageType = bytes[offset + 1];
        if ((gtpuFlags & 0x30) != 0x30 || gtpuMessageType != 0xff)
        {
            debug->parseGtpuNoHeader++;
            return false;
        }

        size_t gtpuHeaderLength = 8;
        const bool hasExtension = (gtpuFlags & 0x04) != 0;
        const bool hasSequence = (gtpuFlags & 0x02) != 0;
        const bool hasNpdu = (gtpuFlags & 0x01) != 0;
        if (hasExtension || hasSequence || hasNpdu)
        {
            gtpuHeaderLength += 4;
            if (packetSize < offset + gtpuHeaderLength)
            {
                debug->parseGtpuNoHeader++;
                return false;
            }
            if (hasExtension)
            {
                size_t extensionOffset = offset + gtpuHeaderLength;
                while (true)
                {
                    if (packetSize < extensionOffset + 2)
                    {
                        debug->parseGtpuNoHeader++;
                        return false;
                    }
                    const uint8_t extensionLengthUnits = bytes[extensionOffset];
                    const size_t extensionLength = static_cast<size_t>(extensionLengthUnits) * 4;
                    if (extensionLength == 0 || packetSize < extensionOffset + extensionLength)
                    {
                        debug->parseGtpuNoHeader++;
                        return false;
                    }
                    const uint8_t nextExtensionType = bytes[extensionOffset + extensionLength - 1];
                    extensionOffset += extensionLength;
                    gtpuHeaderLength = extensionOffset - offset;
                    if (nextExtensionType == 0)
                    {
                        break;
                    }
                }
            }
        }

        offset += gtpuHeaderLength;
        if (packetSize < offset + 20)
        {
            debug->parseGtpuNoInnerIpv4++;
            return false;
        }

        const uint8_t innerIpv4Version = bytes[offset] >> 4;
        const uint8_t innerIpv4IhlWords = bytes[offset] & 0x0F;
        const size_t innerIpv4HeaderLength = static_cast<size_t>(innerIpv4IhlWords) * 4;
        if (innerIpv4Version != 4 || innerIpv4IhlWords < 5 ||
            packetSize < offset + innerIpv4HeaderLength)
        {
            debug->parseGtpuNoInnerIpv4++;
            return false;
        }

        const uint8_t innerProtocol = bytes[offset + 9];
        offset += innerIpv4HeaderLength;
        if (innerProtocol != 17)
        {
            debug->parseGtpuInnerNonUdp++;
            return false;
        }

        if (packetSize < offset + 8)
        {
            debug->parseGtpuNoInnerUdp++;
            return false;
        }

        *sourcePort = readU16(offset);
        *destinationPort = readU16(offset + 2);
        debug->parseOkGtpuInnerUdp++;
        return true;
    }

    debug->parseOkOuterUdp++;
    return true;
}

bool
ResolveIpv4UdpFlow(const SnapshotContext* context,
                   Ptr<const Packet> packet,
                   SnapshotContext::FlowRuntimeState** runtime,
                   bool* uplink)
{
    NS_ASSERT(context != nullptr);
    NS_ASSERT(runtime != nullptr);
    NS_ASSERT(uplink != nullptr);

    SnapshotContext::TraceParseCounters debug;
    uint16_t sourcePort = 0;
    uint16_t destinationPort = 0;
    if (!ExtractUdpTupleFromPacket(&debug, packet, &sourcePort, &destinationPort))
    {
        return false;
    }

    auto downlinkIt = context->flowRuntimeByPort.find(destinationPort);
    if (downlinkIt != context->flowRuntimeByPort.end())
    {
        *runtime = const_cast<SnapshotContext::FlowRuntimeState*>(&downlinkIt->second);
        *uplink = false;
        return true;
    }

    for (const auto& [port, candidate] : context->flowRuntimeByPort)
    {
        (void)port;
        if (candidate.uplinkPort == destinationPort && candidate.uplinkSourcePort == sourcePort)
        {
            *runtime = const_cast<SnapshotContext::FlowRuntimeState*>(&candidate);
            *uplink = true;
            return true;
        }
    }

    return false;
}

bool
ResolveIpv4UdpFlowFromHeader(const SnapshotContext* context,
                             const Ipv4Header& ipv4Header,
                             Ptr<const Packet> payload,
                             SnapshotContext::FlowRuntimeState** runtime,
                             bool* uplink)
{
    NS_ASSERT(context != nullptr);
    NS_ASSERT(runtime != nullptr);
    NS_ASSERT(uplink != nullptr);

    if (ipv4Header.GetProtocol() != 17 || payload == nullptr)
    {
        return false;
    }

    const uint32_t payloadSize = payload->GetSize();
    UdpHeader udpHeader;
    if (payloadSize < udpHeader.GetSerializedSize())
    {
        return false;
    }

    std::vector<uint8_t> bytes(payloadSize);
    payload->CopyData(bytes.data(), payloadSize);
    const uint16_t sourcePort = static_cast<uint16_t>((static_cast<uint16_t>(bytes[0]) << 8) |
                                                      static_cast<uint16_t>(bytes[1]));
    const uint16_t destinationPort = static_cast<uint16_t>((static_cast<uint16_t>(bytes[2]) << 8) |
                                                           static_cast<uint16_t>(bytes[3]));

    auto downlinkIt = context->flowRuntimeByPort.find(destinationPort);
    if (downlinkIt != context->flowRuntimeByPort.end())
    {
        const auto& candidate = downlinkIt->second;
        if (candidate.ueIndex < context->ueIps.size() &&
            ipv4Header.GetDestination() == context->ueIps[candidate.ueIndex])
        {
            *runtime = const_cast<SnapshotContext::FlowRuntimeState*>(&candidate);
            *uplink = false;
            return true;
        }
    }

    for (const auto& [port, candidate] : context->flowRuntimeByPort)
    {
        (void)port;
        if (candidate.uplinkPort == destinationPort && candidate.uplinkSourcePort == sourcePort &&
            candidate.ueIndex < context->ueIps.size() &&
            ipv4Header.GetSource() == context->ueIps[candidate.ueIndex])
        {
            *runtime = const_cast<SnapshotContext::FlowRuntimeState*>(&candidate);
            *uplink = true;
            return true;
        }
    }

    return false;
}

enum class Ipv4TraceRole
{
    UeTx,
    UeRx,
    PgwRx,
    PgwTx,
    PgwLocalDeliver,
    PgwForward,
    PgwDrop,
    RemoteTx,
    RemoteRx,
};

const char*
TraceRoleName(Ipv4TraceRole role)
{
    switch (role)
    {
    case Ipv4TraceRole::UeTx:
        return "ue-tx";
    case Ipv4TraceRole::UeRx:
        return "ue-rx";
    case Ipv4TraceRole::PgwRx:
        return "pgw-rx";
    case Ipv4TraceRole::PgwTx:
        return "pgw-tx";
    case Ipv4TraceRole::PgwLocalDeliver:
        return "pgw-local-deliver";
    case Ipv4TraceRole::PgwForward:
        return "pgw-forward";
    case Ipv4TraceRole::PgwDrop:
        return "pgw-drop";
    case Ipv4TraceRole::RemoteTx:
        return "remote-tx";
    case Ipv4TraceRole::RemoteRx:
        return "remote-rx";
    }
    return "unknown";
}

bool
IsDetailedTraceTarget(const SnapshotContext::FlowRuntimeState& runtime)
{
    return runtime.profile.flowId == "flow-7528" || runtime.profile.flowId == "flow-4493" ||
           runtime.profile.flowId == "flow-8178" || runtime.profile.flowId == "flow-8036" ||
           runtime.profile.flowId == "flow-7178";
}

void
MaybeEmitUplinkStallTrace(SnapshotContext* context, SnapshotContext::FlowRuntimeState* runtime)
{
    if (context == nullptr || runtime == nullptr || !IsDetailedTraceTarget(*runtime))
    {
        return;
    }
    if (runtime->ipObservedUeTxUl > runtime->lastLoggedUeTxUl &&
        runtime->ipObservedPgwForwardUl == runtime->lastLoggedPgwForwardUl)
    {
        ++runtime->consecutiveUplinkStallTicks;
    }
    else
    {
        runtime->consecutiveUplinkStallTicks = 0;
    }

    runtime->lastLoggedUeTxUl = runtime->ipObservedUeTxUl;
    runtime->lastLoggedPgwForwardUl = runtime->ipObservedPgwForwardUl;

    if (runtime->consecutiveUplinkStallTicks < 3 || runtime->uplinkStallLogs >= 8)
    {
        return;
    }

    ++runtime->uplinkStallLogs;
    const auto& ueRadio =
        runtime->ueIndex < context->ueRadioTelemetry.size() ? context->ueRadioTelemetry[runtime->ueIndex]
                                                            : SnapshotContext::UeRadioTelemetry{};
    const uint32_t gnbIndex =
        runtime->ueIndex < context->ueToGnb.size() ? context->ueToGnb[runtime->ueIndex] : 0;
    const auto& gnbRadio =
        gnbIndex < context->gnbRadioTelemetry.size() ? context->gnbRadioTelemetry[gnbIndex]
                                                     : SnapshotContext::GnbRuntimeTelemetry{};
    std::cerr << "[split-ns3] uplink-stall"
              << " tick=" << context->tickIndex
              << " flow_id=" << runtime->profile.flowId
              << " ue_index=" << runtime->ueIndex
              << " gnb_index=" << gnbIndex
              << " ue_ip="
              << (runtime->ueIndex < context->ueIps.size() ? ToString(context->ueIps[runtime->ueIndex])
                                                           : "0.0.0.0")
              << " session_ref=" << runtime->profile.sessionRef
              << " slice_ref=" << runtime->profile.sliceRef
              << " app_id=" << runtime->profile.appId
              << " stall_ticks=" << runtime->consecutiveUplinkStallTicks
              << " ue_tx_ul=" << runtime->ipObservedUeTxUl
              << " pgw_rx_ul=" << runtime->ipObservedPgwRxUl
              << " pgw_fwd_ul=" << runtime->ipObservedPgwForwardUl
              << " remote_rx_ul=" << runtime->ipObservedRemoteRxUl
              << " alloc_ul_mbps=" << runtime->profile.allocatedBandwidthUlMbps
              << " requested_ul_mbps=" << RequestedBandwidthUlMbps(runtime->profile)
              << " ue_ul_buffer_bytes=" << ueRadio.ulBufferBytes
              << " ue_dl_buffer_bytes=" << ueRadio.dlBufferBytes
              << " gnb_radio_capacity_ul_mbps=" << gnbRadio.radioCapacityUlMbps
              << " gnb_ul_scheduled_bytes=" << gnbRadio.ulScheduledBytes
              << " gnb_ul_prb_utilization=" << gnbRadio.ulPrbUtilization
              << std::endl;
}

bool
ShouldEmitDetailedTrace(const SnapshotContext* context, const SnapshotContext::FlowRuntimeState& runtime)
{
    if (context == nullptr)
    {
        return false;
    }
    if (!IsDetailedTraceTarget(runtime))
    {
        return false;
    }
    if (context->tickIndex < 24)
    {
        return false;
    }
    return runtime.detailedTraceLogs < 24;
}

void
EmitDetailedTrace(SnapshotContext* context,
                  SnapshotContext::FlowRuntimeState* runtime,
                  Ipv4TraceRole role,
                  const Ipv4Header* ipv4Header,
                  uint16_t sourcePort,
                  uint16_t destinationPort,
                  const char* note)
{
    if (context == nullptr || runtime == nullptr || !ShouldEmitDetailedTrace(context, *runtime))
    {
        return;
    }
    ++runtime->detailedTraceLogs;
    std::cerr << "[split-ns3] detailed-ul-trace"
              << " tick=" << context->tickIndex
              << " flow_id=" << runtime->profile.flowId
              << " role=" << TraceRoleName(role)
              << " note=" << note
              << " src_ip=" << (ipv4Header != nullptr ? ToString(ipv4Header->GetSource()) : "0.0.0.0")
              << " dst_ip=" << (ipv4Header != nullptr ? ToString(ipv4Header->GetDestination()) : "0.0.0.0")
              << " src_port=" << sourcePort
              << " dst_port=" << destinationPort
              << " ul_port=" << runtime->uplinkPort
              << " ul_source_port=" << runtime->uplinkSourcePort
              << " ue_ip="
              << (runtime->ueIndex < context->ueIps.size() ? ToString(context->ueIps[runtime->ueIndex]) : "0.0.0.0")
              << " pgw_rx_ul=" << runtime->ipObservedPgwRxUl
              << " pgw_fwd_ul=" << runtime->ipObservedPgwForwardUl
              << " pgw_local_ul=" << runtime->ipObservedPgwLocalDeliverUl
              << " pgw_drop_ul=" << runtime->ipObservedPgwDropUl
              << " remote_rx_ul=" << runtime->ipObservedRemoteRxUl
              << std::endl;
}

void
LogUnmatchedIpv4Trace(SnapshotContext* context, Ipv4TraceRole role, Ptr<const Packet> packet)
{
    if (context == nullptr || packet == nullptr || context->unmatchedIpv4TraceLogs >= 40)
    {
        return;
    }

    SnapshotContext::TraceParseCounters debug;
    uint16_t sourcePort = 0;
    uint16_t destinationPort = 0;
    const bool parsed = ExtractUdpTupleFromPacket(&debug, packet, &sourcePort, &destinationPort);
    const bool isRelevantTargetUplinkPort =
        sourcePort == 25003 || sourcePort == 25004 || sourcePort == 25007 || sourcePort == 25008 ||
        sourcePort == 25009 || destinationPort == 6003 || destinationPort == 6004 ||
        destinationPort == 6007 || destinationPort == 6008 || destinationPort == 6009;
    if (role != Ipv4TraceRole::PgwRx || !parsed || !isRelevantTargetUplinkPort)
    {
        return;
    }
    ++context->unmatchedIpv4TraceLogs;
    std::cerr << "[split-ns3] unmatched-ipv4-trace"
              << " count=" << context->unmatchedIpv4TraceLogs
              << " tick=" << context->tickIndex
              << " role=" << TraceRoleName(role)
              << " parsed=" << (parsed ? "true" : "false")
              << " src_port=" << sourcePort
              << " dst_port=" << destinationPort
              << " parse_ok_outer_udp=" << debug.parseOkOuterUdp
              << " parse_ok_gtpu_inner_udp=" << debug.parseOkGtpuInnerUdp
              << " parse_non_udp_outer=" << debug.parseNonUdpOuter
              << " parse_gtpu_no_inner_ipv4=" << debug.parseGtpuNoInnerIpv4
              << " parse_gtpu_inner_non_udp=" << debug.parseGtpuInnerNonUdp
              << " unmatched_ports=" << debug.unmatchedPorts
              << std::endl;
}

void
LogUnmatchedIpv4Decision(SnapshotContext* context,
                         Ipv4TraceRole role,
                         const Ipv4Header& ipv4Header,
                         Ptr<const Packet> payload)
{
    if (context == nullptr || payload == nullptr || context->unmatchedIpv4DecisionLogs >= 40)
    {
        return;
    }

    const bool relevantSource = ipv4Header.GetSource() == Ipv4Address("7.0.0.4") ||
                                ipv4Header.GetSource() == Ipv4Address("7.0.0.6");
    if (!relevantSource)
    {
        return;
    }

    const uint32_t payloadSize = payload->GetSize();
    if (payloadSize < 4)
    {
        return;
    }
    std::vector<uint8_t> bytes(payloadSize);
    payload->CopyData(bytes.data(), payloadSize);
    const uint16_t sourcePort = static_cast<uint16_t>((static_cast<uint16_t>(bytes[0]) << 8) |
                                                      static_cast<uint16_t>(bytes[1]));
    const uint16_t destinationPort = static_cast<uint16_t>((static_cast<uint16_t>(bytes[2]) << 8) |
                                                           static_cast<uint16_t>(bytes[3]));
    const bool relevantPort = sourcePort == 25003 || sourcePort == 25004 || sourcePort == 25007 ||
                              sourcePort == 25008 || sourcePort == 25009 || destinationPort == 6003 ||
                              destinationPort == 6004 || destinationPort == 6007 || destinationPort == 6008 ||
                              destinationPort == 6009;
    if (!relevantPort)
    {
        return;
    }

    ++context->unmatchedIpv4DecisionLogs;
    std::cerr << "[split-ns3] unmatched-ipv4-decision"
              << " count=" << context->unmatchedIpv4DecisionLogs
              << " tick=" << context->tickIndex
              << " role=" << TraceRoleName(role)
              << " src_ip=" << ToString(ipv4Header.GetSource())
              << " dst_ip=" << ToString(ipv4Header.GetDestination())
              << " src_port=" << sourcePort
              << " dst_port=" << destinationPort
              << std::endl;
}

void
OnIpv4Trace(SnapshotContext* context, Ipv4TraceRole role, Ptr<const Packet> packet, Ptr<Ipv4> ipv4, uint32_t interface)
{
    (void)ipv4;
    (void)interface;
    SnapshotContext::FlowRuntimeState* runtime = nullptr;
    bool uplink = false;
    if (!ResolveIpv4UdpFlow(context, packet, &runtime, &uplink) || runtime == nullptr)
    {
        if (role == Ipv4TraceRole::PgwRx)
        {
            LogUnmatchedIpv4Trace(context, role, packet);
        }
        return;
    }

    switch (role)
    {
    case Ipv4TraceRole::UeTx:
        if (uplink)
        {
            ++runtime->ipObservedUeTxUl;
        }
        break;
    case Ipv4TraceRole::UeRx:
        if (!uplink)
        {
            ++runtime->ipObservedUeRxDl;
        }
        break;
    case Ipv4TraceRole::RemoteTx:
        if (!uplink)

        {
            ++runtime->ipObservedRemoteTxDl;
        }
        break;
    case Ipv4TraceRole::RemoteRx:
        if (uplink)
        {
            ++runtime->ipObservedRemoteRxUl;
        }
        break;
    case Ipv4TraceRole::PgwRx:
        if (uplink)
        {
            ++runtime->ipObservedPgwRxUl;
            SnapshotContext::TraceParseCounters debug;
            uint16_t sourcePort = 0;
            uint16_t destinationPort = 0;
            if (ExtractUdpTupleFromPacket(&debug, packet, &sourcePort, &destinationPort))
            {
                Ipv4Header header;
                EmitDetailedTrace(context, runtime, role, nullptr, sourcePort, destinationPort, "matched-pgw-rx");
            }
        }
        break;
    case Ipv4TraceRole::PgwTx:
        if (uplink)
        {
            ++runtime->ipObservedPgwTxUl;
        }
        break;
    case Ipv4TraceRole::PgwLocalDeliver:
    case Ipv4TraceRole::PgwForward:
    case Ipv4TraceRole::PgwDrop:
        break;
    }
}

void
OnIpv4DecisionTrace(SnapshotContext* context,
                    Ipv4TraceRole role,
                    const Ipv4Header& header,
                    Ptr<const Packet> packet,
                    uint32_t interface)
{
    (void)interface;
    SnapshotContext::FlowRuntimeState* runtime = nullptr;
    bool uplink = false;
    if (!ResolveIpv4UdpFlowFromHeader(context, header, packet, &runtime, &uplink) || runtime == nullptr ||
        !uplink)
    {
        if (role == Ipv4TraceRole::PgwLocalDeliver || role == Ipv4TraceRole::PgwForward || role == Ipv4TraceRole::PgwDrop)
        {
            LogUnmatchedIpv4Decision(context, role, header, packet);
        }
        return;
    }

    switch (role)
    {
    case Ipv4TraceRole::PgwRx:
    case Ipv4TraceRole::PgwTx:
    case Ipv4TraceRole::UeTx:
    case Ipv4TraceRole::UeRx:
    case Ipv4TraceRole::RemoteTx:
    case Ipv4TraceRole::RemoteRx:
        break;
    case Ipv4TraceRole::PgwLocalDeliver:
        ++runtime->ipObservedPgwLocalDeliverUl;
        break;
    case Ipv4TraceRole::PgwForward:
        ++runtime->ipObservedPgwForwardUl;
        break;
    case Ipv4TraceRole::PgwDrop:
        ++runtime->ipObservedPgwDropUl;
        break;
    }

    const uint32_t payloadSize = packet != nullptr ? packet->GetSize() : 0;
    if (payloadSize >= 4)
    {
        std::vector<uint8_t> bytes(payloadSize);
        packet->CopyData(bytes.data(), payloadSize);
        const uint16_t sourcePort = static_cast<uint16_t>((static_cast<uint16_t>(bytes[0]) << 8) |
                                                          static_cast<uint16_t>(bytes[1]));
        const uint16_t destinationPort = static_cast<uint16_t>((static_cast<uint16_t>(bytes[2]) << 8) |
                                                               static_cast<uint16_t>(bytes[3]));
        EmitDetailedTrace(context, runtime, role, &header, sourcePort, destinationPort, "matched-decision");
    }
}

void
OnIpv4DropTrace(SnapshotContext* context,
                const Ipv4Header& header,
                Ptr<const Packet> packet,
                Ipv4L3Protocol::DropReason reason,
                Ptr<Ipv4> ipv4,
                uint32_t interface)
{
    (void)reason;
    (void)ipv4;
    (void)interface;
    SnapshotContext::FlowRuntimeState* runtime = nullptr;
    bool uplink = false;
    if (!ResolveIpv4UdpFlowFromHeader(context, header, packet, &runtime, &uplink) || runtime == nullptr ||
        !uplink)
    {
        LogUnmatchedIpv4Decision(context, Ipv4TraceRole::PgwDrop, header, packet);
        return;
    }
    ++runtime->ipObservedPgwDropUl;
    const uint32_t payloadSize = packet != nullptr ? packet->GetSize() : 0;
    if (payloadSize >= 4)
    {
        std::vector<uint8_t> bytes(payloadSize);
        packet->CopyData(bytes.data(), payloadSize);
        const uint16_t sourcePort = static_cast<uint16_t>((static_cast<uint16_t>(bytes[0]) << 8) |
                                                          static_cast<uint16_t>(bytes[1]));
        const uint16_t destinationPort = static_cast<uint16_t>((static_cast<uint16_t>(bytes[2]) << 8) |
                                                               static_cast<uint16_t>(bytes[3]));
        EmitDetailedTrace(context, runtime, Ipv4TraceRole::PgwDrop, &header, sourcePort, destinationPort, "matched-drop-trace");
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
ResolvePacketSizeBytes(const FlowProfile& profile, bool downlink)
{
    const double value = downlink ? profile.dlPacketSizeBytes : profile.ulPacketSizeBytes;
    if (value > 0.0)
    {
        return value;
    }
    return profile.packetSizeBytes;
}

double
ResolveArrivalRatePps(const FlowProfile& profile, bool downlink)
{
    const double value = downlink ? profile.dlArrivalRatePps : profile.ulArrivalRatePps;
    if (value > 0.0)
    {
        return value;
    }
    return profile.arrivalRatePps;
}

double
RequestedBandwidthDlMbps(const FlowProfile& profile)
{
    if (profile.bandwidthDlMbps > 0.0)
    {
        return profile.bandwidthDlMbps;
    }
    const double packetSizeBytes = ResolvePacketSizeBytes(profile, true);
    const double arrivalRatePps = ResolveArrivalRatePps(profile, true);
    if (packetSizeBytes <= 0.0 || arrivalRatePps <= 0.0)
    {
        return 0.0;
    }
    return arrivalRatePps * packetSizeBytes * 8.0 / 1e6;
}

double
RequestedBandwidthUlMbps(const FlowProfile& profile)
{
    if (profile.bandwidthUlMbps > 0.0)
    {
        return profile.bandwidthUlMbps;
    }
    const double packetSizeBytes = ResolvePacketSizeBytes(profile, false);
    const double arrivalRatePps = ResolveArrivalRatePps(profile, false);
    if (packetSizeBytes <= 0.0 || arrivalRatePps <= 0.0)
    {
        return 0.0;
    }
    return arrivalRatePps * packetSizeBytes * 8.0 / 1e6;
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

std::pair<double, double>
ResolveTddFractions(const std::string& tddPattern)
{
    const auto tokens = SplitString(tddPattern, '|');
    if (tokens.empty())
    {
        NS_FATAL_ERROR("split-mode TDD pattern produced no slots: " << tddPattern);
    }
    double dlWeight = 0.0;
    double ulWeight = 0.0;
    double totalWeight = 0.0;
    for (const auto& token : tokens)
    {
        if (token.empty())
        {
            continue;
        }
        const char slot = token[0];
        if (slot == 'D' || slot == 'L')
        {
            dlWeight += 1.0;
            totalWeight += 1.0;
        }
        else if (slot == 'U')
        {
            ulWeight += 1.0;
            totalWeight += 1.0;
        }
        else if (slot == 'F')
        {
            dlWeight += 0.5;
            ulWeight += 0.5;
            totalWeight += 1.0;
        }
    }
    if (totalWeight <= 0.0)
    {
        NS_FATAL_ERROR("split-mode TDD pattern has no usable slots: " << tddPattern);
    }
    return {dlWeight / totalWeight, ulWeight / totalWeight};
}

double
EstimateNoiseFloorDbm(double bandwidthHz, double noiseFigureDb)
{
    return -174.0 + 10.0 * std::log10(std::max(1.0, bandwidthHz)) + noiseFigureDb;
}

double
EstimateDistanceMeters(const SnapshotContext* context, uint32_t ueIndex)
{
    if (ueIndex >= context->ueToGnb.size() || ueIndex >= context->uePositions.size())
    {
        return 0.0;
    }
    const uint32_t gnbIndex = context->ueToGnb[ueIndex];
    if (gnbIndex >= context->gnbPositions.size())
    {
        return 0.0;
    }
    const Vector uePosition = context->uePositions[ueIndex];
    const Vector gnbPosition = context->gnbPositions[gnbIndex];
    const Vector delta(uePosition.x - gnbPosition.x, uePosition.y - gnbPosition.y, uePosition.z - gnbPosition.z);
    return std::sqrt(delta.x * delta.x + delta.y * delta.y + delta.z * delta.z);
}

double
EstimateDlSinrDb(const SnapshotContext* context, uint32_t ueIndex)
{
    const double distanceMeters = std::max(1.0, EstimateDistanceMeters(context, ueIndex));
    const double distanceKm = distanceMeters / 1000.0;
    const double frequencyMhz = std::max(1.0, context->radioConfig.centralFrequencyHz / 1e6);
    const double freeSpacePathlossDb = 32.4 + 20.0 * std::log10(distanceKm) + 20.0 * std::log10(frequencyMhz);
    const double receivedPowerDbm = context->radioConfig.gnbTxPowerDbm - freeSpacePathlossDb;
    const double noiseFloorDbm =
        EstimateNoiseFloorDbm(context->radioConfig.bandwidthHz, context->radioConfig.ueNoiseFigureDb);
    return receivedPowerDbm - noiseFloorDbm;
}

double
EstimateUlSinrDb(const SnapshotContext* context, uint32_t ueIndex)
{
    const double distanceMeters = std::max(1.0, EstimateDistanceMeters(context, ueIndex));
    const double distanceKm = distanceMeters / 1000.0;
    const double frequencyMhz = std::max(1.0, context->radioConfig.centralFrequencyHz / 1e6);
    const double freeSpacePathlossDb = 32.4 + 20.0 * std::log10(distanceKm) + 20.0 * std::log10(frequencyMhz);
    const double receivedPowerDbm = context->radioConfig.ueTxPowerDbm - freeSpacePathlossDb;
    const double noiseFloorDbm =
        EstimateNoiseFloorDbm(context->radioConfig.bandwidthHz, context->radioConfig.gnbNoiseFigureDb);
    return receivedPowerDbm - noiseFloorDbm;
}

double
EstimateSpectralEfficiencyBpsHz(double sinrDb)
{
    const double sinrLinear = std::max(0.0, DbToLinear(sinrDb));
    return std::max(0.1, 0.72 * std::log2(1.0 + sinrLinear));
}

double
EstimateDirectionalCapacityMbps(const SnapshotContext* context,
                                uint32_t gnbIndex,
                                bool downlink,
                                uint32_t activeUeCount)
{
    const auto [dlFraction, ulFraction] = ResolveTddFractions(context->radioConfig.tddPattern);
    const double directionFraction = downlink ? dlFraction : ulFraction;
    double sinrSumDb = 0.0;
    uint32_t sinrSamples = 0;
    for (uint32_t ueIndex = 0; ueIndex < context->ueToGnb.size(); ++ueIndex)
    {
        if (context->ueToGnb[ueIndex] != gnbIndex)
        {
            continue;
        }
        sinrSumDb += downlink ? EstimateDlSinrDb(context, ueIndex) : EstimateUlSinrDb(context, ueIndex);
        ++sinrSamples;
    }
    const double representativeSinrDb = sinrSamples > 0 ? (sinrSumDb / static_cast<double>(sinrSamples)) : -5.0;
    const double spectralEfficiency = EstimateSpectralEfficiencyBpsHz(representativeSinrDb);
    const double sharingPenalty = 1.0 / std::sqrt(std::max<uint32_t>(1, activeUeCount));
    return context->radioConfig.bandwidthHz * directionFraction * spectralEfficiency * sharingPenalty / 1e6;
}

void
UpdateUeAndGnbRadioTelemetry(SnapshotContext* context)
{
    context->ueRadioTelemetry.assign(context->ueNum, SnapshotContext::UeRadioTelemetry{});
    if (context->gnbRadioTelemetry.size() != context->gNbNum)
    {
        context->gnbRadioTelemetry.assign(context->gNbNum, SnapshotContext::GnbRuntimeTelemetry{});
    }
    for (auto& telemetry : context->gnbRadioTelemetry)
    {
        telemetry.activeUeCount = 0;
        telemetry.ulScheduledBytes = 0;
        telemetry.dlScheduledBytes = 0;
        telemetry.ulPrbUtilization = 0.0;
        telemetry.dlPrbUtilization = 0.0;
        telemetry.ulUsedReg = 0;
        telemetry.dlUsedReg = 0;
        telemetry.ulAvailableReg = 0;
        telemetry.dlAvailableReg = 0;
    }

    std::vector<std::map<uint32_t, bool>> gnbActiveUes(context->gNbNum);
    const double tickSeconds = std::max(0.001, static_cast<double>(context->tickMs) / 1000.0);
    for (auto& [port, runtime] : context->flowRuntimeByPort)
    {
        const uint32_t ueIndex = runtime.ueIndex;
        if (ueIndex >= context->ueToGnb.size())
        {
            continue;
        }
        const uint32_t gnbIndex = context->ueToGnb[ueIndex];
        if (gnbIndex >= context->gnbRadioTelemetry.size())
        {
            continue;
        }

        auto& ueTelemetry = context->ueRadioTelemetry[ueIndex];
        ueTelemetry.servingGnbIndex = gnbIndex;
        ueTelemetry.dlDataSinrDb = EstimateDlSinrDb(context, ueIndex);
        ueTelemetry.hasDlDataSinr = true;
        ueTelemetry.rsrpDbm =
            EstimateNoiseFloorDbm(context->radioConfig.bandwidthHz, context->radioConfig.ueNoiseFigureDb) +
            ueTelemetry.dlDataSinrDb;
        ueTelemetry.rsrqDb = ueTelemetry.dlDataSinrDb - 3.0;
        ueTelemetry.hasMeasurement = true;

        const double dlDeficitMbps =
            std::max(0.0, RequestedBandwidthDlMbps(runtime.profile) - runtime.profile.allocatedBandwidthDlMbps);
        const double ulDeficitMbps =
            std::max(0.0, RequestedBandwidthUlMbps(runtime.profile) - runtime.profile.allocatedBandwidthUlMbps);
        ueTelemetry.dlBufferBytes += static_cast<uint64_t>(
            dlDeficitMbps * 1e6 / 8.0 * static_cast<double>(context->tickMs) / 1000.0);
        ueTelemetry.ulBufferBytes += static_cast<uint64_t>(
            ulDeficitMbps * 1e6 / 8.0 * static_cast<double>(context->tickMs) / 1000.0);

        const uint64_t ranDlRxPkts = runtime.ipObservedUeRxDl;
        const uint64_t ranUlRxPkts = runtime.ipObservedPgwRxUl;
        const uint64_t deltaDlPkts =
            ranDlRxPkts >= runtime.lastSnapshotPacketReceivedDl ? (ranDlRxPkts - runtime.lastSnapshotPacketReceivedDl) : 0;
        const uint64_t deltaUlPkts =
            ranUlRxPkts >= runtime.lastSnapshotPacketReceivedUl ? (ranUlRxPkts - runtime.lastSnapshotPacketReceivedUl) : 0;
        const uint64_t dlPacketSizeBytes =
            static_cast<uint64_t>(std::max(64.0, ResolvePacketSizeBytes(runtime.profile, true)));
        const uint64_t ulPacketSizeBytes =
            static_cast<uint64_t>(std::max(64.0, ResolvePacketSizeBytes(runtime.profile, false)));

        auto& gnbTelemetry = context->gnbRadioTelemetry[gnbIndex];
        gnbTelemetry.dlScheduledBytes += deltaDlPkts * dlPacketSizeBytes;
        gnbTelemetry.ulScheduledBytes += deltaUlPkts * ulPacketSizeBytes;
        if (runtime.profile.enabled)
        {
            gnbActiveUes[gnbIndex][ueIndex] = true;
        }
    }

    for (uint32_t gnbIndex = 0; gnbIndex < context->gNbNum; ++gnbIndex)
    {
        auto& telemetry = context->gnbRadioTelemetry[gnbIndex];
        telemetry.activeUeCount = static_cast<uint32_t>(gnbActiveUes[gnbIndex].size());
        const double theoreticalDlCapacityMbps =
            EstimateDirectionalCapacityMbps(context, gnbIndex, true, telemetry.activeUeCount);
        const double theoreticalUlCapacityMbps =
            EstimateDirectionalCapacityMbps(context, gnbIndex, false, telemetry.activeUeCount);
        const double observedDlMbps = telemetry.dlScheduledBytes * 8.0 / tickSeconds / 1e6;
        const double observedUlMbps = telemetry.ulScheduledBytes * 8.0 / tickSeconds / 1e6;
        telemetry.recentDlThroughputMbps.push_back(observedDlMbps);
        telemetry.recentUlThroughputMbps.push_back(observedUlMbps);
        constexpr size_t kCapacityWindow = 5;
        while (telemetry.recentDlThroughputMbps.size() > kCapacityWindow)
        {
            telemetry.recentDlThroughputMbps.pop_front();
        }
        while (telemetry.recentUlThroughputMbps.size() > kCapacityWindow)
        {
            telemetry.recentUlThroughputMbps.pop_front();
        }
        const double recentPeakDlMbps = telemetry.recentDlThroughputMbps.empty()
                                            ? 0.0
                                            : *std::max_element(telemetry.recentDlThroughputMbps.begin(),
                                                                telemetry.recentDlThroughputMbps.end());
        const double recentPeakUlMbps = telemetry.recentUlThroughputMbps.empty()
                                            ? 0.0
                                            : *std::max_element(telemetry.recentUlThroughputMbps.begin(),
                                                                telemetry.recentUlThroughputMbps.end());
        telemetry.radioCapacityDlMbps = std::min(theoreticalDlCapacityMbps, std::max(observedDlMbps, recentPeakDlMbps));
        telemetry.radioCapacityUlMbps = std::min(theoreticalUlCapacityMbps, std::max(observedUlMbps, recentPeakUlMbps));
        telemetry.radioCapacityUnknown = telemetry.activeUeCount == 0;
        telemetry.radioCapacitySource = telemetry.radioCapacityUnknown ? "idle" : "measured_capped_by_theory";
        telemetry.dlPrbUtilization = theoreticalDlCapacityMbps > 0.0
                                         ? std::clamp(observedDlMbps / theoreticalDlCapacityMbps, 0.0, 1.0)
                                         : 0.0;
        telemetry.ulPrbUtilization = theoreticalUlCapacityMbps > 0.0
                                         ? std::clamp(observedUlMbps / theoreticalUlCapacityMbps, 0.0, 1.0)
                                         : 0.0;
        telemetry.dlUsedReg = telemetry.dlScheduledBytes;
        telemetry.ulUsedReg = telemetry.ulScheduledBytes;
        telemetry.dlAvailableReg = static_cast<uint64_t>(
            std::max(0.0, theoreticalDlCapacityMbps) * 1e6 / 8.0 * tickSeconds);
        telemetry.ulAvailableReg = static_cast<uint64_t>(
            std::max(0.0, theoreticalUlCapacityMbps) * 1e6 / 8.0 * tickSeconds);
    }

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
    std::map<uint32_t, std::vector<SnapshotContext::FlowRuntimeState*>> flowsByGnb;
    for (auto& [port, runtime] : context->flowRuntimeByPort)
    {
        if (runtime.ueIndex >= context->ueToGnb.size())
        {
            continue;
        }
        const uint32_t gnbIndex = context->ueToGnb[runtime.ueIndex];
        flowsByGroup[{gnbIndex, runtime.profile.sliceRef}].push_back(&runtime);
        flowsByGnb[gnbIndex].push_back(&runtime);
    }

    std::map<uint32_t, double> gnbRequestedDlMbps;
    std::map<uint32_t, double> gnbRequestedUlMbps;
    std::map<uint32_t, double> gnbRadioCapacityDlMbps;
    std::map<uint32_t, double> gnbRadioCapacityUlMbps;
    for (const auto& [gnbIndex, runtimes] : flowsByGnb)
    {
        for (const auto* runtime : runtimes)
        {
            if (!runtime->profile.enabled)
            {
                continue;
            }
            gnbRequestedDlMbps[gnbIndex] += RequestedBandwidthDlMbps(runtime->profile);
            gnbRequestedUlMbps[gnbIndex] += RequestedBandwidthUlMbps(runtime->profile);
        }
        if (gnbIndex < context->gnbRadioTelemetry.size())
        {
            gnbRadioCapacityDlMbps[gnbIndex] =
                std::max(0.0, context->gnbRadioTelemetry[gnbIndex].radioCapacityDlMbps);
            gnbRadioCapacityUlMbps[gnbIndex] =
                std::max(0.0, context->gnbRadioTelemetry[gnbIndex].radioCapacityUlMbps);
        }
    }

    for (auto& [groupKey, runtimes] : flowsByGroup)
    {
        const uint32_t gnbIndex = groupKey.first;
        const std::string& sliceId = groupKey.second;
        const auto resourceIt = context->sliceResources.find(sliceId);
        const double capacityDl = resourceIt != context->sliceResources.end()
                                      ? resourceIt->second.capacityDlMbps
                                      : std::numeric_limits<double>::infinity();
        const double capacityUl = resourceIt != context->sliceResources.end()
                                      ? resourceIt->second.capacityUlMbps
                                      : std::numeric_limits<double>::infinity();
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

        double groupRequestedDlMbps = 0.0;
        double groupRequestedUlMbps = 0.0;
        for (const auto* runtime : runtimes)
        {
            if (!runtime->profile.enabled)
            {
                continue;
            }
            groupRequestedDlMbps += RequestedBandwidthDlMbps(runtime->profile);
            groupRequestedUlMbps += RequestedBandwidthUlMbps(runtime->profile);
        }

        const double requestedDlOnGnb = std::max(0.0, gnbRequestedDlMbps[gnbIndex]);
        const double requestedUlOnGnb = std::max(0.0, gnbRequestedUlMbps[gnbIndex]);
        const double gnbCapacityShareDl =
            requestedDlOnGnb > 0.0
                ? gnbRadioCapacityDlMbps[gnbIndex] *
                      (std::max(0.0, groupRequestedDlMbps) / requestedDlOnGnb)
                : gnbRadioCapacityDlMbps[gnbIndex];
        const double gnbCapacityShareUl =
            requestedUlOnGnb > 0.0
                ? gnbRadioCapacityUlMbps[gnbIndex] *
                      (std::max(0.0, groupRequestedUlMbps) / requestedUlOnGnb)
                : gnbRadioCapacityUlMbps[gnbIndex];
        const double effectiveCapacityDl =
            gnbRadioCapacityDlMbps.count(gnbIndex) > 0 ? std::min(capacityDl, gnbCapacityShareDl) : capacityDl;
        const double effectiveCapacityUl =
            gnbRadioCapacityUlMbps.count(gnbIndex) > 0 ? std::min(capacityUl, gnbCapacityShareUl) : capacityUl;

        auto allocateDirection = [&](bool downlink, double capacity) {
            double guaranteedSum = 0.0;
            for (auto* runtime : runtimes)
            {
                if (!runtime->profile.enabled)
                {
                    if (downlink)
                    {
                        runtime->profile.allocatedBandwidthDlMbps = 0.0;
                    }
                    else
                    {
                        runtime->profile.allocatedBandwidthUlMbps = 0.0;
                    }
                    continue;
                }
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
                if (!runtime->profile.enabled)
                {
                    continue;
                }
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
                    if (!runtime->profile.enabled)
                    {
                        continue;
                    }
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

        allocateDirection(true, effectiveCapacityDl);
        allocateDirection(false, effectiveCapacityUl);

        for (auto* runtime : runtimes)
        {
            if (!runtime->profile.enabled)
            {
                runtime->profile.allocatedBandwidthDlMbps = 0.0;
                runtime->profile.allocatedBandwidthUlMbps = 0.0;
                continue;
            }
            telemetry.allocatedDlMbps += runtime->profile.allocatedBandwidthDlMbps;
            telemetry.allocatedUlMbps += runtime->profile.allocatedBandwidthUlMbps;
            const double deficitMbps = std::max(0.0, RequestedBandwidthDlMbps(runtime->profile) -
                                                         runtime->profile.allocatedBandwidthDlMbps) +
                                      std::max(0.0, RequestedBandwidthUlMbps(runtime->profile) -
                                                         runtime->profile.allocatedBandwidthUlMbps);
            const double flowQueueBytes =
                deficitMbps * 1e6 / 8.0 * static_cast<double>(context->tickMs) / 1000.0;
            telemetry.queueBytes += flowQueueBytes;
            const double averagePacketSizeBytes =
                std::max(1.0, (ResolvePacketSizeBytes(runtime->profile, true) +
                               ResolvePacketSizeBytes(runtime->profile, false)) /
                                      2.0);
            telemetry.droppedPackets += averagePacketSizeBytes > 0.0
                                            ? flowQueueBytes / averagePacketSizeBytes
                                            : 0.0;
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
        const uint64_t rxBytesDl = runtime.downlinkSink != nullptr ? runtime.downlinkSink->GetTotalRx() : 0;
        const uint64_t rxBytesUl = runtime.uplinkSink != nullptr ? runtime.uplinkSink->GetTotalRx() : 0;
        const uint64_t dlPacketSizeBytes =
            static_cast<uint64_t>(std::max(1.0, ResolvePacketSizeBytes(runtime.profile, true)));
        const uint64_t ulPacketSizeBytes =
            static_cast<uint64_t>(std::max(1.0, ResolvePacketSizeBytes(runtime.profile, false)));
        output << "{" << Quote("flow_id") << ":" << Quote(runtime.profile.flowId) << ","
               << Quote("allocated_bandwidth_dl_mbps") << ":" << runtime.profile.allocatedBandwidthDlMbps << ","
               << Quote("allocated_bandwidth_ul_mbps") << ":" << runtime.profile.allocatedBandwidthUlMbps << ","
               << Quote("packet_size_bytes") << ":" << runtime.profile.packetSizeBytes << ","
               << Quote("arrival_rate_pps") << ":" << runtime.profile.arrivalRatePps << ","
               << Quote("dl_packet_size_bytes") << ":" << runtime.profile.dlPacketSizeBytes << ","
               << Quote("ul_packet_size_bytes") << ":" << runtime.profile.ulPacketSizeBytes << ","
               << Quote("dl_arrival_rate_pps") << ":" << runtime.profile.dlArrivalRatePps << ","
               << Quote("ul_arrival_rate_pps") << ":" << runtime.profile.ulArrivalRatePps << ","
               << Quote("enabled") << ":" << (runtime.profile.enabled ? "true" : "false") << ","
               << Quote("app_rx_packets_dl") << ":" << (rxBytesDl / dlPacketSizeBytes) << ","
               << Quote("app_rx_packets_ul") << ":" << (rxBytesUl / ulPacketSizeBytes) << "}";
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
    std::cerr << "[split-ns3] tick=" << context->tickIndex
              << " sim_time_ms=" << Simulator::Now().GetMilliSeconds() << std::endl;
    const uint32_t reloadEveryTicks = std::max<uint32_t>(1, std::max<uint32_t>(1, context->policyReloadMs) /
                                                                std::max<uint32_t>(1, context->tickMs));
    if (!context->flowRuntimeByPort.empty() && context->tickIndex % reloadEveryTicks == 0)
    {
        ReloadFlowProfiles(context);
        ApplySlaDrivenAllocations(context);
    }
    UpdateUeAndGnbRadioTelemetry(context);
    for (auto& [port, runtime] : context->flowRuntimeByPort)
    {
        const uint64_t rxBytesDl = runtime.downlinkSink != nullptr ? runtime.downlinkSink->GetTotalRx() : 0;
        const uint64_t rxBytesUl = runtime.uplinkSink != nullptr ? runtime.uplinkSink->GetTotalRx() : 0;
        const uint64_t dlPacketSizeBytes =
            static_cast<uint64_t>(std::max(1.0, ResolvePacketSizeBytes(runtime.profile, true)));
        const uint64_t ulPacketSizeBytes =
            static_cast<uint64_t>(std::max(1.0, ResolvePacketSizeBytes(runtime.profile, false)));
        std::cerr << "[split-ns3] flow-state"
                  << " tick=" << context->tickIndex
                  << " flow_id=" << runtime.profile.flowId
                  << " enabled=" << (runtime.profile.enabled ? "true" : "false")
                  << " ue_index=" << runtime.ueIndex
                  << " ue_ip=" << (runtime.ueIndex < context->ueIps.size()
                                       ? ToString(context->ueIps[runtime.ueIndex])
                                       : "0.0.0.0")
                  << " dl_port=" << runtime.port
                  << " ul_port=" << runtime.uplinkPort
                  << " ul_source_port=" << runtime.uplinkSourcePort
                  << " ul_peer_ip=" << ToString(context->remoteHostAddress)
                  << " alloc_dl_mbps=" << runtime.profile.allocatedBandwidthDlMbps
                  << " alloc_ul_mbps=" << runtime.profile.allocatedBandwidthUlMbps
                  << " app_tx_pkts_dl=" << runtime.appTxPacketsDl
                  << " app_tx_pkts_ul=" << runtime.appTxPacketsUl
                  << " app_rx_pkts_dl=" << (rxBytesDl / dlPacketSizeBytes)
                  << " app_rx_pkts_ul=" << (rxBytesUl / ulPacketSizeBytes)
                  << " app_send_err_dl=" << runtime.appSendErrorsDl
                  << " app_send_err_ul=" << runtime.appSendErrorsUl
                  << " ip_ue_tx_ul=" << runtime.ipObservedUeTxUl
                  << " ip_pgw_rx_ul=" << runtime.ipObservedPgwRxUl
                  << " ip_pgw_tx_ul=" << runtime.ipObservedPgwTxUl
                  << " ip_pgw_local_ul=" << runtime.ipObservedPgwLocalDeliverUl
                  << " ip_pgw_fwd_ul=" << runtime.ipObservedPgwForwardUl
                  << " ip_pgw_drop_ul=" << runtime.ipObservedPgwDropUl
                  << " ip_remote_rx_ul=" << runtime.ipObservedRemoteRxUl
                  << " ip_remote_tx_dl=" << runtime.ipObservedRemoteTxDl
                  << " ip_ue_rx_dl=" << runtime.ipObservedUeRxDl
                  << std::endl;
        MaybeEmitUplinkStallTrace(context, &runtime);
    }

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

    }
    for (const auto& link : context->n3Links)
    {
        if (!first)
        {
            json << ",";
        }
        first = false;
        json << "{" << Quote("source") << ":" << Quote("ran-node-" + std::to_string(link.gnbIndex + 1)) << ","
             << Quote("target") << ":" << Quote("core-node-" + std::to_string(link.upfIndex + 1)) << ","
             << Quote("type") << ":" << Quote("tunneled_via") << ","
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
        const auto& radioTelemetry =
            gnb < context->gnbRadioTelemetry.size() ? context->gnbRadioTelemetry[gnb]
                                                    : SnapshotContext::GnbRuntimeTelemetry{};
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
        json << "]," << Quote("dst_upfs") << ":[";
        bool firstUpf = true;
        for (const auto& link : context->n3Links)
        {
            if (link.gnbIndex != gnb)
            {
                continue;
            }
            if (!firstUpf)
            {
                json << ",";
            }
            firstUpf = false;
            json << Quote(context->upfNames[link.upfIndex]);
        }
        json << "],"
             << Quote("telemetry") << ":{" << Quote("ran") << ":{" << Quote("active_ue_count") << ":"
             << radioTelemetry.activeUeCount << "," << Quote("ul_scheduled_bytes") << ":"
             << radioTelemetry.ulScheduledBytes << "," << Quote("dl_scheduled_bytes") << ":"
             << radioTelemetry.dlScheduledBytes << "," << Quote("ul_prb_utilization") << ":"
             << radioTelemetry.ulPrbUtilization << "," << Quote("dl_prb_utilization") << ":"
             << radioTelemetry.dlPrbUtilization << "," << Quote("radio_capacity_ul_mbps") << ":"
             << radioTelemetry.radioCapacityUlMbps << "," << Quote("radio_capacity_dl_mbps") << ":"
             << radioTelemetry.radioCapacityDlMbps << "," << Quote("radio_capacity_unknown") << ":"
             << (radioTelemetry.radioCapacityUnknown ? "true" : "false") << ","
             << Quote("radio_capacity_source") << ":" << Quote(radioTelemetry.radioCapacitySource) << "}}}";
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
        const auto& ueRadio =
            ue < context->ueRadioTelemetry.size() ? context->ueRadioTelemetry[ue]
                                                  : SnapshotContext::UeRadioTelemetry{};
        json << "{" << Quote("ue_id") << ":" << Quote("ue-" + std::to_string(ue + 1)) << ","
             << Quote("supi") << ":" << Quote(context->ueSupis[ue]) << ","
             << Quote("gnb_id") << ":" << Quote("gnb-" + std::to_string(context->ueToGnb[ue] + 1)) << ","
             << Quote("slice_id") << ":" << resolvedSliceId << ","
             << Quote("ip_address") << ":" << Quote(ToString(context->ueIps[ue])) << ","
             << Quote("sessions") << ":[";
        bool firstSession = true;
        for (const auto& [port, runtime] : context->flowRuntimeByPort)
        {
            if (runtime.ueIndex != ue)
            {
                continue;
            }
            if (!firstSession)
            {
                json << ",";
            }
            firstSession = false;
            json << "{"
                 << Quote("session_ref") << ":" << Quote(runtime.profile.sessionRef) << ","
                 << Quote("slice_id") << ":" << Quote(runtime.profile.sliceRef) << ","

                 << Quote("flow_id") << ":" << Quote(runtime.profile.flowId) << ","
                 << Quote("enabled") << ":" << (runtime.profile.enabled ? "true" : "false") << "}";
        }
        json << "],"
             << Quote("telemetry") << ":{" << Quote("ran") << ":{" << Quote("sinr_db") << ":"
             << (ueRadio.hasDlDataSinr ? ueRadio.dlDataSinrDb : 0.0) << ","
             << Quote("distance_to_serving_gnb_m") << ":" << EstimateDistanceMeters(context, ue) << ","
             << Quote("ul_buffer_bytes") << ":" << ueRadio.ulBufferBytes << ","
             << Quote("dl_buffer_bytes") << ":" << ueRadio.dlBufferBytes << ","
             << Quote("rsrp_dbm") << ":" << (ueRadio.hasMeasurement ? ueRadio.rsrpDbm : 0.0) << ","
             << Quote("rsrq_db") << ":" << (ueRadio.hasMeasurement ? ueRadio.rsrqDb : 0.0) << "}}}";
    }
    json << "],";

    json << Quote("flows") << ":[";
    bool firstFlow = true;
    auto appendFlow = [&](SnapshotContext::FlowRuntimeState* runtimeState,
                          const FlowProfile* profile,
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
        if (profile == nullptr || !profile->enabled)
        {
            return;
        }
        const uint32_t gnbIndex = context->ueToGnb[ueIndex];
        const uint32_t upfIndex = ResolveFlowUpfIndex(context, gnbIndex, profile);
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
        const double dlPacketSizeBytes = profile != nullptr ? ResolvePacketSizeBytes(*profile, true) : packetSizeBytes;
        const double ulPacketSizeBytes = profile != nullptr ? ResolvePacketSizeBytes(*profile, false) : packetSizeBytes;
        const double dlArrivalRatePps = profile != nullptr ? ResolveArrivalRatePps(*profile, true) : arrivalRatePps;
        const double ulArrivalRatePps = profile != nullptr ? ResolveArrivalRatePps(*profile, false) : arrivalRatePps;
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
        const uint64_t ranUlTxPkts = runtimeState != nullptr ? runtimeState->ipObservedUeTxUl : 0;
        const uint64_t ranUlRxPkts = runtimeState != nullptr ? runtimeState->ipObservedPgwRxUl : 0;
        const uint64_t ranDlTxPkts = runtimeState != nullptr ? runtimeState->ipObservedRemoteTxDl : 0;
        const uint64_t ranDlRxPkts = runtimeState != nullptr ? runtimeState->ipObservedUeRxDl : 0;
        const double ranUlDeliveryRatio = ranUlTxPkts > 0
                                              ? static_cast<double>(ranUlRxPkts) / static_cast<double>(ranUlTxPkts)
                                              : 1.0;
        const uint64_t ranUlDropBeforePgwPkts = ranUlTxPkts > ranUlRxPkts ? (ranUlTxPkts - ranUlRxPkts) : 0;
        const auto& gnbRadio = gnbIndex < context->gnbRadioTelemetry.size()
                                   ? context->gnbRadioTelemetry[gnbIndex]
                                   : SnapshotContext::GnbRuntimeTelemetry{};
        uint64_t tickDeltaPacketSent = txPackets;
        uint64_t tickDeltaPacketReceived = rxPackets;
        if (runtimeState != nullptr)
        {
            tickDeltaPacketSent =
                txPackets >= runtimeState->lastSnapshotPacketSent ? (txPackets - runtimeState->lastSnapshotPacketSent) : txPackets;
            tickDeltaPacketReceived =
                rxPackets >= runtimeState->lastSnapshotPacketReceived ? (rxPackets - runtimeState->lastSnapshotPacketReceived) : rxPackets;
            runtimeState->lastSnapshotPacketSent = txPackets;
            runtimeState->lastSnapshotPacketReceived = rxPackets;
            runtimeState->lastSnapshotPacketSentDl = ranDlTxPkts;
            runtimeState->lastSnapshotPacketSentUl = ranUlTxPkts;
            runtimeState->lastSnapshotPacketReceivedDl = ranDlRxPkts;
            runtimeState->lastSnapshotPacketReceivedUl = ranUlRxPkts;
        }

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
             << Quote("packet_size_dl") << ":" << dlPacketSizeBytes << ","
             << Quote("packet_size_ul") << ":" << ulPacketSizeBytes << ","
             << Quote("filter") << ":" << Quote(policyFilter) << ","
             << Quote("arrival_rate") << ":" << arrivalRatePps << ","
             << Quote("arrival_rate_dl") << ":" << dlArrivalRatePps << ","
             << Quote("arrival_rate_ul") << ":" << ulArrivalRatePps << "},"
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
             << Quote("throughput_ul") << ":" << throughputUl << ","
             << Quote("tick_delta_packet_sent") << ":" << tickDeltaPacketSent << ","
             << Quote("tick_delta_packet_received") << ":" << tickDeltaPacketReceived << ","
             << Quote("ip_path") << ":{" << Quote("ue_tx_ul") << ":"
             << (runtimeState != nullptr ? runtimeState->ipObservedUeTxUl : 0) << ","
             << Quote("pgw_rx_ul") << ":" << (runtimeState != nullptr ? runtimeState->ipObservedPgwRxUl : 0)
             << "," << Quote("pgw_tx_ul") << ":"
             << (runtimeState != nullptr ? runtimeState->ipObservedPgwTxUl : 0) << ","
             << Quote("pgw_local_deliver_ul") << ":"
             << (runtimeState != nullptr ? runtimeState->ipObservedPgwLocalDeliverUl : 0) << ","
             << Quote("pgw_forward_ul") << ":"
             << (runtimeState != nullptr ? runtimeState->ipObservedPgwForwardUl : 0) << ","
             << Quote("pgw_drop_ul") << ":"
             << (runtimeState != nullptr ? runtimeState->ipObservedPgwDropUl : 0) << ","
             << Quote("remote_rx_ul") << ":"
             << (runtimeState != nullptr ? runtimeState->ipObservedRemoteRxUl : 0) << ","
             << Quote("remote_tx_dl") << ":"
             << (runtimeState != nullptr ? runtimeState->ipObservedRemoteTxDl : 0) << ","
             << Quote("ue_rx_dl") << ":" << (runtimeState != nullptr ? runtimeState->ipObservedUeRxDl : 0)
             << "},"
             << Quote("ran") << ":{" << Quote("ul") << ":{" << Quote("tx_pkts") << ":" << ranUlTxPkts << ","
             << Quote("rx_pkts") << ":" << ranUlRxPkts << "," << Quote("drop_before_pgw_pkts") << ":"
             << ranUlDropBeforePgwPkts << "," << Quote("delivery_ratio") << ":" << ranUlDeliveryRatio
             << "}," << Quote("dl") << ":{" << Quote("tx_pkts") << ":" << ranDlTxPkts << ","
             << Quote("rx_pkts") << ":" << ranDlRxPkts << "}}";
        json << "},"
             << Quote("allocation") << ":{" << Quote("optimize_requested") << ":"
             << (optimizeRequested ? "true" : "false") << ","
             << Quote("qos_ref") << ":" << qosRef << ","
             << Quote("current_slice_snssai") << ":" << Quote(sliceSnssai) << ","
             << Quote("requested_bandwidth_dl") << ":" << targetBandwidthDlMbps << ","
             << Quote("requested_bandwidth_ul") << ":" << targetBandwidthUlMbps << ","
             << Quote("allocated_bandwidth_dl") << ":" << allocatedBandwidthDlMbps << ","
             << Quote("allocated_bandwidth_ul") << ":" << allocatedBandwidthUlMbps << ","
             << Quote("radio_capacity_dl_mbps") << ":" << gnbRadio.radioCapacityDlMbps << ","
             << Quote("radio_capacity_ul_mbps") << ":" << gnbRadio.radioCapacityUlMbps << ","
             << Quote("radio_capacity_unknown") << ":" << (gnbRadio.radioCapacityUnknown ? "true" : "false") << ","
             << Quote("radio_capacity_source") << ":" << Quote(gnbRadio.radioCapacitySource) << ","
             << Quote("capacity_limited_dl") << ":"
             << (allocatedBandwidthDlMbps + 1e-6 < targetBandwidthDlMbps ? "true" : "false") << ","
             << Quote("capacity_limited_ul") << ":"
             << (allocatedBandwidthUlMbps + 1e-6 < targetBandwidthUlMbps ? "true" : "false") << "}}";
    };

        struct FlowSnapshotAggregate
        {
            SnapshotContext::FlowRuntimeState* runtime = nullptr;
            const FlowProfile* profile = nullptr;
            uint32_t ueIndex = 0;
            uint32_t protocol = 17;
            std::string sourceIp;
            uint32_t sourcePort = 0;
            std::string destinationIp;
            uint32_t destinationPort = 0;
            double delaySumMs = 0.0;
            double jitterSumMs = 0.0;
            uint64_t rxPacketsForDelay = 0;
            uint64_t rxPacketsForJitter = 0;
            uint64_t txPackets = 0;
            uint64_t rxPackets = 0;
            double throughputUl = 0.0;
            double throughputDl = 0.0;
            bool sawDownlink = false;
            bool sawUplink = false;
        };

        std::map<uint16_t, FlowSnapshotAggregate> aggregates;
        for (const auto& [flowId, flow] : stats)
        {
            Ipv4FlowClassifier::FiveTuple tuple = context->classifier->FindFlow(flowId);
            const SnapshotContext::FlowRuntimeState* runtime = nullptr;
            bool downlink = false;
            auto ueByDestination = ipToUeIndex.find(ToString(tuple.destinationAddress));
            if (ueByDestination != ipToUeIndex.end())
            {
                auto profileIt = context->flowRuntimeByPort.find(tuple.destinationPort);
                if (profileIt != context->flowRuntimeByPort.end() &&
                    profileIt->second.ueIndex == ueByDestination->second)
                {
                    runtime = &profileIt->second;
                    downlink = true;
                }
            }
            if (runtime == nullptr)
            {
                auto ueBySource = ipToUeIndex.find(ToString(tuple.sourceAddress));
                if (ueBySource != ipToUeIndex.end())
                {
                    for (const auto& [port, candidate] : context->flowRuntimeByPort)
                    {
                        if (candidate.ueIndex == ueBySource->second &&
                            candidate.uplinkPort == tuple.destinationPort &&
                            candidate.uplinkSourcePort == tuple.sourcePort)
                        {
                            runtime = &candidate;
                            downlink = false;
                            break;
                        }
                    }
                }
            }
            if (runtime == nullptr)
            {
                std::cerr << "[split-ns3] unmatched-flow-stats"
                          << " tick=" << context->tickIndex
                          << " protocol=" << static_cast<uint32_t>(tuple.protocol)
                          << " src_ip=" << ToString(tuple.sourceAddress)
                          << " src_port=" << tuple.sourcePort
                          << " dst_ip=" << ToString(tuple.destinationAddress)
                          << " dst_port=" << tuple.destinationPort
                          << " tx_packets=" << flow.txPackets
                          << " rx_packets=" << flow.rxPackets
                          << " rx_bytes=" << flow.rxBytes
                          << std::endl;
                continue;
            }

            auto& aggregate = aggregates[runtime->port];
            if (aggregate.profile == nullptr)
            {
                aggregate.runtime = const_cast<SnapshotContext::FlowRuntimeState*>(runtime);
                aggregate.profile = &runtime->profile;
                aggregate.ueIndex = runtime->ueIndex;
                aggregate.protocol = static_cast<uint32_t>(tuple.protocol);
                aggregate.sourceIp = ToString(tuple.sourceAddress);
                aggregate.sourcePort = tuple.sourcePort;
                aggregate.destinationIp = ToString(tuple.destinationAddress);
                aggregate.destinationPort = tuple.destinationPort;
            }
            aggregate.delaySumMs += 1000.0 * flow.delaySum.GetSeconds();
            aggregate.jitterSumMs += 1000.0 * flow.jitterSum.GetSeconds();
            aggregate.rxPacketsForDelay += flow.rxPackets;
            aggregate.rxPacketsForJitter += flow.rxPackets;
            aggregate.txPackets += flow.txPackets;
            aggregate.rxPackets += flow.rxPackets;
            if (downlink)
            {
                aggregate.sawDownlink = true;
                aggregate.throughputDl += flow.rxBytes * 8.0 / elapsedSeconds / 1e6;
                aggregate.sourceIp = ToString(tuple.sourceAddress);
                aggregate.sourcePort = tuple.sourcePort;
                aggregate.destinationIp = ToString(tuple.destinationAddress);
                aggregate.destinationPort = tuple.destinationPort;
            }
            else
            {
                aggregate.sawUplink = true;
                aggregate.throughputUl += flow.rxBytes * 8.0 / elapsedSeconds / 1e6;
            }
        }

        for (const auto& [port, runtime] : context->flowRuntimeByPort)
        {
            auto aggregateIt = aggregates.find(port);
            if (aggregateIt == aggregates.end())
            {
                if (runtime.profile.enabled)
                {
                    std::cerr << "[split-ns3] missing-flow-stats"
                              << " tick=" << context->tickIndex
                              << " flow_id=" << runtime.profile.flowId
                              << " ue_index=" << runtime.ueIndex
                              << " dl_port=" << runtime.port
                              << " ul_port=" << runtime.uplinkPort
                              << " ul_source_port=" << runtime.uplinkSourcePort
                              << " allocated_dl_mbps=" << runtime.profile.allocatedBandwidthDlMbps
                              << " allocated_ul_mbps=" << runtime.profile.allocatedBandwidthUlMbps
                              << std::endl;
                    appendFlow(const_cast<SnapshotContext::FlowRuntimeState*>(&runtime),
                               &runtime.profile,
                               runtime.ueIndex,
                               17,
                               ToString(context->remoteHostAddress),
                               runtime.downlinkSourcePort,
                               ToString(context->ueIps[runtime.ueIndex]),
                               runtime.port,
                               0.0,
                               0.0,
                               0.0,
                               0.0,
                               0.0,
                               0,
                               0,
                               "bidirectional",
                               "ns3_remote_host",
                               "ue_pdu_ip",
                               runtime.profile.flowId);
                }
                continue;
            }
            const auto& aggregate = aggregateIt->second;
            const double delayMs = aggregate.rxPacketsForDelay > 0
                                       ? aggregate.delaySumMs / static_cast<double>(aggregate.rxPacketsForDelay)
                                       : 0.0;
            const double jitterMs = aggregate.rxPacketsForJitter > 0
                                        ? aggregate.jitterSumMs / static_cast<double>(aggregate.rxPacketsForJitter)
                                        : 0.0;
            const double lossRate = aggregate.txPackets > 0
                                        ? static_cast<double>(aggregate.txPackets - aggregate.rxPackets) /
                                              static_cast<double>(aggregate.txPackets)
                                        : 0.0;
            appendFlow(aggregate.runtime,
                       aggregate.profile,
                       aggregate.ueIndex,
                       aggregate.protocol,
                       aggregate.sourceIp,
                       aggregate.sourcePort,
                       aggregate.destinationIp,
                       aggregate.destinationPort,
                       delayMs,
                       jitterMs,
                       lossRate,
                       aggregate.throughputUl,
                       aggregate.throughputDl,
                       aggregate.txPackets,
                       aggregate.rxPackets,
                       "bidirectional",
                       "ns3_remote_host",
                       "ue_pdu_ip",
                       aggregate.profile != nullptr ? aggregate.profile->flowId : ("flow-" + std::to_string(port)));
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
             << Quote("label") << ":" << Quote(sliceId);
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
    std::string gnbUpfLinksArg;
    std::string gnbPositionsArg;
    std::string uePositionsArg;
    uint16_t numerology = 1;
    double centralFrequency = 3.5e9;
    double bandwidth = 100e6;
    double totalTxPower = 43.0;
    std::string schedulerType = "pf";
    std::string tddPattern = "DL|UL|UL|F|DL|UL|UL|F|";
    double ueTxPowerDb = 23.0;
    double gnbNoiseFigureDb = 5.0;
    double ueNoiseFigureDb = 7.0;
    bool enableUplinkPowerControl = true;

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
    cmd.AddValue("gnbUpfLinks", "Semicolon separated 1-based gNB:UPF N3 links", gnbUpfLinksArg);
    cmd.AddValue("gnbPositions", "Semicolon separated x:y:z gNB positions or auto", gnbPositionsArg);
    cmd.AddValue("uePositions", "Semicolon separated x:y:z UE positions or auto", uePositionsArg);
    cmd.AddValue("nrNumerology", "NR numerology used by split-mode radio", numerology);
    cmd.AddValue("nrBandwidthHz", "NR channel bandwidth in Hz used by split-mode radio", bandwidth);
    cmd.AddValue("nrCentralFrequencyHz", "NR central frequency in Hz used by split-mode radio", centralFrequency);
    cmd.AddValue("nrTxPowerDb", "NR gNB transmit power in dBm used by split-mode radio", totalTxPower);
    cmd.AddValue("schedulerType", "Split-mode NR scheduler type", schedulerType);
    cmd.AddValue("tddPattern", "Split-mode NR TDD pattern", tddPattern);
    cmd.AddValue("ueTxPowerDb", "Split-mode NR UE transmit power in dBm", ueTxPowerDb);
    cmd.AddValue("gnbNoiseFigureDb", "Split-mode NR gNB noise figure in dB", gnbNoiseFigureDb);
    cmd.AddValue("ueNoiseFigureDb", "Split-mode NR UE noise figure in dB", ueNoiseFigureDb);
    cmd.AddValue("enableUplinkPowerControl",
                 "Enable split-mode NR uplink power control",
                 enableUplinkPowerControl);
    cmd.Parse(argc, argv);

    schedulerType = NormalizeSchedulerType(schedulerType);
    tddPattern = NormalizeTddPattern(tddPattern);

    std::cerr << "[split-ns3] start"
              << " run_id=" << runId
              << " scenario_id=" << scenarioId
              << " tick_ms=" << tickMs
              << " sim_time_ms=" << simTimeMs
              << " nr_numerology=" << numerology
              << " nr_bandwidth_hz=" << bandwidth
              << " nr_central_frequency_hz=" << centralFrequency
              << " nr_tx_power_db=" << totalTxPower
              << " scheduler_type=" << schedulerType
              << " tdd_pattern=" << tddPattern
              << " ue_tx_power_db=" << ueTxPowerDb
              << " gnb_noise_figure_db=" << gnbNoiseFigureDb
              << " ue_noise_figure_db=" << ueNoiseFigureDb
              << " enable_uplink_power_control=" << (enableUplinkPowerControl ? "true" : "false")
              << " flow_profile_file=" << flowProfileFile
              << " output_file=" << outputFile
              << " clock_file=" << clockFile
              << std::endl;

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

    const auto n3Links = ParseN3Links(gnbUpfLinksArg, gNbNum, upfNames.size());


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
    nrHelper->SetSchedulerTypeId(TypeId::LookupByName(schedulerType));
    nrHelper->SetGnbPhyAttribute("TxPower", DoubleValue(totalTxPower));
    nrHelper->SetGnbPhyAttribute("NoiseFigure", DoubleValue(gnbNoiseFigureDb));
    nrHelper->SetUePhyAttribute("TxPower", DoubleValue(ueTxPowerDb));
    nrHelper->SetUePhyAttribute("NoiseFigure", DoubleValue(ueNoiseFigureDb));
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
    for (uint32_t index = 0; index < ueNetDev.GetN(); ++index)
    {
        NrHelper::GetUePhy(ueNetDev.Get(index), 0)->SetAttribute("TxPower", DoubleValue(ueTxPowerDb));
        context.ueIndexByRnti[NrHelper::GetUePhy(ueNetDev.Get(index), 0)->GetRnti()] = index;
        context.ueIndexByImsi[DynamicCast<NrUeNetDevice>(ueNetDev.Get(index))->GetImsi()] = index;
    }
    for (uint32_t index = 0; index < gnbNetDev.GetN(); ++index)
    {
        context.gnbIndexByCellId[NrHelper::GetGnbPhy(gnbNetDev.Get(index), 0)->GetCellId()] = index;
    }

    auto [remoteHost, remoteHostGatewayAddress] = nrEpcHelper->SetupRemoteHost("100Gb/s", 2500, Seconds(0.000));
    Ptr<Node> pgwNode = nrEpcHelper->GetPgwNode();
    Ptr<Ipv4> pgwIpv4 = pgwNode->GetObject<Ipv4>();
    if (pgwIpv4 != nullptr)
    {
        for (uint32_t ifIndex = 0; ifIndex < pgwIpv4->GetNInterfaces(); ++ifIndex)
        {
            for (uint32_t addrIndex = 0; addrIndex < pgwIpv4->GetNAddresses(ifIndex); ++addrIndex)
            {
                const auto ifAddr = pgwIpv4->GetAddress(ifIndex, addrIndex);
                context.pgwLocalAddresses.push_back(ifAddr.GetLocal());
            }
        }
    }
    context.remoteHostAddresses = CollectNonLoopbackAddresses(remoteHost);
    context.remoteHostGatewayAddress = remoteHostGatewayAddress;
    const Ipv4Address remoteHostAddress =
        ResolveRemoteHostDataAddress(remoteHost, context.pgwLocalAddresses);
    std::cerr << "[split-ns3] address-plan"
              << " remote_host=" << ToString(remoteHostAddress)
              << " remote_host_addrs=" << JoinIpv4Addresses(context.remoteHostAddresses)
              << " remote_host_gateway=" << ToString(remoteHostGatewayAddress)
              << " pgw_addrs=" << JoinIpv4Addresses(context.pgwLocalAddresses) << std::endl;
    pgwNode->GetObject<Ipv4>()->TraceConnectWithoutContext(
        "Rx",
        MakeBoundCallback(&OnIpv4Trace, &context, Ipv4TraceRole::PgwRx));
    pgwNode->GetObject<Ipv4>()->TraceConnectWithoutContext(
        "Tx",
        MakeBoundCallback(&OnIpv4Trace, &context, Ipv4TraceRole::PgwTx));
    pgwNode->GetObject<Ipv4>()->TraceConnectWithoutContext(
        "LocalDeliver",
        MakeBoundCallback(&OnIpv4DecisionTrace, &context, Ipv4TraceRole::PgwLocalDeliver));
    pgwNode->GetObject<Ipv4>()->TraceConnectWithoutContext(
        "UnicastForward",
        MakeBoundCallback(&OnIpv4DecisionTrace, &context, Ipv4TraceRole::PgwForward));
    pgwNode->GetObject<Ipv4>()->TraceConnectWithoutContext(
        "Drop",
        MakeBoundCallback(&OnIpv4DropTrace, &context));
    remoteHost->GetObject<Ipv4>()->TraceConnectWithoutContext(
        "Tx",
        MakeBoundCallback(&OnIpv4Trace, &context, Ipv4TraceRole::RemoteTx));
    remoteHost->GetObject<Ipv4>()->TraceConnectWithoutContext(
        "Rx",
        MakeBoundCallback(&OnIpv4Trace, &context, Ipv4TraceRole::RemoteRx));

    InternetStackHelper internet;
    internet.Install(gridScenario.GetUserTerminals());
    Ipv4InterfaceContainer ueIpIfaces = nrEpcHelper->AssignUeIpv4Address(NetDeviceContainer(ueNetDev));
    Ipv4StaticRoutingHelper ipv4RoutingHelper;
    for (uint32_t index = 0; index < resolvedUeNum; ++index)
    {
        Ptr<Ipv4> ueIpv4 = gridScenario.GetUserTerminals().Get(index)->GetObject<Ipv4>();
        Ptr<Ipv4StaticRouting> ueStaticRouting = ipv4RoutingHelper.GetStaticRouting(
            ueIpv4);
        ueStaticRouting->SetDefaultRoute(nrEpcHelper->GetUeDefaultGatewayAddress(), 1);
        context.ueDefaultGateway = nrEpcHelper->GetUeDefaultGatewayAddress();
        std::cerr << "[split-ns3] ue-address"
                  << " ue_index=" << index
                  << " ue_ip=" << ToString(ueIpIfaces.GetAddress(index))
                  << " ue_default_gw=" << ToString(nrEpcHelper->GetUeDefaultGatewayAddress())
                  << std::endl;
        ueIpv4->TraceConnectWithoutContext(
            "Tx",
            MakeBoundCallback(&OnIpv4Trace, &context, Ipv4TraceRole::UeTx));
        ueIpv4->TraceConnectWithoutContext(
            "Rx",
            MakeBoundCallback(&OnIpv4Trace, &context, Ipv4TraceRole::UeRx));
    }
    for (uint32_t index = 0; index < resolvedUeNum; ++index)
    {
        nrHelper->AttachToGnb(ueNetDev.Get(index), gnbNetDev.Get(ueToGnb[index] - 1));
    }

    ApplicationContainer serverApps;
    ApplicationContainer clientApps;
    auto& flowRuntimeByPort = context.flowRuntimeByPort;

    if (flowProfiles.empty())
    {
        NS_FATAL_ERROR("split-mode requires a non-empty flow profile file");
    }
    else
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
            const uint16_t ulPort = static_cast<uint16_t>(6000 + index);
            const uint16_t dlSourcePort = static_cast<uint16_t>(15000 + index);
            const uint16_t ulSourcePort = static_cast<uint16_t>(25000 + index);
            Ptr<PacketSink> downlinkSink;
            Ptr<PacketSink> uplinkSink;
            PacketSinkHelper packetSinkHelper(
                "ns3::UdpSocketFactory",
                InetSocketAddress(Ipv4Address::GetAny(), dlPort));
            ApplicationContainer installedDownlinkSink =
                packetSinkHelper.Install(gridScenario.GetUserTerminals().Get(ueIndex));
            serverApps.Add(installedDownlinkSink);
            downlinkSink = DynamicCast<PacketSink>(installedDownlinkSink.Get(0));
            PacketSinkHelper remoteSinkHelper(
                "ns3::UdpSocketFactory",
                InetSocketAddress(Ipv4Address::GetAny(), ulPort));
            ApplicationContainer installedUplinkSink = remoteSinkHelper.Install(remoteHost);
            serverApps.Add(installedUplinkSink);
            uplinkSink = DynamicCast<PacketSink>(installedUplinkSink.Get(0));
            flowRuntimeByPort[dlPort] = SnapshotContext::FlowRuntimeState{
                profile,
                downlinkSink,
                uplinkSink,
                dlPort,
                ulPort,
                dlSourcePort,
                ulSourcePort,
                ueIndex,
            };
            Ptr<SplitFlowUdpApp> downlinkClient = CreateObject<SplitFlowUdpApp>();
            downlinkClient->Configure(
                addressUtils::ConvertToSocketAddress(ueIpIfaces.GetAddress(ueIndex), dlPort),
                &flowRuntimeByPort[dlPort],
                MilliSeconds(tickMs),
                dlSourcePort,
                true);
            remoteHost->AddApplication(downlinkClient);
            clientApps.Add(downlinkClient);

            Ptr<SplitFlowUdpApp> uplinkClient = CreateObject<SplitFlowUdpApp>();
            uplinkClient->Configure(
                addressUtils::ConvertToSocketAddress(remoteHostAddress, ulPort),
                &flowRuntimeByPort[dlPort],
                MilliSeconds(tickMs),
                ulSourcePort,
                false);
            gridScenario.GetUserTerminals().Get(ueIndex)->AddApplication(uplinkClient);
            clientApps.Add(uplinkClient);
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
    context.radioConfig.schedulerType = schedulerType;
    context.radioConfig.tddPattern = tddPattern;
    context.radioConfig.gnbTxPowerDbm = totalTxPower;
    context.radioConfig.ueTxPowerDbm = ueTxPowerDb;
    context.radioConfig.gnbNoiseFigureDb = gnbNoiseFigureDb;
    context.radioConfig.ueNoiseFigureDb = ueNoiseFigureDb;
    context.radioConfig.enableUplinkPowerControl = enableUplinkPowerControl;
    context.radioConfig.numerology = numerology;
    context.radioConfig.centralFrequencyHz = centralFrequency;
    context.radioConfig.bandwidthHz = bandwidth;
    context.upfNames = upfNames;
    context.sliceSds = sliceSds;
    context.sliceIds = sliceIds;
    context.sliceResources = sliceResources;
    context.ueSliceIds = ueSliceIds;
    context.remoteHostAddress = remoteHostAddress;
    context.n3Links = n3Links;
    context.monitor = monitor;
    context.classifier = classifier;
    context.appStartTime = appStartTime;
    context.simTime = simTime;
    for (uint32_t index = 0; index < resolvedUeNum; ++index)
    {
        context.ueIps.push_back(ueIpIfaces.GetAddress(index));
        context.ueToGnb.push_back(ueToGnb[index] - 1);
        context.ueSupis.push_back(ueSupis[index]);
        context.uePorts.push_back(static_cast<uint16_t>(5000 + index));
        context.uePositions.push_back(gridScenario.GetUserTerminals().Get(index)->GetObject<MobilityModel>()->GetPosition());
    }
    for (uint32_t index = 0; index < gNbNum; ++index)
    {
        context.gnbPositions.push_back(gridScenario.GetBaseStations().Get(index)->GetObject<MobilityModel>()->GetPosition());
    }

    ApplySlaDrivenAllocations(&context);

    Simulator::Schedule(MilliSeconds(tickMs), &EmitSnapshot, &context);
    Simulator::Stop(simTime);
    Simulator::Run();
    Simulator::Destroy();
    return 0;
}
