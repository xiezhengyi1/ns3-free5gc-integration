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

std::vector<std::string>
SplitCsv(const std::string& input)
{
    std::vector<std::string> parts;
    std::stringstream stream(input);
    std::string part;
    while (std::getline(stream, part, ','))
    {
        if (!part.empty())
        {
            parts.push_back(part);
        }
    }
    if (parts.empty())
    {
        parts.push_back(input);
    }
    return parts;
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
    std::vector<Ipv4Address> ueIps;
    std::vector<uint32_t> ueToGnb;
    std::vector<std::string> ueSupis;
    std::vector<uint16_t> uePorts;
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
        ipToUeIndex[context->ueIps[index].ToString()] = index;
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
             << Quote("core-node-" + std::to_string((context->ueToGnb[ue] % context->upfNames.size()) + 1))
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
             << Quote(context->upfNames[gnb % context->upfNames.size()]) << "}";
    }
    json << "],";

    json << Quote("ues") << ":[";
    for (uint32_t ue = 0; ue < context->ueNum; ++ue)
    {
        if (ue > 0)
        {
            json << ",";
        }
        const auto sliceIndex = ue % context->sliceSds.size();
        json << "{" << Quote("ue_id") << ":" << Quote("ue-" + std::to_string(ue + 1)) << ","
             << Quote("supi") << ":" << Quote(context->ueSupis[ue]) << ","
             << Quote("gnb_id") << ":" << Quote("gnb-" + std::to_string(context->ueToGnb[ue] + 1)) << ","
             << Quote("slice_id") << ":" << Quote("slice-1-" + context->sliceSds[sliceIndex]) << ","
             << Quote("ip_address") << ":" << Quote(context->ueIps[ue].ToString()) << "}";
    }
    json << "],";

    json << Quote("flows") << ":[";
    bool firstFlow = true;
    for (const auto& [flowId, flow] : stats)
    {
        Ipv4FlowClassifier::FiveTuple tuple = context->classifier->FindFlow(flowId);
        auto ueIt = ipToUeIndex.find(tuple.destinationAddress.ToString());
        if (ueIt == ipToUeIndex.end())
        {
            continue;
        }
        const uint32_t ueIndex = ueIt->second;
        const uint32_t gnbIndex = context->ueToGnb[ueIndex];
        const uint32_t upfIndex = gnbIndex % context->upfNames.size();
        const uint32_t sliceIndex = ueIndex % context->sliceSds.size();
        const double delayMs = flow.rxPackets > 0 ? 1000.0 * flow.delaySum.GetSeconds() / flow.rxPackets : 0.0;
        const double jitterMs = flow.rxPackets > 0 ? 1000.0 * flow.jitterSum.GetSeconds() / flow.rxPackets : 0.0;
        const double lossRate = flow.txPackets > 0
                                    ? static_cast<double>(flow.txPackets - flow.rxPackets) /
                                          static_cast<double>(flow.txPackets)
                                    : 0.0;
        const double throughputDl = flow.rxBytes * 8.0 / elapsedSeconds / 1e6;

        totalThroughputDl += throughputDl;
        totalDelayMs += delayMs;
        totalLossRate += lossRate;
        activeFlows++;

        if (!firstFlow)
        {
            json << ",";
        }
        firstFlow = false;
        json << "{" << Quote("flow_id") << ":" << Quote("flow-" + std::to_string(flowId)) << ","
             << Quote("supi") << ":" << Quote(context->ueSupis[ueIndex]) << ","
             << Quote("app_id") << ":" << Quote("dl-app-" + std::to_string(ueIndex + 1)) << ","
             << Quote("src_gnb") << ":" << Quote("gnb-" + std::to_string(gnbIndex + 1)) << ","
             << Quote("dst_upf") << ":" << Quote(context->upfNames[upfIndex]) << ","
             << Quote("slice_id") << ":" << Quote("slice-1-" + context->sliceSds[sliceIndex]) << ","
             << Quote("5qi") << ":9,"
             << Quote("delay_ms") << ":" << delayMs << ","
             << Quote("jitter_ms") << ":" << jitterMs << ","
             << Quote("loss_rate") << ":" << lossRate << ","
             << Quote("throughput_ul_mbps") << ":0,"
             << Quote("throughput_dl_mbps") << ":" << throughputDl << ","
             << Quote("queue_bytes") << ":0,"
             << Quote("rlc_buffer_bytes") << ":0}"
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
    uint16_t ueNumPerGnb = 1;
    uint32_t tickMs = 1000;
    uint32_t simTimeMs = 30000;
    std::string runId = "run-local";
    std::string scenarioId = "scenario-local";
    std::string outputFile = "./tick-snapshots.jsonl";
    std::string upfNamesCsv = "upf";
    std::string sliceSdsCsv = "010203";

    CommandLine cmd(__FILE__);
    cmd.AddValue("gNbNum", "Number of gNBs", gNbNum);
    cmd.AddValue("ueNumPerGnb", "Number of UEs per gNB", ueNumPerGnb);
    cmd.AddValue("tickMs", "Tick interval in milliseconds", tickMs);
    cmd.AddValue("simTimeMs", "Simulation time in milliseconds", simTimeMs);
    cmd.AddValue("runId", "Run identifier", runId);
    cmd.AddValue("scenarioId", "Scenario identifier", scenarioId);
    cmd.AddValue("outputFile", "Snapshot JSONL output path", outputFile);
    cmd.AddValue("upfNames", "Comma separated UPF names", upfNamesCsv);
    cmd.AddValue("sliceSds", "Comma separated slice SD list", sliceSdsCsv);
    cmd.Parse(argc, argv);

    std::filesystem::create_directories(std::filesystem::path(outputFile).parent_path());
    std::ofstream(outputFile, std::ios::trunc).close();

    const uint32_t ueNum = gNbNum * ueNumPerGnb;
    const auto upfNames = SplitCsv(upfNamesCsv);
    const auto sliceSds = SplitCsv(sliceSdsCsv);

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
    gridScenario.SetUtNumber(ueNum);
    gridScenario.SetScenarioHeight(3);
    gridScenario.SetScenarioLength(3);
    gridScenario.CreateScenario();

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
    nrHelper->AttachToClosestGnb(ueNetDev, gnbNetDev);

    ApplicationContainer serverApps;
    ApplicationContainer clientApps;

    for (uint32_t index = 0; index < ueNum; ++index)
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
    context.ueNum = ueNum;
    context.upfNames = upfNames;
    context.sliceSds = sliceSds;
    context.monitor = monitor;
    context.classifier = classifier;
    context.appStartTime = appStartTime;
    context.simTime = simTime;

    for (uint32_t index = 0; index < ueNum; ++index)
    {
        context.ueIps.push_back(ueIpIfaces.GetAddress(index));
        context.ueToGnb.push_back(index % gNbNum);
        context.ueSupis.push_back(BuildSupi(index + 1));
        context.uePorts.push_back(static_cast<uint16_t>(5000 + index));
    }

    Simulator::Schedule(MilliSeconds(tickMs), &EmitSnapshot, &context);
    Simulator::Stop(simTime);
    Simulator::Run();
    Simulator::Destroy();
    return 0;
}