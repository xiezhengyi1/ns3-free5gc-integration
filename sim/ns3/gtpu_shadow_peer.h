// SPDX-License-Identifier: GPL-2.0-only
#ifndef GTPU_SHADOW_PEER_H
#define GTPU_SHADOW_PEER_H

#include "ns3/callback.h"
#include "ns3/core-module.h"
#include "ns3/object.h"

#include <arpa/inet.h>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>
#include <thread>

namespace ns3
{

struct ShadowFlowAuthorization
{
    std::string flowId;
    uint32_t payloadSize = 0;
};

struct ShadowPacketRequest
{
    uint64_t packetId = 0;
    uint64_t epochId = 0;
    std::string flowId;
    std::string direction;
    uint32_t sizeBytes = 0;
    uint32_t qfi = 0;
    uint64_t enqueueNs3Us = 0;
    uint64_t virtualExpiryUs = 0;
};

class GtpuShadowPeer : public Object
{
  public:
    enum MessageType : uint8_t
    {
        HELLO = 1,
        PACKET_ENQUEUE = 2,
        PACKET_DELIVER = 3,
        PACKET_DROP = 4,
        TICK_COMPLETE = 5,
        AUTHORIZE_SEND = 6,
        EPOCH_START = 7,
    };

    using InjectCallback = Callback<void, ShadowPacketRequest>;

    static TypeId GetTypeId()
    {
        static TypeId tid =
            TypeId("ns3::GtpuShadowPeer").SetParent<Object>().AddConstructor<GtpuShadowPeer>();
        return tid;
    }

    ~GtpuShadowPeer() override
    {
        if (m_fd >= 0)
        {
            close(m_fd);
        }
    }

    void Configure(const std::string& socketPath,
                   uint64_t virtualEpochUs,
                   const std::vector<ShadowFlowAuthorization>& flows,
                   InjectCallback inject,
                   uint64_t maxEpochs = 0)
    {
        m_socketPath = socketPath;
        m_virtualEpochUs = virtualEpochUs;
        m_flows = flows;
        m_inject = inject;
        m_maxEpochs = maxEpochs;
    }

    void Start()
    {
        Connect();
        Simulator::ScheduleNow(&GtpuShadowPeer::StartEpoch, this);
    }

    void Deliver(uint64_t packetId)
    {
        auto it = m_pending.find(packetId);
        if (it == m_pending.end())
        {
            return;
        }
        Send(PACKET_DELIVER,
             JsonObject({{"packet_id", Number(packetId)},
                         {"epoch_id", Number(it->second.epochId)},
                         {"ns3_time_us", Number(Simulator::Now().GetMicroSeconds())}}));
        m_pending.erase(it);
    }

    void Drop(uint64_t packetId, const std::string& reason)
    {
        auto it = m_pending.find(packetId);
        if (it == m_pending.end())
        {
            return;
        }
        Send(PACKET_DROP,
             JsonObject({{"packet_id", Number(packetId)},
                         {"epoch_id", Number(it->second.epochId)},
                         {"ns3_time_us", Number(Simulator::Now().GetMicroSeconds())},
                         {"reason", Quote(reason)}}));
        m_pending.erase(it);
    }

  private:
    struct PendingPacket
    {
        uint64_t epochId;
    };

    static uint64_t HostToNetwork64(uint64_t value)
    {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return (static_cast<uint64_t>(htonl(static_cast<uint32_t>(value))) << 32) |
               htonl(static_cast<uint32_t>(value >> 32));
#else
        return value;
#endif
    }

    static uint64_t NetworkToHost64(uint64_t value)
    {
        return HostToNetwork64(value);
    }

    static std::string Quote(const std::string& value)
    {
        std::ostringstream output;
        output << '"';
        for (char character : value)
        {
            if (character == '"' || character == '\\')
            {
                output << '\\';
            }
            output << character;
        }
        output << '"';
        return output.str();
    }

    static std::string Number(uint64_t value)
    {
        return std::to_string(value);
    }

    static std::string JsonObject(
        const std::vector<std::pair<std::string, std::string>>& fields)
    {
        std::ostringstream output;
        output << "{";
        bool first = true;
        for (const auto& [name, value] : fields)
        {
            if (!first)
            {
                output << ",";
            }
            first = false;
            output << Quote(name) << ":" << value;
        }
        output << "}";
        return output.str();
    }

    static uint64_t FindUnsigned(const std::string& json, const std::string& key)
    {
        const std::string marker = Quote(key) + ":";
        const auto start = json.find(marker);
        if (start == std::string::npos)
        {
            throw std::runtime_error("missing JSON integer field " + key);
        }
        size_t cursor = start + marker.size();
        size_t end = cursor;
        while (end < json.size() && json[end] >= '0' && json[end] <= '9')
        {
            ++end;
        }
        if (end == cursor)
        {
            throw std::runtime_error("invalid JSON integer field " + key);
        }
        return std::stoull(json.substr(cursor, end - cursor));
    }

    static std::string FindString(const std::string& json, const std::string& key)
    {
        const std::string marker = Quote(key) + ":\"";
        const auto start = json.find(marker);
        if (start == std::string::npos)
        {
            throw std::runtime_error("missing JSON string field " + key);
        }
        const size_t valueStart = start + marker.size();
        const auto end = json.find('"', valueStart);
        if (end == std::string::npos)
        {
            throw std::runtime_error("unterminated JSON string field " + key);
        }
        return json.substr(valueStart, end - valueStart);
    }

    static uint64_t FindOptionalUnsigned(const std::string& json,
                                         const std::string& key,
                                         uint64_t fallback)
    {
        const std::string marker = Quote(key) + ":";
        const auto start = json.find(marker);
        if (start == std::string::npos)
        {
            return fallback;
        }
        const size_t cursor = start + marker.size();
        if (json.compare(cursor, 4, "null") == 0)
        {
            return fallback;
        }
        return FindUnsigned(json, key);
    }

    void Connect()
    {
        m_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (m_fd < 0)
        {
            throw std::runtime_error("failed to create gate socket");
        }
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        if (m_socketPath.size() >= sizeof(address.sun_path))
        {
            throw std::runtime_error("gate socket path is too long");
        }
        std::strncpy(address.sun_path, m_socketPath.c_str(), sizeof(address.sun_path) - 1);
        if (connect(m_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
        {
            const int error = errno;
            close(m_fd);
            m_fd = -1;
            throw std::runtime_error(
                "user-plane gate is not ready for non-blocking connect: " +
                std::string(std::strerror(error)));
        }
        Send(HELLO, JsonObject({{"role", Quote("ns3-shadow-peer")}}));
    }

    void StartEpoch()
    {
        if (!m_pending.empty())
        {
            Simulator::Schedule(MicroSeconds(1), &GtpuShadowPeer::StartEpoch, this);
            return;
        }
        if (m_epochId > 0)
        {
            Send(TICK_COMPLETE,
                 JsonObject({{"epoch_id", Number(m_epochId)},
                             {"ns3_time_us", Number(Simulator::Now().GetMicroSeconds())}}));
            if (m_maxEpochs > 0 && m_epochId >= m_maxEpochs)
            {
                return;
            }
        }
        ++m_epochId;
        m_receivedThisEpoch = 0;
        m_expectedThisEpoch = m_flows.size() * 2;
        m_epochWaitStarted = std::chrono::steady_clock::now();
        Send(EPOCH_START,
             JsonObject({{"epoch_id", Number(m_epochId)},
                         {"ns3_time_us", Number(Simulator::Now().GetMicroSeconds())}}));
        for (const auto& flow : m_flows)
        {
            for (const std::string direction : {"uplink", "downlink"})
            {
                Send(AUTHORIZE_SEND,
                     JsonObject({{"authorization_id", Number(++m_authorizationId)},
                                 {"epoch_id", Number(m_epochId)},
                                 {"application_sequence", Number(m_authorizationId)},
                                 {"flow_id", Quote(flow.flowId)},
                                 {"direction", Quote(direction)},
                                 {"payload_size", Number(flow.payloadSize)}}));
            }
        }
        Simulator::ScheduleNow(&GtpuShadowPeer::PumpUntilEpochReady, this);
    }

    void PumpUntilEpochReady()
    {
        ReceiveAvailable();
        if (m_receivedThisEpoch < m_expectedThisEpoch)
        {
            if (std::chrono::steady_clock::now() - m_epochWaitStarted >
                std::chrono::seconds(30))
            {
                NS_FATAL_ERROR("timed out waiting for controlled packets in epoch "
                               << m_epochId << ": received=" << m_receivedThisEpoch
                               << " expected=" << m_expectedThisEpoch);
            }
            std::this_thread::yield();
            Simulator::ScheduleNow(&GtpuShadowPeer::PumpUntilEpochReady, this);
            return;
        }
        Simulator::Schedule(MicroSeconds(m_virtualEpochUs),
                            &GtpuShadowPeer::StartEpoch,
                            this);
    }

    void ReceiveAvailable()
    {
        uint8_t buffer[65536];
        while (true)
        {
            const ssize_t received = recv(m_fd, buffer, sizeof(buffer), MSG_DONTWAIT);
            if (received < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    break;
                }
                throw std::runtime_error("failed reading gate socket");
            }
            if (received == 0)
            {
                throw std::runtime_error("user-plane gate disconnected");
            }
            m_receiveBuffer.insert(m_receiveBuffer.end(), buffer, buffer + received);
        }
        DecodeFrames();
    }

    void DecodeFrames()
    {
        constexpr size_t headerSize = 20;
        while (m_receiveBuffer.size() >= headerSize)
        {
            if (std::memcmp(m_receiveBuffer.data(), "N6AI", 4) != 0 ||
                m_receiveBuffer[4] != 1)
            {
                throw std::runtime_error("invalid gate protocol header");
            }
            const uint8_t type = m_receiveBuffer[5];
            uint32_t networkLength;
            std::memcpy(&networkLength, m_receiveBuffer.data() + 8, sizeof(networkLength));
            const uint32_t payloadLength = ntohl(networkLength);
            if (payloadLength > 1024 * 1024)
            {
                throw std::runtime_error("gate protocol payload exceeds 1 MiB limit");
            }
            const size_t frameLength = headerSize + payloadLength;
            if (m_receiveBuffer.size() < frameLength)
            {
                return;
            }
            const std::string payload(
                reinterpret_cast<const char*>(m_receiveBuffer.data() + headerSize),
                payloadLength);
            m_receiveBuffer.erase(m_receiveBuffer.begin(),
                                  m_receiveBuffer.begin() + frameLength);
            if (type == PACKET_ENQUEUE)
            {
                HandleEnqueue(payload);
            }
        }
    }

    void HandleEnqueue(const std::string& payload)
    {
        ShadowPacketRequest request;
        request.packetId = FindUnsigned(payload, "packet_id");
        request.epochId = FindUnsigned(payload, "epoch_id");
        request.flowId = FindString(payload, "flow_id");
        request.direction = FindString(payload, "direction");
        request.sizeBytes = static_cast<uint32_t>(FindUnsigned(payload, "size_bytes"));
        request.qfi = static_cast<uint32_t>(FindOptionalUnsigned(payload, "qfi", 0));
        request.enqueueNs3Us = FindUnsigned(payload, "enqueue_ns3_us");
        request.virtualExpiryUs = FindUnsigned(payload, "virtual_expiry_us");
        if (request.epochId != m_epochId)
        {
            throw std::runtime_error("received packet for the wrong virtual epoch");
        }
        if (request.packetId == 0 || !m_seenPacketIds.insert(request.packetId).second)
        {
            throw std::runtime_error("duplicate packet_id in shadow peer");
        }
        if (request.direction != "uplink" && request.direction != "downlink")
        {
            throw std::runtime_error("invalid shadow packet direction");
        }
        if (request.sizeBytes == 0 || request.virtualExpiryUs == 0)
        {
            throw std::runtime_error("shadow packet size and expiry must be positive");
        }
        if (request.qfi > 63)
        {
            throw std::runtime_error("shadow packet QFI is outside [0, 63]");
        }
        if (request.enqueueNs3Us !=
            static_cast<uint64_t>(Simulator::Now().GetMicroSeconds()))
        {
            throw std::runtime_error("shadow packet enqueue time does not match ns-3 virtual time");
        }
        bool knownFlow = false;
        for (const auto& flow : m_flows)
        {
            if (flow.flowId == request.flowId)
            {
                knownFlow = true;
                break;
            }
        }
        if (!knownFlow)
        {
            throw std::runtime_error("unknown shadow flow " + request.flowId);
        }
        if (m_receivedThisEpoch >= m_expectedThisEpoch)
        {
            throw std::runtime_error("too many packets received for virtual epoch");
        }
        m_pending.emplace(request.packetId, PendingPacket{request.epochId});
        ++m_receivedThisEpoch;
        m_inject(request);
        Simulator::Schedule(MicroSeconds(request.virtualExpiryUs),
                            &GtpuShadowPeer::Expire,
                            this,
                            request.packetId);
    }

    void Expire(uint64_t packetId)
    {
        Drop(packetId, "virtual-expiry");
    }

    void Send(uint8_t type, const std::string& payload)
    {
        std::vector<uint8_t> frame(20 + payload.size(), 0);
        std::memcpy(frame.data(), "N6AI", 4);
        frame[4] = 1;
        frame[5] = type;
        const uint32_t networkLength = htonl(static_cast<uint32_t>(payload.size()));
        const uint64_t networkSequence = HostToNetwork64(++m_sequence);
        std::memcpy(frame.data() + 8, &networkLength, sizeof(networkLength));
        std::memcpy(frame.data() + 12, &networkSequence, sizeof(networkSequence));
        std::memcpy(frame.data() + 20, payload.data(), payload.size());
        size_t sent = 0;
        while (sent < frame.size())
        {
            const ssize_t result =
                send(m_fd, frame.data() + sent, frame.size() - sent, MSG_NOSIGNAL);
            if (result <= 0)
            {
                if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                {
                    throw std::runtime_error("gate socket send buffer is full");
                }
                throw std::runtime_error("failed writing gate socket");
            }
            sent += static_cast<size_t>(result);
        }
    }

    std::string m_socketPath;
    uint64_t m_virtualEpochUs = 100000;
    std::vector<ShadowFlowAuthorization> m_flows;
    InjectCallback m_inject;
    int m_fd = -1;
    uint64_t m_sequence = 0;
    uint64_t m_epochId = 0;
    uint64_t m_authorizationId = 0;
    uint64_t m_maxEpochs = 0;
    size_t m_expectedThisEpoch = 0;
    size_t m_receivedThisEpoch = 0;
    std::chrono::steady_clock::time_point m_epochWaitStarted;
    std::vector<uint8_t> m_receiveBuffer;
    std::map<uint64_t, PendingPacket> m_pending;
    std::set<uint64_t> m_seenPacketIds;
};

} // namespace ns3

#endif // GTPU_SHADOW_PEER_H
