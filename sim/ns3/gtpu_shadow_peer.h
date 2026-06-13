// SPDX-License-Identifier: GPL-2.0-only
#ifndef GTPU_SHADOW_PEER_H
#define GTPU_SHADOW_PEER_H

#include "ns3/callback.h"
#include "ns3/core-module.h"
#include "ns3/object.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <map>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

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
                   InjectCallback inject)
    {
        m_socketPath = socketPath;
        m_virtualEpochUs = virtualEpochUs;
        m_flows = flows;
        m_inject = inject;
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

    void Connect()
    {
        m_fd = socket(AF_UNIX, SOCK_STREAM, 0);
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
        while (connect(m_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
        {
            if (errno != ENOENT && errno != ECONNREFUSED)
            {
                throw std::runtime_error("failed to connect to user-plane gate");
            }
            usleep(100000);
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
        }
        ++m_epochId;
        m_receivedThisEpoch = 0;
        m_expectedThisEpoch = m_flows.size() * 2;
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
            pollfd descriptor{m_fd, POLLIN, 0};
            poll(&descriptor, 1, 10);
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
        request.virtualExpiryUs = FindUnsigned(payload, "virtual_expiry_us");
        if (request.epochId != m_epochId)
        {
            throw std::runtime_error("received packet for the wrong virtual epoch");
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
    size_t m_expectedThisEpoch = 0;
    size_t m_receivedThisEpoch = 0;
    std::vector<uint8_t> m_receiveBuffer;
    std::map<uint64_t, PendingPacket> m_pending;
};

} // namespace ns3

#endif // GTPU_SHADOW_PEER_H
