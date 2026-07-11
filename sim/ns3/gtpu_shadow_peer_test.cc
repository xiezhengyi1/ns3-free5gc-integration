// SPDX-License-Identifier: GPL-2.0-only

#include "gtpu_shadow_peer.h"
#include "ns3/core-module.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace ns3;

namespace
{

uint64_t
HostToNetwork64(uint64_t value)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (static_cast<uint64_t>(htonl(static_cast<uint32_t>(value))) << 32) |
           htonl(static_cast<uint32_t>(value >> 32));
#else
    return value;
#endif
}

std::vector<uint8_t>
Encode(uint8_t type, uint64_t sequence, const std::string& payload)
{
    std::vector<uint8_t> frame(20 + payload.size(), 0);
    std::memcpy(frame.data(), "N6AI", 4);
    frame[4] = 1;
    frame[5] = type;
    const uint32_t networkLength = htonl(static_cast<uint32_t>(payload.size()));
    const uint64_t networkSequence = HostToNetwork64(sequence);
    std::memcpy(frame.data() + 8, &networkLength, sizeof(networkLength));
    std::memcpy(frame.data() + 12, &networkSequence, sizeof(networkSequence));
    std::memcpy(frame.data() + 20, payload.data(), payload.size());
    return frame;
}

void
SendAll(int fd, const uint8_t* data, size_t size)
{
    size_t sent = 0;
    while (sent < size)
    {
        const ssize_t result = send(fd, data + sent, size - sent, MSG_NOSIGNAL);
        if (result <= 0)
        {
            throw std::runtime_error("native test failed to write socket");
        }
        sent += static_cast<size_t>(result);
    }
}

void
SendFragmented(int fd, const std::vector<uint8_t>& frame)
{
    const size_t first = std::min<size_t>(7, frame.size());
    const size_t second = std::min<size_t>(17, frame.size());
    SendAll(fd, frame.data(), first);
    if (second > first)
    {
        SendAll(fd, frame.data() + first, second - first);
    }
    if (frame.size() > second)
    {
        SendAll(fd, frame.data() + second, frame.size() - second);
    }
}

void
OnShadowPacketRequest(Ptr<GtpuShadowPeer> peer,
                      std::vector<ShadowPacketRequest>* requests,
                      ShadowPacketRequest request)
{
    requests->push_back(request);
    if (request.direction == "uplink")
    {
        peer->Deliver(request.packetId);
    }
}

void
RunGateServer(const std::string& socketPath,
              std::atomic<bool>* ready,
              std::atomic<uint32_t>* delivered,
              std::atomic<uint32_t>* dropped,
              std::atomic<uint32_t>* completed,
              std::string* error)
{
    int listener = -1;
    int connection = -1;
    try
    {
        unlink(socketPath.c_str());
        listener = socket(AF_UNIX, SOCK_STREAM, 0);
        if (listener < 0)
        {
            throw std::runtime_error("native test failed to create listener");
        }
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::strncpy(address.sun_path, socketPath.c_str(), sizeof(address.sun_path) - 1);
        if (bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
            listen(listener, 1) != 0)
        {
            throw std::runtime_error("native test failed to bind listener");
        }
        ready->store(true);
        connection = accept(listener, nullptr, nullptr);
        if (connection < 0)
        {
            throw std::runtime_error("native test failed to accept peer");
        }

        SendFragmented(
            connection,
            Encode(GtpuShadowPeer::PACKET_ENQUEUE,
                   1,
                   "{\"packet_id\":1,\"epoch_id\":1,\"flow_id\":\"flow-smoke\"," 
                   "\"direction\":\"uplink\",\"size_bytes\":128,\"qfi\":9,"
                   "\"enqueue_ns3_us\":0,\"virtual_expiry_us\":500}"));
        SendFragmented(
            connection,
            Encode(GtpuShadowPeer::PACKET_ENQUEUE,
                   2,
                   "{\"packet_id\":2,\"epoch_id\":1,\"flow_id\":\"flow-smoke\"," 
                   "\"direction\":\"downlink\",\"size_bytes\":96,\"qfi\":9,"
                   "\"enqueue_ns3_us\":0,\"virtual_expiry_us\":100}"));

        std::vector<uint8_t> receiveBuffer;
        uint8_t buffer[4096];
        while (completed->load() == 0)
        {
            const ssize_t count = recv(connection, buffer, sizeof(buffer), 0);
            if (count <= 0)
            {
                throw std::runtime_error("native test peer closed before tick completion");
            }
            receiveBuffer.insert(receiveBuffer.end(), buffer, buffer + count);
            while (receiveBuffer.size() >= 20)
            {
                uint32_t networkLength = 0;
                std::memcpy(&networkLength, receiveBuffer.data() + 8, sizeof(networkLength));
                const size_t frameSize = 20 + ntohl(networkLength);
                if (receiveBuffer.size() < frameSize)
                {
                    break;
                }
                const uint8_t type = receiveBuffer[5];
                if (type == GtpuShadowPeer::PACKET_DELIVER)
                {
                    ++(*delivered);
                }
                else if (type == GtpuShadowPeer::PACKET_DROP)
                {
                    ++(*dropped);
                }
                else if (type == GtpuShadowPeer::TICK_COMPLETE)
                {
                    ++(*completed);
                }
                receiveBuffer.erase(receiveBuffer.begin(), receiveBuffer.begin() + frameSize);
            }
        }
    }
    catch (const std::exception& exception)
    {
        *error = exception.what();
    }
    if (connection >= 0)
    {
        close(connection);
    }
    if (listener >= 0)
    {
        close(listener);
    }
    unlink(socketPath.c_str());
}

} // namespace

int
main(int argc, char* argv[])
{
    uint64_t virtualEpochUs = 1000;
    CommandLine cmd(__FILE__);
    cmd.AddValue("virtualEpochUs", "Virtual epoch duration used by the native test", virtualEpochUs);
    cmd.Parse(argc, argv);

    const std::string socketPath =
        "/tmp/ns3-free5gc-shadow-peer-test-" + std::to_string(getpid()) + ".sock";
    std::atomic<bool> ready{false};
    std::atomic<uint32_t> delivered{0};
    std::atomic<uint32_t> dropped{0};
    std::atomic<uint32_t> completed{0};
    std::string serverError;
    std::thread server(RunGateServer,
                       socketPath,
                       &ready,
                       &delivered,
                       &dropped,
                       &completed,
                       &serverError);
    while (!ready.load())
    {
        std::this_thread::yield();
    }

    Ptr<GtpuShadowPeer> peer = CreateObject<GtpuShadowPeer>();
    std::vector<ShadowPacketRequest> requests;
    std::vector<ShadowFlowAuthorization> flows = {
        ShadowFlowAuthorization{"flow-smoke", 128},
    };
    peer->Configure(socketPath,
                    virtualEpochUs,
                    flows,
                    MakeBoundCallback(&OnShadowPacketRequest, peer, &requests),
                    1);
    peer->Start();
    Simulator::Run();
    server.join();
    Simulator::Destroy();

    if (!serverError.empty() || requests.size() != 2 || delivered.load() != 1 ||
        dropped.load() != 1 || completed.load() != 1)
    {
        std::cerr << "gtpu_shadow_peer_test failed error=" << serverError
                  << " requests=" << requests.size()
                  << " delivered=" << delivered.load()
                  << " dropped=" << dropped.load()
                  << " completed=" << completed.load() << std::endl;
        return 1;
    }
    if (requests[0].packetId != 1 || requests[0].epochId != 1 ||
        requests[0].flowId != "flow-smoke" || requests[0].sizeBytes != 128)
    {
        std::cerr << "gtpu_shadow_peer_test did not retain packet identity" << std::endl;
        return 1;
    }
    if (requests[0].qfi != 9 || requests[0].enqueueNs3Us != 0)
    {
        std::cerr << "gtpu_shadow_peer_test did not retain QFI or enqueue time" << std::endl;
        return 1;
    }

    std::cout << "gtpu_shadow_peer_test passed requests=2 delivered=1 dropped=1 tick_complete=1"
              << std::endl;
    return 0;
}
