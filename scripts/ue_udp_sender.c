#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <net/if.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define EXPERIMENT_HEADER_SIZE 27

static uint64_t host_to_network_u64(uint64_t value)
{
    uint32_t high = htonl((uint32_t)(value >> 32));
    uint32_t low = htonl((uint32_t)(value & 0xffffffffu));
    return ((uint64_t)low << 32) | high;
}

static unsigned long parse_ulong(const char* value, const char* name)
{
    char* end = NULL;
    errno = 0;
    unsigned long parsed = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0')
    {
        fprintf(stderr, "%s must be an unsigned integer\n", name);
        exit(2);
    }
    return parsed;
}

int main(int argc, char** argv)
{
    if (argc != 7 && argc != 10)
    {
        fprintf(
            stderr,
            "usage: %s TARGET_IP DEST_PORT SOURCE_PORT IFACE PACKET_SIZE PACKETS "
            "[FLOW_ID EPOCH_ID APP_SEQUENCE]\n",
            argv[0]);
        return 2;
    }

    const char* target_ip = argv[1];
    unsigned long dest_port = parse_ulong(argv[2], "DEST_PORT");
    unsigned long source_port = parse_ulong(argv[3], "SOURCE_PORT");
    const char* iface = argv[4];
    unsigned long packet_size = parse_ulong(argv[5], "PACKET_SIZE");
    unsigned long packets = parse_ulong(argv[6], "PACKETS");

    if (dest_port == 0 || dest_port > 65535 || source_port == 0 || source_port > 65535)
    {
        fprintf(stderr, "ports must be 1..65535\n");
        return 2;
    }
    if (packet_size == 0 || packet_size > 65507)
    {
        fprintf(stderr, "PACKET_SIZE must be 1..65507\n");
        return 2;
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
    {
        perror("socket");
        return 1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface)) != 0)
    {
        perror("SO_BINDTODEVICE");
        close(fd);
        return 1;
    }

    struct sockaddr_in local;
    memset(&local, 0, sizeof(local));
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = htons((uint16_t)source_port);
    if (bind(fd, (struct sockaddr*)&local, sizeof(local)) != 0)
    {
        perror("bind");
        close(fd);
        return 1;
    }

    struct sockaddr_in remote;
    memset(&remote, 0, sizeof(remote));
    remote.sin_family = AF_INET;
    remote.sin_port = htons((uint16_t)dest_port);
    if (inet_pton(AF_INET, target_ip, &remote.sin_addr) != 1)
    {
        fprintf(stderr, "invalid TARGET_IP\n");
        close(fd);
        return 2;
    }

    char* payload = malloc(packet_size);
    if (payload == NULL)
    {
        perror("malloc");
        close(fd);
        return 1;
    }
    memset(payload, ' ', packet_size);
    if (argc == 10)
    {
        const char* flow_id = argv[7];
        size_t flow_length = strlen(flow_id);
        unsigned long epoch_id = parse_ulong(argv[8], "EPOCH_ID");
        unsigned long app_sequence = parse_ulong(argv[9], "APP_SEQUENCE");
        if (flow_length > 65535 || packet_size < EXPERIMENT_HEADER_SIZE + flow_length)
        {
            fprintf(stderr, "PACKET_SIZE is too small for the experiment header\n");
            free(payload);
            close(fd);
            return 2;
        }
        memcpy(payload, "N6UD", 4);
        payload[4] = 1;
        uint16_t network_flow_length = htons((uint16_t)flow_length);
        uint64_t network_epoch = host_to_network_u64((uint64_t)epoch_id);
        uint64_t network_sequence = host_to_network_u64((uint64_t)app_sequence);
        uint32_t network_payload_length =
            htonl((uint32_t)(packet_size - EXPERIMENT_HEADER_SIZE - flow_length));
        memcpy(payload + 5, &network_flow_length, sizeof(network_flow_length));
        memcpy(payload + 7, &network_epoch, sizeof(network_epoch));
        memcpy(payload + 15, &network_sequence, sizeof(network_sequence));
        memcpy(payload + 23, &network_payload_length, sizeof(network_payload_length));
        memcpy(payload + EXPERIMENT_HEADER_SIZE, flow_id, flow_length);
    }

    for (unsigned long index = 0; index < packets; ++index)
    {
        ssize_t sent = sendto(fd, payload, packet_size, 0, (struct sockaddr*)&remote, sizeof(remote));
        if (sent != (ssize_t)packet_size)
        {
            perror("sendto");
            free(payload);
            close(fd);
            return 1;
        }
    }

    printf("source_port=%lu packets=%lu\n", source_port, packets);
    free(payload);
    close(fd);
    return 0;
}
