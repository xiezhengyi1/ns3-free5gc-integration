from __future__ import annotations

from collections import deque
import os
import socket
import struct
from typing import Protocol


class FramePort(Protocol):
    def write(self, frame: bytes) -> int:
        ...


class MemoryFramePort:
    def __init__(self, incoming_frames: list[bytes] | None = None) -> None:
        self._incoming = deque(incoming_frames or [])
        self.written_frames: list[bytes] = []

    def read(self) -> bytes:
        return self._incoming.popleft()

    def write(self, frame: bytes) -> int:
        copied = bytes(frame)
        self.written_frames.append(copied)
        return len(copied)


class AfPacketFramePort:
    ETH_P_ALL = 0x0003

    def __init__(self, interface_name: str) -> None:
        if not hasattr(socket, "AF_PACKET"):
            raise OSError("AF_PACKET frame ports are available only on Linux")
        self.interface_name = interface_name
        self.socket = socket.socket(
            socket.AF_PACKET,
            socket.SOCK_RAW,
            socket.htons(self.ETH_P_ALL),
        )
        self.socket.bind((interface_name, 0))

    def fileno(self) -> int:
        return self.socket.fileno()

    def read(self, max_frame_bytes: int = 65535) -> bytes:
        return self.socket.recv(max_frame_bytes)

    def write(self, frame: bytes) -> int:
        return self.socket.send(frame)

    def close(self) -> None:
        self.socket.close()


class TunTapFramePort:
    TUNSETIFF = 0x400454CA
    IFF_TAP = 0x0002
    IFF_NO_PI = 0x1000

    def __init__(self, interface_name: str) -> None:
        if os.name != "posix":
            raise OSError("TAP frame ports are available only on POSIX systems")
        import fcntl

        self._fd = os.open("/dev/net/tun", os.O_RDWR | os.O_NONBLOCK)
        request = struct.pack(
            "16sH",
            interface_name.encode("ascii"),
            self.IFF_TAP | self.IFF_NO_PI,
        )
        response = fcntl.ioctl(self._fd, self.TUNSETIFF, request)
        self.interface_name = response[:16].split(b"\x00", 1)[0].decode("ascii")

    def fileno(self) -> int:
        return self._fd

    def read(self, max_frame_bytes: int = 65535) -> bytes:
        return os.read(self._fd, max_frame_bytes)

    def write(self, frame: bytes) -> int:
        return os.write(self._fd, frame)

    def close(self) -> None:
        os.close(self._fd)
