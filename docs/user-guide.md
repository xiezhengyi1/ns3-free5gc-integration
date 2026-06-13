# GTP-U-Aware NR User Plane

## Data Path

1. free5GC and UERANSIM establish the real control plane and PDU sessions.
2. The generated bridge script redirects the gNB and UPF N3 interfaces to two TAP devices.
3. `bridge.user_plane.cli` reads both TAPs. ARP and non-G-PDU control traffic passes immediately.
4. A mapped G-PDU is retained in bounded memory and submitted to ns-3 as metadata.
5. ns-3 injects a tagged shadow UDP packet through EPC and the configured NR bearer.
6. NR delivery releases the original frame. Socket failure, virtual expiry, malformed input, and peer loss drop it.

The original GTP-U bytes never enter ns-3. Their release is controlled one-for-one by the corresponding NR shadow packet.

## Virtual Time

- `EPOCH_START`, enqueue, delivery, drop, expiry, and KPI timestamps use ns-3 microseconds.
- The peer blocks at an epoch barrier while waiting for authorized real packets; wall-clock waiting does not advance simulation time.
- The next UDP datagram is generated only after `AUTHORIZE_SEND`.
- Delay is `deliver_ns3_us - enqueue_ns3_us`.
- IPDV is the difference between consecutive one-way delays for the same flow and direction.
- SLA targets are configuration metadata, not packet-drop or KPI generators.

## Scenario Fields

Each gated flow must define `ue_ip` and either `qfi` or an inner tuple using `inner_protocol`, `ue_port`, and `remote_port`. `rlc_mode` accepts `UM` or `AM`; `virtual_expiry_ms` is an ns-3-time deadline.

The `ns3` section controls `rng_run`, `virtual_epoch_us`, `channel_update_ms`, and `shadowing_enabled`. Reusing the same seed/run reproduces the random stream.

## Linux Requirements

The online path requires Linux, `/dev/net/tun`, `CAP_NET_ADMIN`, raw network access, Docker, free5GC/UERANSIM, and a built ns-3/5G-LENA tree. The Python lifecycle tests and `scripts/run_gate_loopback.py` run without those privileges.

The first online implementation supports one gNB-UPF N3 link. The gate is fail-closed and keeps at most the configured packet and byte capacities.

## Generated Assets

- `generated/user-plane-gate.json`: TAPs, socket paths, limits, N3 endpoints, and flow bindings.
- `generated/bearer-map.json`: RLC mode, 5QI, virtual expiry, seed/run, and channel parameters.
- `generated/ns3/packet-events.jsonl`: observed submitted/delivered/dropped packet events.
- `generated/ns3/packet-kpis.jsonl`: per-epoch delay, IPDV, throughput, and drop summaries.

## Verification

```bash
python -m unittest tests.test_gated_user_plane_e2e
python scripts/run_gate_loopback.py
python -m unittest discover -s tests -p "test_*.py"
```

On the Linux experiment host, also run `scripts/build_ns3_twin.sh` before starting the manifest. A repeated run with the same seed and run number should produce identical shadow-packet event logs.
