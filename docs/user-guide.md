# GTP-U-Aware NR User Plane

## Data Path

1. free5GC and UERANSIM establish the real control plane and PDU sessions.
2. The generated bridge script redirects each unique gNB and UPF N3 endpoint to one TAP device.
3. `bridge.user_plane.cli` reads all endpoint TAPs. IPv4/ARP destination routing and MAC learning forward control traffic only across configured N3 adjacencies.
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

Use the required `gnbs[].backhaul_upfs` list to declare all N3 peers for a gNB. `flows[].upf_ref` optionally selects one of the target gNB's connected UPFs; if omitted, the first connected UPF is used. A flow that selects an unlinked UPF fails scenario validation. The removed singular `backhaul_upf` field is rejected.

The `ns3` section controls `rng_run`, `virtual_epoch_us`, `channel_update_ms`, and `shadowing_enabled`. Reusing the same seed/run reproduces the random stream.

## Linux Requirements

The online path requires Linux, `/dev/net/tun`, `CAP_NET_ADMIN`, raw network access, Docker, free5GC/UERANSIM, and a built ns-3/5G-LENA tree. The Python lifecycle tests and `scripts/run_gate_loopback.py` run without those privileges.

The online gate supports multiple gNBs, multiple UPFs, shared endpoints, and multiple N3 edges per gNB. The scenario path is fail-closed and keeps at most the configured packet and byte capacities. A late controlled agent receives buffered current-epoch authorizations; duplicate authorization IDs are ignored after a successful send.

## Fast Reset API

Use the manifest supervisor instead of `start_stack.py` for repeated episodes:

```bash
python3 -m bridge.orchestrator.fast_reset serve artifacts/runs/RUN/run-manifest.json --initialize cold
python3 -m bridge.orchestrator.fast_reset reset
python3 -m bridge.orchestrator.fast_reset status
```

`POST /v1/reset` stops the previous process generation, clears run-owned packet/snapshot/state artifacts, restarts the existing Compose services without recreating the network, restores subscriber/application/TAP baseline state, and starts writers, gate, traffic agents, and ns-3. Split-mode manifests are supported. The response means the new process generation has started, not that every UE PDU session is already established. Do not run the standard stack runner and reset supervisor against the same manifest concurrently. A bearer token is mandatory for non-loopback API binding.

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

# Linux/WSL native ns-3 verification
python3 scripts/build_ns3_program.py --scenario scenarios/free5gc_ueransim_gtpu_nr.yaml
./ns3 run "scratch/gtpu_shadow_peer_test/gtpu_shadow_peer_test"
python3 scripts/run_native_gate_smoke.py --ns3-root /path/to/ns-3.46.1
python3 scripts/run_native_multi_n3_smoke.py --ns3-root /path/to/ns-3.46.1
```

For gated flows, 5QI must be supported by ns-3.46.1 and must agree with the requested `rlc_mode` under 5G-LENA's `PacketErrorRateBased` mapping. Invalid combinations fail during scenario validation instead of silently selecting another bearer.

On the Linux experiment host, also run `scripts/build_ns3_twin.sh` before starting the manifest. A repeated run with the same seed and run number should produce identical shadow-packet event logs.
