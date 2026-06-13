# GTP-U-Aware NR User Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** Make real free5GC user-plane GTP-U packets wait for an ns-3 NR shadow-packet result, then release or drop the original packet using ns-3 virtual time as the experiment clock.

**Architecture:** A Linux frame gate captures the two existing inline bridge directions, parses GTP-U and maps each packet to a configured NR flow. A coordinator owns the packet lifecycle and exchanges versioned messages with ns-3. ns-3 creates one shadow packet per real packet and returns delivery/drop events with virtual timestamps. Controlled UDP agents emit the next application packet only after an ns-3 authorization, while control-plane traffic remains wall-clock driven.

**Tech Stack:** Python 3.11, `unittest`, Linux TAP/AF_PACKET, free5GC/UERANSIM, ns-3/5G-LENA C++, Unix domain sockets, YAML/JSON scenario rendering.

---

## Task 1: Extend the scenario contract

**Files:**
- Modify: `bridge/common/scenario.py`
- Modify: `bridge/orchestrator/config_renderer.py`
- Modify: `pyproject.toml`
- Test: `tests/test_scenario_validation.py`
- Test: `tests/test_renderer.py`

1. Add failing validation tests for:
   - `flow.rlc_mode` accepting only `UM` or `AM`.
   - `flow.virtual_expiry_ms` being positive.
   - `ns3.rng_run`, `ns3.virtual_epoch_us`, and `ns3.channel_update_ms`.
   - `bridge.user_plane_gate` capacity and fail-closed defaults.
2. Run:

   ```powershell
   python -m unittest tests.test_scenario_validation tests.test_renderer
   ```

   Expected: failures because the new fields are not modeled or rendered.
3. Add immutable dataclasses:

   ```python
   @dataclass(frozen=True)
   class UserPlaneGateConfig:
       enabled: bool = False
       fail_closed: bool = True
       max_pending_packets: int = 8192
       max_pending_bytes: int = 64 * 1024 * 1024
       socket_path: str = "/tmp/ns3-free5gc-gate.sock"
   ```

   Add `rlc_mode="UM"` and `virtual_expiry_ms=1000.0` to `FlowConfig`; add reproducibility and channel update fields to `Ns3Config`; parse and validate all values.
4. Render the new fields into the ns-3 flow-profile TSV and runtime manifest. Include `bridge.user_plane_gate` in generated scenario data.
5. Add `bridge.user_plane` to the package list in `pyproject.toml`.
6. Re-run the focused tests and commit:

   ```powershell
   git add bridge/common/scenario.py bridge/orchestrator/config_renderer.py pyproject.toml tests/test_scenario_validation.py tests/test_renderer.py
   git commit -m "feat: model gated NR user plane"
   ```

## Task 2: Define the versioned gate protocol

**Files:**
- Create: `bridge/user_plane/__init__.py`
- Create: `bridge/user_plane/protocol.py`
- Create: `tests/test_user_plane_protocol.py`

1. Write failing tests covering:
   - Round-trip encoding for `HELLO`, `PACKET_ENQUEUE`, `PACKET_DELIVER`, `PACKET_DROP`, `TICK_COMPLETE`, and `AUTHORIZE_SEND`.
   - Rejection of bad magic, unsupported protocol version, truncated headers, and payload-length mismatch.
   - Incremental stream decoding when a frame arrives in fragments.
2. Run:

   ```powershell
   python -m unittest tests.test_user_plane_protocol
   ```

3. Implement a fixed network-byte-order header:

   ```python
   HEADER = struct.Struct("!4sBBHIQ")
   MAGIC = b"N6AI"
   VERSION = 1
   ```

   The header carries magic, version, message type, flags, payload length, and sequence number. Payloads use deterministic UTF-8 JSON with sorted keys. Implement `encode_message`, `decode_message`, and `StreamDecoder`.
4. Re-run tests and commit:

   ```powershell
   git add bridge/user_plane tests/test_user_plane_protocol.py
   git commit -m "feat: add user-plane gate protocol"
   ```

## Task 3: Parse and classify real GTP-U packets

**Files:**
- Create: `bridge/user_plane/gtpu.py`
- Create: `tests/test_gtpu.py`

1. Write packet-builder helpers and failing tests for:
   - Ethernet, optional 802.1Q VLAN, IPv4, UDP/2152, and GTP-U G-PDU parsing.
   - GTP-U optional fields and chained extension headers.
   - PDU Session Container QFI extraction.
   - Inner IPv4 UDP tuple extraction.
   - Uplink/downlink classification by outer endpoints.
   - QFI-first mapping with UE PDU IP and inner five-tuple fallback.
   - Fail-closed behavior for malformed or unmapped G-PDUs.
   - Explicit bypass for ARP, control-plane traffic, and non-G-PDU GTP-U messages.
2. Run:

   ```powershell
   python -m unittest tests.test_gtpu
   ```

3. Implement bounds-checked dataclasses:

   ```python
   @dataclass(frozen=True)
   class GtpuPacket:
       teid: int
       qfi: int | None
       inner_src: IPv4Address
       inner_dst: IPv4Address
       inner_protocol: int
       inner_src_port: int | None
       inner_dst_port: int | None
   ```

   Never scan raw bytes heuristically; advance through Ethernet, IP, UDP, GTP-U, and extension-header lengths.
4. Implement `FlowBinding` and `FlowClassifier`, returning a typed decision: `CONTROL_BYPASS`, `MANAGED`, `UNMAPPED`, or `MALFORMED`.
5. Re-run tests and commit:

   ```powershell
   git add bridge/user_plane/gtpu.py tests/test_gtpu.py
   git commit -m "feat: classify GTP-U packets into NR flows"
   ```

## Task 4: Implement the virtual-time packet lifecycle

**Files:**
- Create: `bridge/user_plane/coordinator.py`
- Create: `tests/test_user_plane_coordinator.py`

1. Write failing tests for:
   - Monotonic packet IDs and epoch IDs.
   - `CAPTURED -> SUBMITTED -> DELIVERED|DROPPED -> RELEASED|DISCARDED`.
   - Duplicate, late, unknown, and cross-epoch results being rejected.
   - Tick completion only after every packet in the epoch reaches a terminal ns-3 result.
   - Packet/byte capacity enforcement.
   - Virtual expiry based on ns-3 time, never wall-clock time.
   - Fail-closed behavior when the ns-3 peer disconnects.
2. Run:

   ```powershell
   python -m unittest tests.test_user_plane_coordinator
   ```

3. Implement `PacketRecord`, `PacketState`, `EpochRecord`, and `PacketCoordinator`. Keep state transitions in one guarded method and expose terminal release/drop actions to the gate.
4. Re-run tests and commit:

   ```powershell
   git add bridge/user_plane/coordinator.py tests/test_user_plane_coordinator.py
   git commit -m "feat: coordinate packets on ns-3 virtual time"
   ```

## Task 5: Compute KPI values from packet events

**Files:**
- Create: `bridge/user_plane/kpi.py`
- Create: `tests/test_user_plane_kpi.py`

1. Write failing tests for:
   - One-way virtual delay as `deliver_ns3_us - enqueue_ns3_us`.
   - RFC 3393-style IPDV as the difference between consecutive one-way delays in the same flow/direction.
   - Per-tick p50/p95/p99, delivered bytes, drops, and throughput.
   - Exclusion of warm-up packets without fabricating replacement values.
   - Invariants: submitted equals delivered plus dropped after tick completion; timestamps are monotonic and non-negative.
2. Run:

   ```powershell
   python -m unittest tests.test_user_plane_kpi
   ```

3. Implement an event-driven collector using only ns-3 virtual timestamps. Do not derive drop decisions from SLA thresholds and do not synthesize KPI samples.
4. Re-run tests and commit:

   ```powershell
   git add bridge/user_plane/kpi.py tests/test_user_plane_kpi.py
   git commit -m "feat: derive KPIs from virtual packet events"
   ```

## Task 6: Add the controlled UDP agent

**Files:**
- Create: `bridge/user_plane/udp_agent.py`
- Create: `tests/test_controlled_udp_agent.py`
- Modify: `scripts/run_real_ue_flows.py`
- Modify: `tests/test_real_ue_flows.py`

1. Write failing tests for:
   - No UDP send before `AUTHORIZE_SEND`.
   - Exactly one datagram per authorization.
   - Experiment header containing flow ID, epoch ID, application sequence, and payload length.
   - Duplicate authorization idempotence.
   - Downlink and uplink destination selection.
2. Implement a socket-injected `ControlledUdpAgent` so tests use loopback/fakes. Add a `--controlled` mode to `run_real_ue_flows.py`; preserve the existing free-running mode for compatibility.
3. Re-run tests and commit:

   ```powershell
   python -m unittest tests.test_controlled_udp_agent tests.test_real_ue_flows
   git add bridge/user_plane/udp_agent.py scripts/run_real_ue_flows.py tests/test_controlled_udp_agent.py tests/test_real_ue_flows.py
   git commit -m "feat: gate UDP generation with ns-3 authorization"
   ```

## Task 7: Build the Linux frame gate

**Files:**
- Create: `bridge/user_plane/gate.py`
- Create: `bridge/user_plane/frame_port.py`
- Create: `bridge/user_plane/cli.py`
- Create: `tests/test_user_plane_gate.py`
- Modify: `adapters/free5gc_ueransim/bridge_setup.py`
- Modify: `tests/test_bridge_setup.py`

1. Write failing in-memory integration tests:
   - Control frames pass immediately.
   - Managed G-PDUs remain pending until `PACKET_DELIVER`.
   - `PACKET_DROP` discards the original frame.
   - Unmapped/malformed G-PDUs are discarded and counted.
   - Peer disconnect discards all pending managed frames.
   - Capacity overflow discards the newly captured frame.
2. Implement `FramePort` plus Linux `AfPacketFramePort`; keep privileged I/O behind the interface so the lifecycle is testable on Windows.
3. Implement an asyncio gate loop connecting two Linux interfaces and a Unix socket. The gate sends `PACKET_ENQUEUE` metadata and never sends frame payloads to ns-3; the original bytes remain in bounded local memory.
4. Change bridge setup so managed interfaces are attached to the gate rather than directly redirected through both TAPs. Preserve cleanup and dry-run behavior.
5. Add the CLI entry point:

   ```toml
   ns3-free5gc-gate = "bridge.user_plane.cli:main"
   ```

6. Re-run tests and commit:

   ```powershell
   python -m unittest tests.test_user_plane_gate tests.test_bridge_setup
   git add bridge/user_plane adapters/free5gc_ueransim/bridge_setup.py tests/test_user_plane_gate.py tests/test_bridge_setup.py pyproject.toml
   git commit -m "feat: hold real GTP-U frames behind NR decisions"
   ```

## Task 8: Render gate and bearer-map runtime assets

**Files:**
- Modify: `bridge/orchestrator/config_renderer.py`
- Modify: `bridge/split_mode/renderer.py`
- Modify: `tests/test_renderer.py`
- Modify: `tests/test_split_mode.py`
- Create: `scenarios/free5gc_ueransim_gtpu_nr.yaml`

1. Add failing renderer tests for:
   - `user-plane-gate.json` containing interface names, socket, limits, fail-closed policy, and outer endpoint roles.
   - `bearer-map.json` containing flow ID, QFI, UE PDU IP, tuple fallback, RLC mode, and virtual expiry.
   - Startup ordering: bridge preparation, gate, ns-3 peer readiness, controlled agents.
   - Cleanup always stopping the gate and removing interfaces/socket.
2. Implement rendering and command specs without shell-string interpolation of untrusted values.
3. Add a complete example scenario with fixed UE/gNB positions, UMi channel, shadowing, channel updates, `rng_seed`, `rng_run`, and UM default.
4. Re-run tests and commit:

   ```powershell
   python -m unittest tests.test_renderer tests.test_split_mode
   git add bridge/orchestrator/config_renderer.py bridge/split_mode/renderer.py tests/test_renderer.py tests/test_split_mode.py scenarios/free5gc_ueransim_gtpu_nr.yaml
   git commit -m "feat: render gated NR experiment assets"
   ```

## Task 9: Add the ns-3 shadow-packet peer

**Files:**
- Create: `sim/ns3/gtpu_shadow_peer.h`
- Create: `sim/ns3/gtpu_shadow_peer.cc`
- Create: `sim/ns3/gtpu_shadow_peer_test.cc`
- Modify: `sim/ns3/nr_multignb_multiupf_split.cc`
- Modify: `scripts/build_ns3_program.py`
- Modify: `tests/test_ns3_program.py`

1. Add a native protocol/state test that feeds fragmented messages and verifies:
   - One ns-3 packet is created for each enqueue.
   - Packet tags retain packet ID, flow ID, epoch ID, and virtual enqueue time.
   - RX callback emits one delivery event.
   - RLC/PHY discard and virtual-expiry paths emit one drop event.
   - An epoch-complete event is emitted only after all packet IDs terminate.
2. Extend the build helper to copy the peer `.h/.cc` beside the scratch program and compile the native test when a compiler is available.
3. Implement a non-blocking Unix-domain socket peer integrated with ns-3 through short polling events. Socket readiness must not advance simulation time; packet outcomes use `Simulator::Now()`.
4. Replace synthetic per-flow datagram generation for controlled flows with packet injection from `PACKET_ENQUEUE`. Attach bearer/QFI metadata and use configured UM/AM RLC mode.
5. Emit `AUTHORIZE_SEND` only at the start of the next virtual epoch, after the previous epoch completes.
6. Bind `RngSeedManager::SetSeed` and `SetRun`; apply the configured NR TDD pattern and channel update period to actual 5G-LENA attributes.
7. Re-run tests and commit:

   ```powershell
   python -m unittest tests.test_ns3_program
   git add sim/ns3 scripts/build_ns3_program.py tests/test_ns3_program.py
   git commit -m "feat: drive NR shadow packets from real GTP-U arrivals"
   ```

## Task 10: Remove synthetic KPI decisions and expose event logs

**Files:**
- Modify: `sim/ns3/nr_multignb_multiupf_split.cc`
- Modify: `bridge/orchestrator/metrics.py`
- Modify: `tests/test_metrics.py`
- Modify: `tests/test_ns3_program.py`

1. Add failing tests that reject:
   - SLA-threshold-driven packet drops.
   - Fabricated latency/jitter values.
   - Cumulative FlowMonitor values mislabeled as per-tick samples.
2. Emit packet event JSONL and per-tick KPI JSONL from actual enqueue/deliver/drop events. Keep FlowMonitor output only as a cross-check.
3. Update metrics ingestion to validate conservation and timestamp invariants, and surface invalid runs instead of repairing them silently.
4. Re-run tests and commit:

   ```powershell
   python -m unittest tests.test_metrics tests.test_ns3_program
   git add sim/ns3/nr_multignb_multiupf_split.cc bridge/orchestrator/metrics.py tests/test_metrics.py tests/test_ns3_program.py
   git commit -m "fix: report only observed NR packet metrics"
   ```

## Task 11: Add an end-to-end deterministic loopback test

**Files:**
- Create: `tests/test_gated_user_plane_e2e.py`
- Create: `scripts/run_gate_loopback.py`
- Modify: `README.md`
- Modify: `docs/user-guide.md`

1. Build a test with in-memory frame ports and a deterministic fake ns-3 peer:
   - Inject two G-PDUs and one control frame.
   - Deliver the first at virtual time 1200 us and drop the second at 1500 us.
   - Verify exact released bytes, drop count, delay, IPDV inputs, epoch completion, and next-send authorization.
2. Add a runnable local demonstration that prints the packet-event and KPI records.
3. Document Linux privileges, topology, startup order, virtual-time semantics, failure behavior, and the boundary between the deterministic test peer and real 5G-LENA execution.
4. Run:

   ```powershell
   python -m unittest tests.test_gated_user_plane_e2e
   python scripts/run_gate_loopback.py
   ```

5. Commit:

   ```powershell
   git add tests/test_gated_user_plane_e2e.py scripts/run_gate_loopback.py README.md docs/user-guide.md
   git commit -m "test: verify gated user plane end to end"
   ```

## Task 12: Full verification

1. Run the complete Python suite:

   ```powershell
   python -m unittest discover -s tests -p "test_*.py"
   ```

2. Run static syntax checks:

   ```powershell
   python -m compileall bridge scripts tests
   ```

3. On the configured Linux ns-3 host, build and run the native peer test and a short fixed-seed scenario:

   ```bash
   python3 scripts/build_ns3_program.py --scenario scenarios/free5gc_ueransim_gtpu_nr.yaml
   ./ns3 run "scratch/gtpu_shadow_peer_test"
   ./ns3 run "scratch/nr_multignb_multiupf_split --duration=2 --rngSeed=1 --rngRun=1"
   ```

4. Verify artifacts:
   - Every managed packet ID has exactly one terminal event.
   - No original managed frame is released before its delivery event.
   - All KPI timestamps are ns-3 virtual timestamps.
   - Repeating seed/run produces identical packet-event logs.
   - Changing `rng_run` changes fading outcomes.
5. Review the diff and commit any final documentation/test fixes.
