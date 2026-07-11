# Refactor Completion Audit

## Completed

- Multi-gNB/multi-UPF N3 topology is represented as explicit `backhaul_upfs` edges; per-flow `upf_ref` selects and validates the real outer GTP-U edge.
- Endpoint-aware TAP routing supports shared gNB/UPF endpoints without the implicit two-port fallback.
- Gate, packet coordinator, classifier, KPI collector, protocol, and ns-3 peer have explicit ownership boundaries and fail-closed behavior.
- Removed compatibility paths: singular `backhaul_upf`, `gnb_to_upf`, legacy top-level two-TAP gate JSON, configurable fail-open, host-side virtual expiry, and free-running real UE traffic.
- Standard and split-mode manifests expose the fast reset API with generation-scoped process supervision and run-owned state cleanup.
- Native ns-3 peer, gated packet path, deterministic RNG, and 2-gNB/2-UPF selection have automated smoke coverage.

## Remaining Validation

- Run the fully privileged Docker/UERANSIM/multi-TAP composition and measure real reset latency on the target Linux experiment host. This environment validates native ns-3 and in-memory routing but cannot prove host-specific TAP timing.
- Confirm NF-specific convergence time after Compose restart for the exact deployed free5GC images. The API reports process startup; it intentionally does not claim PDU-session readiness.
- Per-edge N3 rate, delay, and loss remain global ns-3 bridge parameters. Add edge-specific channel configuration only if experiments require heterogeneous N3 transport.
- External graph history is retained across episodes. A destructive graph reset needs a separate, explicitly scoped storage API and is not part of fast scene reset.
