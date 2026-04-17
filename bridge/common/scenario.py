"""Scenario configuration models and YAML loading."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def _resolve_path(value: str | Path) -> str:
    return str(Path(value).expanduser().resolve())


def _coerce_slice_identifier(sst: int, sd: str) -> str:
    return f"slice-{sst}-{sd.lower()}"


@dataclass(slots=True, frozen=True)
class SliceConfig:
    sst: int
    sd: str
    label: str | None = None

    @property
    def slice_id(self) -> str:
        return _coerce_slice_identifier(self.sst, self.sd)


@dataclass(slots=True, frozen=True)
class SessionConfig:
    apn: str
    slice_ref: str
    session_type: str = "IPv4"
    five_qi: int = 9
    app_id: str = "default-app"


@dataclass(slots=True, frozen=True)
class UpfConfig:
    name: str
    role: str = "upf"
    dnn: str = "internet"


@dataclass(slots=True, frozen=True)
class GnbConfig:
    name: str
    alias: str
    tac: int = 1
    nci: str = "0x000000010"
    slices: tuple[str, ...] = ()
    backhaul_upf: str | None = None


@dataclass(slots=True, frozen=True)
class UeConfig:
    name: str
    supi: str
    gnb: str
    key: str
    op: str
    op_type: str = "OPC"
    amf: str = "8000"
    sessions: tuple[SessionConfig, ...] = ()


@dataclass(slots=True, frozen=True)
class Free5gcConfig:
    compose_file: str
    config_root: str
    bridge_name: str = "br-free5gc"
    mode: str = "single_upf"
    project_name: str = "ns3int"


@dataclass(slots=True, frozen=True)
class Ns3Config:
    ns3_root: str
    scratch_name: str = "nr_multignb_multiupf"
    output_subdir: str = "ns3"
    simulator: str = "RealtimeSimulatorImpl"
    sim_time_ms: int = 30000


@dataclass(slots=True, frozen=True)
class WriterConfig:
    archive_dir: str = "artifacts/archive"
    state_db: str = "artifacts/state/writer.db"
    ingestion_url: str | None = None


@dataclass(slots=True, frozen=True)
class BridgeHarnessConfig:
    enable_inline_harness: bool = False
    gnb_prefix: str = "tap-gnb"
    ue_prefix: str = "tap-ue"
    bridge_prefix: str = "br-ran"
    host_veth_prefix: str = "veth-ran"


@dataclass(slots=True, frozen=True)
class ScenarioConfig:
    name: str
    scenario_id: str
    tick_ms: int
    seed: int
    slices: tuple[SliceConfig, ...]
    upfs: tuple[UpfConfig, ...]
    gnbs: tuple[GnbConfig, ...]
    ues: tuple[UeConfig, ...]
    free5gc: Free5gcConfig
    ns3: Ns3Config
    writer: WriterConfig = field(default_factory=WriterConfig)
    bridge: BridgeHarnessConfig = field(default_factory=BridgeHarnessConfig)

    def slice_map(self) -> dict[str, SliceConfig]:
        return {slice_config.slice_id: slice_config for slice_config in self.slices}

    def gnb_map(self) -> dict[str, GnbConfig]:
        return {gnb.name: gnb for gnb in self.gnbs}

    def ue_groups(self) -> dict[str, list[UeConfig]]:
        grouped: dict[str, list[UeConfig]] = {gnb.name: [] for gnb in self.gnbs}
        for ue in self.ues:
            grouped.setdefault(ue.gnb, []).append(ue)
        return grouped

    def validate(self) -> None:
        slices = self.slice_map()
        gnbs = self.gnb_map()
        if not self.gnbs:
            raise ValueError("scenario must define at least one gNB")
        if not self.ues:
            raise ValueError("scenario must define at least one UE")
        for ue in self.ues:
            if ue.gnb not in gnbs:
                raise ValueError(f"UE {ue.name} references unknown gNB {ue.gnb}")
            if not ue.sessions:
                raise ValueError(f"UE {ue.name} must define at least one session")
            for session in ue.sessions:
                if session.slice_ref not in slices:
                    raise ValueError(
                        f"UE {ue.name} session references unknown slice {session.slice_ref}"
                    )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ScenarioConfig":
        slices = tuple(
            SliceConfig(
                sst=int(item["sst"]),
                sd=str(item["sd"]),
                label=item.get("label"),
            )
            for item in payload.get("slices", [])
        )

        slice_map = {_coerce_slice_identifier(item.sst, item.sd): item for item in slices}
        upfs = tuple(
            UpfConfig(
                name=item["name"],
                role=item.get("role", "upf"),
                dnn=item.get("dnn", "internet"),
            )
            for item in payload.get("upfs", [])
        )

        gnbs = []
        for index, item in enumerate(payload.get("gnbs", []), start=1):
            alias = item.get("alias") or f"gnb-{index}.free5gc.org"
            gnb = GnbConfig(
                name=item["name"],
                alias=alias,
                tac=int(item.get("tac", 1)),
                nci=str(item.get("nci", f"0x{index:09x}")),
                slices=tuple(item.get("slices", tuple(slice_map.keys()))),
                backhaul_upf=item.get("backhaul_upf"),
            )
            gnbs.append(gnb)

        ues = []
        for item in payload.get("ues", []):
            sessions = tuple(
                SessionConfig(
                    apn=session.get("apn", "internet"),
                    slice_ref=session["slice_ref"],
                    session_type=session.get("type", "IPv4"),
                    five_qi=int(session.get("five_qi", 9)),
                    app_id=session.get("app_id", f"app-{item['name']}"),
                )
                for session in item.get("sessions", [])
            )
            ues.append(
                UeConfig(
                    name=item["name"],
                    supi=item["supi"],
                    gnb=item["gnb"],
                    key=item["key"],
                    op=item["op"],
                    op_type=item.get("op_type", "OPC"),
                    amf=str(item.get("amf", "8000")),
                    sessions=sessions,
                )
            )

        free5gc_payload = payload["free5gc"]
        ns3_payload = payload["ns3"]
        writer_payload = payload.get("writer", {})
        bridge_payload = payload.get("bridge", {})

        scenario = cls(
            name=payload["name"],
            scenario_id=payload["scenario_id"],
            tick_ms=int(payload.get("tick_ms", 1000)),
            seed=int(payload.get("seed", 1)),
            slices=slices,
            upfs=upfs,
            gnbs=tuple(gnbs),
            ues=tuple(ues),
            free5gc=Free5gcConfig(
                compose_file=_resolve_path(free5gc_payload["compose_file"]),
                config_root=_resolve_path(free5gc_payload["config_root"]),
                bridge_name=free5gc_payload.get("bridge_name", "br-free5gc"),
                mode=free5gc_payload.get("mode", "single_upf"),
                project_name=free5gc_payload.get("project_name", "ns3int"),
            ),
            ns3=Ns3Config(
                ns3_root=_resolve_path(ns3_payload["ns3_root"]),
                scratch_name=ns3_payload.get("scratch_name", "nr_multignb_multiupf"),
                output_subdir=ns3_payload.get("output_subdir", "ns3"),
                simulator=ns3_payload.get("simulator", "RealtimeSimulatorImpl"),
                sim_time_ms=int(ns3_payload.get("sim_time_ms", 30000)),
            ),
            writer=WriterConfig(
                archive_dir=writer_payload.get("archive_dir", "artifacts/archive"),
                state_db=writer_payload.get("state_db", "artifacts/state/writer.db"),
                ingestion_url=writer_payload.get("ingestion_url"),
            ),
            bridge=BridgeHarnessConfig(
                enable_inline_harness=bool(
                    bridge_payload.get("enable_inline_harness", False)
                ),
                gnb_prefix=bridge_payload.get("gnb_prefix", "tap-gnb"),
                ue_prefix=bridge_payload.get("ue_prefix", "tap-ue"),
                bridge_prefix=bridge_payload.get("bridge_prefix", "br-ran"),
                host_veth_prefix=bridge_payload.get("host_veth_prefix", "veth-ran"),
            ),
        )
        scenario.validate()
        return scenario


def load_scenario(path: str | Path) -> ScenarioConfig:
    with Path(path).expanduser().resolve().open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("scenario YAML root must be a mapping")
    return ScenarioConfig.from_dict(payload)