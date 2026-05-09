"""Configuration loading for split control-plane/user-plane mode."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import yaml

from bridge.common.scenario import ScenarioConfig, load_scenario


_FORBIDDEN_KEYS = {
    "bridge",
    "gnb_n3_ip",
    "upf_n3_ip",
    "n3_network_cidr",
    "n3_network_name",
    "gnb_n3_ifname",
    "upf_n3_ifname",
}
_FORBIDDEN_VALUE_TOKENS = ("10.210.", "n3g", "n3u")
_DEFAULT_SPLIT_NR_CENTRAL_FREQUENCY_HZ = 3.5e9
_DEFAULT_SPLIT_NR_BANDWIDTH_HZ = 100e6
_DEFAULT_TDD_PATTERN_UL_FRIENDLY = "DL|UL|UL|F|DL|UL|UL|F|"


def _resolve_path(value: str | Path, base_dir: Path | None = None) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    return path.resolve()


def _coerce_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    raise ValueError(f"expected boolean-like value, got: {value!r}")


def _find_forbidden(payload: object, path: str = "") -> list[str]:
    hits: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            current = f"{path}.{key}" if path else str(key)
            if str(key) in _FORBIDDEN_KEYS:
                hits.append(current)
            hits.extend(_find_forbidden(value, current))
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            hits.extend(_find_forbidden(item, f"{path}[{index}]"))
    elif isinstance(payload, str):
        for token in _FORBIDDEN_VALUE_TOKENS:
            if token in payload:
                hits.append(path or "<root>")
                break
    return hits


@dataclass(slots=True, frozen=True)
class SplitNs3Config:
    output_subdir: str = "ns3-split"
    scratch_name: str = "nr_multignb_multiupf_split"
    policy_reload_ms: int = 100
    activation_poll_ms: int = 200
    sim_time_ms: int | None = None
    nr_numerology: int = 1
    nr_bandwidth_hz: float = _DEFAULT_SPLIT_NR_BANDWIDTH_HZ
    nr_central_frequency_hz: float = _DEFAULT_SPLIT_NR_CENTRAL_FREQUENCY_HZ


@dataclass(slots=True, frozen=True)
class SplitRuntimeConfig:
    startup_timeout_seconds: int = 180
    state_poll_ms: int = 200


@dataclass(slots=True, frozen=True)
class SplitRadioConfig:
    scheduler_type: str = "pf"
    tdd_pattern: str = "ul_friendly"
    gnb_tx_power_dbm: float = 43.0
    ue_tx_power_dbm: float = 23.0
    enable_uplink_power_control: bool = True
    gnb_noise_figure_db: float = 5.0
    ue_noise_figure_db: float = 7.0

    def resolved_tdd_pattern(self) -> str:
        pattern = self.tdd_pattern.strip()
        if pattern == "ul_friendly":
            return _DEFAULT_TDD_PATTERN_UL_FRIENDLY
        if not pattern:
            raise ValueError("split-mode radio.tdd_pattern must be non-empty")
        return pattern


@dataclass(slots=True, frozen=True)
class SplitModeConfig:
    name: str
    scenario_id: str
    base_scenario_path: Path
    base_scenario: ScenarioConfig
    ns3: SplitNs3Config
    runtime: SplitRuntimeConfig
    radio: SplitRadioConfig

    @property
    def control_plane_scenario(self) -> ScenarioConfig:
        base_ns3 = self.base_scenario.ns3
        updated_ns3 = replace(
            base_ns3,
            output_subdir=self.ns3.output_subdir,
            scratch_name=self.ns3.scratch_name,
            bridge_mode="split_cp_up",
            policy_reload_ms=self.ns3.policy_reload_ms,
            sim_time_ms=self.ns3.sim_time_ms if self.ns3.sim_time_ms is not None else base_ns3.sim_time_ms,
        )
        return replace(
            self.base_scenario,
            name=self.name,
            scenario_id=self.scenario_id,
            ns3=updated_ns3,
            bridge=replace(self.base_scenario.bridge, enable_inline_harness=False),
        )

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        *,
        base_dir: Path | None = None,
    ) -> "SplitModeConfig":
        forbidden_hits = _find_forbidden(payload)
        if forbidden_hits:
            joined = ", ".join(sorted(set(forbidden_hits)))
            raise ValueError(f"split-mode scenario must not define synthetic N3 fields: {joined}")

        name = str(payload.get("name") or payload.get("scenario_id") or "split-mode")
        scenario_id = str(payload.get("scenario_id") or name)
        base_scenario_value = payload.get("base_scenario")
        if not isinstance(base_scenario_value, str) or not base_scenario_value.strip():
            raise ValueError("split-mode scenario requires non-empty base_scenario")
        base_scenario_path = _resolve_path(base_scenario_value, base_dir)
        base_scenario = load_scenario(base_scenario_path)

        ns3_payload = dict(payload.get("ns3") or {})
        runtime_payload = dict(payload.get("runtime") or {})
        radio_payload = dict(payload.get("radio") or {})
        ns3 = SplitNs3Config(
            output_subdir=str(ns3_payload.get("output_subdir") or SplitNs3Config.output_subdir),
            scratch_name=str(ns3_payload.get("scratch_name") or SplitNs3Config.scratch_name),
            policy_reload_ms=int(ns3_payload.get("policy_reload_ms") or SplitNs3Config.policy_reload_ms),
            activation_poll_ms=int(ns3_payload.get("activation_poll_ms") or SplitNs3Config.activation_poll_ms),
            sim_time_ms=(
                int(ns3_payload["sim_time_ms"])
                if ns3_payload.get("sim_time_ms") is not None
                else None
            ),
            nr_numerology=int(ns3_payload.get("nr_numerology") or SplitNs3Config.nr_numerology),
            nr_bandwidth_hz=float(ns3_payload.get("nr_bandwidth_hz") or SplitNs3Config.nr_bandwidth_hz),
            nr_central_frequency_hz=float(
                ns3_payload.get("nr_central_frequency_hz") or _DEFAULT_SPLIT_NR_CENTRAL_FREQUENCY_HZ
            ),
        )
        runtime = SplitRuntimeConfig(
            startup_timeout_seconds=int(
                runtime_payload.get("startup_timeout_seconds") or SplitRuntimeConfig.startup_timeout_seconds
            ),
            state_poll_ms=int(runtime_payload.get("state_poll_ms") or SplitRuntimeConfig.state_poll_ms),
        )
        radio = SplitRadioConfig(
            scheduler_type=str(radio_payload.get("scheduler_type") or SplitRadioConfig.scheduler_type),
            tdd_pattern=str(radio_payload.get("tdd_pattern") or SplitRadioConfig.tdd_pattern),
            gnb_tx_power_dbm=float(radio_payload.get("gnb_tx_power_dbm") or SplitRadioConfig.gnb_tx_power_dbm),
            ue_tx_power_dbm=float(radio_payload.get("ue_tx_power_dbm") or SplitRadioConfig.ue_tx_power_dbm),
            enable_uplink_power_control=_coerce_bool(
                radio_payload.get("enable_uplink_power_control"),
                default=SplitRadioConfig.enable_uplink_power_control,
            ),
            gnb_noise_figure_db=float(
                radio_payload.get("gnb_noise_figure_db") or SplitRadioConfig.gnb_noise_figure_db
            ),
            ue_noise_figure_db=float(radio_payload.get("ue_noise_figure_db") or SplitRadioConfig.ue_noise_figure_db),
        )
        radio.resolved_tdd_pattern()
        return cls(
            name=name,
            scenario_id=scenario_id,
            base_scenario_path=base_scenario_path,
            base_scenario=base_scenario,
            ns3=ns3,
            runtime=runtime,
            radio=radio,
        )


def load_split_mode_config(path: str | Path) -> SplitModeConfig:
    resolved = Path(path).expanduser().resolve()
    with resolved.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("split-mode YAML root must be a mapping")
    return SplitModeConfig.from_dict(payload, base_dir=resolved.parent)
