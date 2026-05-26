"""Config loading for the projector.

`projector_config.yaml` is the source of truth for the topic->table mapping.
This module loads it into typed objects and is re-invoked on SIGHUP.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = "/app/src/config/projector_config.yaml"


@dataclass(frozen=True)
class Mapping:
    topic: str
    handler: str
    table: str
    consumer_group: str
    decode_as: str
    mode: str  # "upsert" | "append"
    retention_hours: int | None = None


@dataclass
class Settings:
    rate_limit_per_sec: int = 10
    postgres_retry_base_seconds: float = 0.5
    postgres_retry_max_seconds: float = 30.0


@dataclass
class Config:
    mappings: list[Mapping] = field(default_factory=list)
    settings: Settings = field(default_factory=Settings)
    # Raw `edge_assignment` block from the YAML, consumed by
    # src/edge_assignment.py's `configure_from_config`. Kept as a dict here so
    # registering a new strategy doesn't require touching this typed Config.
    edge_assignment: dict[str, Any] = field(default_factory=dict)

    def mapping_by_topic(self) -> dict[str, Mapping]:
        return {m.topic: m for m in self.mappings}


def _config_path() -> Path:
    return Path(os.getenv("PROJECTOR_CONFIG", DEFAULT_CONFIG_PATH))


def load_config(path: Path | None = None) -> Config:
    """Parse projector_config.yaml into a Config. Raises on a malformed file
    (a bad config at startup IS fatal — unlike a bad Kafka message)."""
    cfg_path = path or _config_path()
    raw: dict[str, Any] = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))

    mappings: list[Mapping] = []
    for entry in raw.get("mappings", []):
        mappings.append(
            Mapping(
                topic=entry["topic"],
                handler=entry["handler"],
                table=entry["table"],
                consumer_group=entry["consumer_group"],
                decode_as=entry["decode_as"],
                mode=entry.get("mode", "upsert"),
                retention_hours=entry.get("retention_hours"),
            )
        )

    s = raw.get("settings", {}) or {}
    settings = Settings(
        rate_limit_per_sec=int(
            os.getenv("RATE_LIMIT_PER_SEC", s.get("rate_limit_per_sec", 10))
        ),
        postgres_retry_base_seconds=float(
            s.get("postgres_retry_base_seconds", 0.5)
        ),
        postgres_retry_max_seconds=float(
            s.get("postgres_retry_max_seconds", 30.0)
        ),
    )
    edge_assignment = raw.get("edge_assignment") or {}

    # Overlay-supplied edge assignment. A deployment overlay can point this
    # at a separate file (typically a ConfigMap mount) so customer-specific
    # FOB lists and asset_id mappings live OUTSIDE the OSS config. When the
    # env var is set and the file exists, its contents REPLACE the main
    # config's edge_assignment block. Both shapes are accepted: a top-level
    # `edge_assignment:` wrapper, or the block's contents at the top level.
    override_path = os.getenv("EDGE_ASSIGNMENT_CONFIG")
    if override_path:
        override_file = Path(override_path)
        if override_file.is_file():
            override_raw = (
                yaml.safe_load(override_file.read_text(encoding="utf-8")) or {}
            )
            edge_assignment = override_raw.get("edge_assignment", override_raw)

    return Config(mappings=mappings, settings=settings, edge_assignment=edge_assignment)
