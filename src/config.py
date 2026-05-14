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
    return Config(mappings=mappings, settings=settings)
