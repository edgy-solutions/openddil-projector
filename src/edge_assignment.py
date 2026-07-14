"""Edge assignment for customer assets that don't carry edge_id.

Customer wire shapes (Unit telemetry, weapons capability) carry no edge_id —
edges are an OpenDDIL-side construct, not customer reality. The projector
decides the presentation edge_id for each customer asset using a pluggable
strategy named in `projector_config.yaml` under `edge_assignment.strategy`.

Built-in strategies:

  nearest_fob     — Haversine to a configured list of FOB coordinates.
                    Best for assets that carry a position.
  asset_id_prefix — longest-prefix match against asset_id (good for
                    positionless assets like strike-only launchers).
  static          — explicit asset_id -> (edge, region) map.
  chain           — try a list of strategies in order; first non-None wins.

External strategies can be plugged in via the `register_strategy` decorator
without editing this file — `@register_strategy("my_thing")` on a
`(cfg) -> Strategy` builder makes it usable as `strategy: my_thing` in the
config.

Provenance: edge_ids derived here are stamped ORIGIN_DERIVED in the
projector's logs; the `derivation_basis` (method, fob_id, distance, …) is
logged so the assignment is auditable. Schema-level audit (a new column)
would require a migration and is deferred.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional

log = logging.getLogger("edge_assignment")


# ----- types ---------------------------------------------------------------

@dataclass(frozen=True)
class Fob:
    """A Forward Operating Base — an OpenDDIL edge's geographic anchor."""
    edge_id: str
    region_id: str
    lat: float
    lon: float
    label: str = ""


@dataclass(frozen=True)
class AssetContext:
    """Inputs an assignment strategy may use to decide an edge."""
    asset_id: str
    lat: Optional[float] = None
    lon: Optional[float] = None


@dataclass(frozen=True)
class EdgeAssignment:
    edge_id: str
    region_id: str
    # JSON-safe. The strategy's basis: method, fob_id, distance, prefix, etc.
    derivation_basis: dict = field(default_factory=dict)


# A strategy is `(AssetContext) -> EdgeAssignment | None`. `None` means
# "I can't decide for this asset" — the chain (or the fallback) takes over.
Strategy = Callable[[AssetContext], Optional[EdgeAssignment]]


# ----- distance helper -----------------------------------------------------

EARTH_RADIUS_KM = 6371.0088


def great_circle_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in km. ~0.5% error vs Vincenty — fine for demo."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return EARTH_RADIUS_KM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ----- strategy builders ---------------------------------------------------

def nearest_fob_strategy(fobs: list[Fob]) -> Strategy:
    """Pick the FOB whose great-circle distance to (lat, lon) is smallest."""
    def s(ctx: AssetContext) -> Optional[EdgeAssignment]:
        if ctx.lat is None or ctx.lon is None or not fobs:
            return None
        nearest, dist = min(
            ((f, great_circle_km(ctx.lat, ctx.lon, f.lat, f.lon)) for f in fobs),
            key=lambda x: x[1],
        )
        return EdgeAssignment(
            edge_id=nearest.edge_id,
            region_id=nearest.region_id,
            derivation_basis={
                "method": "nearest_fob",
                "fob_edge_id": nearest.edge_id,
                "fob_label": nearest.label,
                "distance_km": round(dist, 2),
            },
        )
    return s


def asset_id_prefix_strategy(
    prefix_map: Mapping[str, tuple[str, str]],
) -> Strategy:
    """Pick (edge, region) for the longest matching asset_id prefix."""
    # Sort once at build time; longest prefix wins, so e.g. "AAA_BBB_"
    # beats "AAA_".
    sorted_prefixes = sorted(prefix_map.items(), key=lambda x: -len(x[0]))

    def s(ctx: AssetContext) -> Optional[EdgeAssignment]:
        for prefix, (edge_id, region_id) in sorted_prefixes:
            if ctx.asset_id.startswith(prefix):
                return EdgeAssignment(
                    edge_id=edge_id,
                    region_id=region_id,
                    derivation_basis={"method": "asset_id_prefix", "prefix": prefix},
                )
        return None
    return s


def static_strategy(
    static_map: Mapping[str, tuple[str, str]],
) -> Strategy:
    """Pick (edge, region) by exact asset_id match."""
    def s(ctx: AssetContext) -> Optional[EdgeAssignment]:
        entry = static_map.get(ctx.asset_id)
        if entry is None:
            return None
        edge_id, region_id = entry
        return EdgeAssignment(
            edge_id=edge_id,
            region_id=region_id,
            derivation_basis={"method": "static_map", "asset_id": ctx.asset_id},
        )
    return s


def chained_strategy(*strategies: Strategy) -> Strategy:
    """Try strategies in order; first non-None wins."""
    def s(ctx: AssetContext) -> Optional[EdgeAssignment]:
        for strat in strategies:
            r = strat(ctx)
            if r is not None:
                return r
        return None
    return s


# ----- pluggable config-driven builder ------------------------------------

_BUILDERS: dict[str, Callable[[dict], Strategy]] = {}


def register_strategy(name: str) -> Callable[[Callable[[dict], Strategy]],
                                              Callable[[dict], Strategy]]:
    """Register a config-keyed strategy builder. External code can add new
    strategies (`@register_strategy("my_thing")`) without editing this file.
    """
    def deco(builder: Callable[[dict], Strategy]) -> Callable[[dict], Strategy]:
        _BUILDERS[name] = builder
        return builder
    return deco


@register_strategy("nearest_fob")
def _build_nearest_fob(cfg: dict) -> Strategy:
    fobs = [Fob(**f) for f in (cfg.get("fobs") or [])]
    return nearest_fob_strategy(fobs)


@register_strategy("asset_id_prefix")
def _build_asset_id_prefix(cfg: dict) -> Strategy:
    raw = cfg.get("asset_id_prefix_map") or {}
    mapping = {p: (v["edge_id"], v["region_id"]) for p, v in raw.items()}
    return asset_id_prefix_strategy(mapping)


@register_strategy("static")
def _build_static(cfg: dict) -> Strategy:
    raw = cfg.get("static_map") or {}
    mapping = {a: (v["edge_id"], v["region_id"]) for a, v in raw.items()}
    return static_strategy(mapping)


@register_strategy("chain")
def _build_chain(cfg: dict) -> Strategy:
    members = [build_strategy_from_config(sub) for sub in (cfg.get("chain") or [])]
    return chained_strategy(*members)


def build_strategy_from_config(cfg: dict) -> Strategy:
    """Build a strategy from a config dict with a `strategy:` key."""
    name = (cfg or {}).get("strategy")
    if not name:
        raise ValueError("edge_assignment config missing `strategy:`")
    builder = _BUILDERS.get(name)
    if builder is None:
        raise ValueError(
            f"unknown edge_assignment strategy: {name!r} "
            f"(registered: {sorted(_BUILDERS)})"
        )
    return builder(cfg)


# ----- module-singleton resolver (configured at startup) ------------------

@dataclass(frozen=True)
class FallbackAssignment:
    edge_id: str
    region_id: str


_strategy: Optional[Strategy] = None
_fallback: Optional[FallbackAssignment] = None


def configure(strategy: Strategy, fallback: FallbackAssignment) -> None:
    """Install the active strategy + last-resort fallback. Called once at
    startup from main.py after loading projector_config.yaml."""
    global _strategy, _fallback
    _strategy = strategy
    _fallback = fallback


def configure_from_config(cfg: dict) -> None:
    """Convenience: build + install from the `edge_assignment` config block."""
    if not cfg:
        # No edge_assignment configured — install a no-op strategy with a
        # plain fallback so handlers always get a usable resolver.
        configure(
            strategy=lambda ctx: None,
            fallback=FallbackAssignment("edge-unspecified", "region-unspecified"),
        )
        return
    strategy = build_strategy_from_config(cfg)
    fb = cfg.get("fallback") or {}
    fallback = FallbackAssignment(
        edge_id=fb.get("edge_id", "edge-unspecified"),
        region_id=fb.get("region_id", "region-unspecified"),
    )
    configure(strategy=strategy, fallback=fallback)


def resolve_for(
    asset_id: str,
    lat: Optional[float],
    lon: Optional[float],
    handler_label: str,
) -> EdgeAssignment:
    """Run the configured strategy; fall back if it returns None. Logs the
    chosen derivation basis at DEBUG so the assignment is auditable."""
    if _strategy is None or _fallback is None:
        raise RuntimeError(
            "edge_assignment.configure() not called — "
            "main.py must wire this from projector_config.yaml at startup",
        )
    ctx = AssetContext(asset_id=asset_id, lat=lat, lon=lon)
    result = _strategy(ctx)
    if result is None:
        result = EdgeAssignment(
            edge_id=_fallback.edge_id,
            region_id=_fallback.region_id,
            derivation_basis={"method": "fallback"},
        )
    log.debug(
        "%s: %s -> %s/%s via %s",
        handler_label, asset_id,
        result.edge_id, result.region_id,
        result.derivation_basis.get("method"),
    )
    return result


# ----- helpers used by handlers --------------------------------------------

def extract_wgs84(kinematics: Any) -> tuple[Optional[float], Optional[float]]:
    """Best-effort lat/lon extraction from an EntityTelemetryEvent's
    `kinematics` JSONB shape. Tolerates camelCase and snake_case keys and
    missing nested blocks. Returns (None, None) if anything is missing."""
    if not isinstance(kinematics, dict):
        return (None, None)
    pos = kinematics.get("position") or {}
    wgs84 = pos.get("wgs84") or pos  # tolerate either nesting
    lat_raw = wgs84.get("latitude") if isinstance(wgs84, dict) else None
    lon_raw = wgs84.get("longitude") if isinstance(wgs84, dict) else None
    if lat_raw is None and isinstance(wgs84, dict):
        lat_raw = wgs84.get("lat")
    if lon_raw is None and isinstance(wgs84, dict):
        lon_raw = wgs84.get("lon")
    # Unwrap {unit, value} objects emitted by sensor-ingest's unit-aware
    # encoder (e.g. {"lat": {"unit":"deg","value":52.957}}). Legacy/inline
    # shapes still send raw floats. Tolerate both.
    if isinstance(lat_raw, dict):
        lat_raw = lat_raw.get("value")
    if isinstance(lon_raw, dict):
        lon_raw = lon_raw.get("value")
    try:
        lat = float(lat_raw) if lat_raw is not None else None
        lon = float(lon_raw) if lon_raw is not None else None
        return (lat, lon)
    except (TypeError, ValueError):
        return (None, None)
