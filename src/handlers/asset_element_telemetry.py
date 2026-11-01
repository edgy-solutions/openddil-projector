"""Handler: asset-element-telemetry -> asset_element_telemetry + asset_element_state.

mode "custom": the handler owns its Postgres I/O (called as
`await handle(key, decoded, pool)`), because one envelope fans out to a
per-asset row AND a batch of per-element rows that must land in ONE
transaction.

Decodes the logistics-sim per-asset envelope:
  {
    "asset_id": "<asset_id>", "platform_variant": "...", "profile_name": "...",
    "observed_at_ns": <int>,
    "operational": {...},
    "elements": [
      {"element_id": "<path>", "layer_depth": ..., "layer_name": ...,
       "health": ..., "temp_c": ..., "load_pct": ...,
       "tx_active": ..., "rx_active": ..., "moved_by": ...},
      ...
    ]
  }

Change-only (Option D, DESIGN-D-element-state.md 3.1, 3.2, 3.4, 3.6.4, 3.6.5):

* The per-asset row (asset_element_telemetry, no elements array) is written
  when `operational` / platform_variant / profile_name / labels differ from
  the last written, or when `heartbeat_s` has passed since the last write.
  The heartbeat keeps updated_at fresh, which is what the prune keys on.
* Element rows (asset_element_state) are written only when a value-class
  field (temp_c, load_pct) crosses its declared deadband against the last
  WRITTEN value, or a transition-class field (tx_active, rx_active, moved_by,
  health_band) changes. Health is numeric but only its band is delivered
  (Schmitt trigger with declared hysteresis).
* Ancestors of a written element get subtree_changes / subtree_changes_window
  counts; a stationary asset writes no parent rows.
* Last-written state is held in memory per asset and loaded from the table on
  the first envelope for the asset after start.

Labels (ADR-0029 section 3): the element feed carries none, so the asset's
labels are read from the tier's asset label record (telemetry_latest_state,
the asset's ingress stamp) once per envelope and stamped on both row types.
A write for an asset whose record is missing or unlabelled is refused with a
declared reason; nothing is written and no in-memory state advances, so the
next envelope retries the same diff. When the record's labels change, the
write that follows also restamps the asset's existing element rows in the
same transaction.

Config is `ELEMENT_STATE_JSON`, validated at start; there are no defaults.
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from metrics import (
    ELEMENT_CACHE_EVICTED,
    ELEMENT_CHANGES_EMITTED,
    ELEMENT_LABEL_RESTAMPS,
    ELEMENT_UNDECLARED_KIND,
    ELEMENT_WRITES_REFUSED,
)
from persistence import Write

from .base import now_utc, refuse_row, resolve_origin_or_derive

log = logging.getLogger("projector.element_state")

TABLE = "asset_element_telemetry"
STATE_TABLE = "asset_element_state"
HANDLER_LABEL = "asset_element_telemetry"

VALUE_FIELDS = ("temp_c", "load_pct")
TRANSITION_FIELDS = ("tx_active", "rx_active", "moved_by")
ENV_VAR = "ELEMENT_STATE_JSON"


class ElementStateConfigError(ValueError):
    """ELEMENT_STATE_JSON is missing or invalid."""


@dataclass(frozen=True)
class ElementStateConfig:
    deadbands: dict[str, dict[str, float]]
    limits: tuple[float, ...]
    names: tuple[str, ...]
    hysteresis: float
    heartbeat_s: float


def _num(v: Any, what: str, *, positive: bool = False) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise ElementStateConfigError(f"{ENV_VAR}: {what} must be a finite number")
    if v < 0 or (positive and v == 0):
        raise ElementStateConfigError(
            f"{ENV_VAR}: {what} must be {'> 0' if positive else '>= 0'}")
    return float(v)


def parse_config(raw: str | None) -> ElementStateConfig:
    """Validate ELEMENT_STATE_JSON. Raises ElementStateConfigError; no defaults."""
    if raw is None or not raw.strip():
        raise ElementStateConfigError(f"{ENV_VAR} is not set")
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ElementStateConfigError(f"{ENV_VAR} is not valid JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise ElementStateConfigError(f"{ENV_VAR} must be a JSON object")
    for k in ("deadbands", "bands", "heartbeat_s"):
        if k not in doc:
            raise ElementStateConfigError(f"{ENV_VAR}: missing '{k}'")

    db = doc["deadbands"]
    if not isinstance(db, dict) or not db:
        raise ElementStateConfigError(f"{ENV_VAR}: 'deadbands' must be a non-empty object")
    deadbands: dict[str, dict[str, float]] = {}
    for layer, d in db.items():
        if not isinstance(d, dict) or set(d) != set(VALUE_FIELDS):
            raise ElementStateConfigError(
                f"{ENV_VAR}: deadbands['{layer}'] must have exactly {list(VALUE_FIELDS)}")
        deadbands[layer] = {f: _num(d[f], f"deadbands['{layer}'].{f}") for f in VALUE_FIELDS}

    bands = doc["bands"]
    if not isinstance(bands, dict) or not isinstance(bands.get("health"), dict):
        raise ElementStateConfigError(f"{ENV_VAR}: 'bands.health' is required")
    h = bands["health"]
    for k in ("limits", "names", "hysteresis"):
        if k not in h:
            raise ElementStateConfigError(f"{ENV_VAR}: bands.health missing '{k}'")
    if not isinstance(h["limits"], list) or not h["limits"]:
        raise ElementStateConfigError(f"{ENV_VAR}: bands.health.limits must be a non-empty list")
    limits = []
    for x in h["limits"]:
        if isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x):
            raise ElementStateConfigError(f"{ENV_VAR}: bands.health.limits must be finite numbers")
        limits.append(float(x))
    if any(b <= a for a, b in zip(limits, limits[1:])):
        raise ElementStateConfigError(f"{ENV_VAR}: bands.health.limits must be strictly ascending")
    names = h["names"]
    if (not isinstance(names, list) or len(names) != len(limits) + 1
            or not all(isinstance(n, str) and n for n in names)
            or len(set(names)) != len(names)):
        raise ElementStateConfigError(
            f"{ENV_VAR}: bands.health.names must be {len(limits) + 1} distinct "
            "non-empty strings (lowest health first)")
    hyst = _num(h["hysteresis"], "bands.health.hysteresis")
    hb = _num(doc["heartbeat_s"], "heartbeat_s", positive=True)
    return ElementStateConfig(deadbands, tuple(limits), tuple(names), hyst, hb)


_config: ElementStateConfig | None = None


def configure(raw: str | None) -> ElementStateConfig:
    """Validate and install the config (called at start; raises on invalid)."""
    global _config
    _config = parse_config(raw)
    return _config


# -- pure diff ---------------------------------------------------------------

def band_index(cfg: ElementStateConfig, health: float, prev: int | None) -> int:
    """Schmitt trigger. First observation (prev None): plain thresholds.
    Rise when health > limit; fall only when health < limit - hysteresis."""
    if prev is None:
        return sum(1 for lim in cfg.limits if health > lim)
    i = prev
    while i < len(cfg.limits) and health > cfg.limits[i]:
        i += 1
    while i > 0 and health < cfg.limits[i - 1] - cfg.hysteresis:
        i -= 1
    return i


def value_crossed(new: Any, old: Any, deadband: float) -> bool:
    if new is None and old is None:
        return False
    if new is None or old is None:
        return True
    diff = abs(new - old)
    return diff > 0 if deadband == 0 else diff >= deadband


def _ancestors(path: str) -> list[str]:
    parts = path.split("/")
    return ["/".join(parts[:i]) for i in range(len(parts) - 1, 0, -1)]


def _num_or_none(v: Any) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def diff_snapshot(
    cfg: ElementStateConfig,
    last: dict[str, dict[str, Any]],
    elements: list[Any],
    observed_at: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, set[str]], dict[str, int]]:
    """Compare a snapshot to the last-written element rows.

    Returns (rows, classes, undeclared):
      rows       path -> full row dict to write (no asset/label/provenance keys)
      classes    path -> {"value","transition"} that caused an element write
                 (count-only ancestor rows are absent: no field caused them)
      undeclared layer_name -> number of elements skipped
    `last` is not modified.
    """
    rows: dict[str, dict[str, Any]] = {}
    classes: dict[str, set[str]] = {}
    undeclared: dict[str, int] = {}
    window: dict[str, int] = {}

    for el in elements:
        if not isinstance(el, dict):
            continue
        path = el.get("element_id")
        if not isinstance(path, str) or not path:
            continue
        layer = el.get("layer_name")
        db = cfg.deadbands.get(layer) if isinstance(layer, str) else None
        if db is None:
            undeclared[str(layer)] = undeclared.get(str(layer), 0) + 1
            continue
        old = last.get(path)
        health = _num_or_none(el.get("health"))
        cur = {
            "temp_c": _num_or_none(el.get("temp_c")),
            "load_pct": _num_or_none(el.get("load_pct")),
            "tx_active": el.get("tx_active") if isinstance(el.get("tx_active"), bool) else None,
            "rx_active": el.get("rx_active") if isinstance(el.get("rx_active"), bool) else None,
            "moved_by": str(el["moved_by"]) if el.get("moved_by") is not None else None,
        }
        old_band = old.get("health_band") if old else None
        if health is None:
            band = old_band
        else:
            prev_idx = cfg.names.index(old_band) if old_band in cfg.names else None
            band = cfg.names[band_index(cfg, health, prev_idx)]
        cur["health_band"] = band

        cause: set[str] = set()
        if old is None:
            # First observation: classes by what the element carries.
            if cur["temp_c"] is not None or cur["load_pct"] is not None:
                cause.add("value")
            if any(cur[f] is not None for f in (*TRANSITION_FIELDS, "health_band")):
                cause.add("transition")
            if not cause:
                cause.add("transition")
        else:
            for f in VALUE_FIELDS:
                if value_crossed(cur[f], old.get(f), db[f]):
                    cause.add("value")
            for f in (*TRANSITION_FIELDS, "health_band"):
                if cur[f] != old.get(f):
                    cause.add("transition")
        if not cause:
            continue

        depth = path.count("/")
        rows[path] = {
            "element_path": path,
            "parent_path": path.rsplit("/", 1)[0] if depth else None,
            "depth": depth,
            "layer_name": layer,
            "health": health,
            **cur,
            "value_changes": (old["value_changes"] if old else 0)
                             + (1 if old and "value" in cause else 0),
            "subtree_changes": old["subtree_changes"] if old else 0,
            "subtree_changes_window": 0,
            "observed_at": observed_at,
        }
        classes[path] = cause
        if old is not None:  # a first observation is a baseline, not a change
            for a in _ancestors(path):
                window[a] = window.get(a, 0) + 1

    # Ancestor counts.
    for a, n in window.items():
        if a in rows:
            row = rows[a]
        elif a in last:
            row = dict(last[a])
            row["observed_at"] = observed_at
            rows[a] = row
        else:
            continue  # ancestor is not an element we hold
        row["subtree_changes"] = row.get("subtree_changes", 0) + n
        row["subtree_changes_window"] = n
    # Return to zero, once: a previously non-zero window, none this snapshot.
    for path, old in last.items():
        if old.get("subtree_changes_window", 0) and path not in window:
            row = rows.get(path)
            if row is None:
                row = dict(old)
                row["observed_at"] = observed_at
                rows[path] = row
            row["subtree_changes_window"] = 0
    return rows, classes, undeclared


# -- handler -----------------------------------------------------------------

@dataclass
class _AssetState:
    elements: dict[str, dict[str, Any]]
    asset_sig: tuple | None = None
    asset_written_at: float = 0.0
    stamped_labels: dict[str, Any] | None = None


_assets: dict[str, _AssetState] = {}
_logged_undeclared: set[str] = set()


def _clock() -> float:
    return time.monotonic()


def reset_state() -> None:
    """Forget all in-memory last-written state (tests)."""
    _assets.clear()
    _logged_undeclared.clear()


def evict_assets(asset_ids: Any) -> int:
    """Drop the in-memory last-written state (elements, heartbeat signature)
    for assets whose rows were pruned, so their next snapshot is a first
    observation and writes the full tree plus the per-asset row. Returns the
    number of cached assets evicted."""
    n = 0
    for aid in asset_ids:
        if _assets.pop(aid, None) is not None:
            n += 1
    if n:
        ELEMENT_CACHE_EVICTED.inc(n)
        log.info("evicted %d asset(s) from the element cache after prune", n)
    return n


def _observed_at_from_ns(observed_at_ns: Any) -> datetime:
    try:
        return datetime.fromtimestamp(int(observed_at_ns) / 1_000_000_000, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return now_utc()


async def handle(key: str, decoded: dict[str, Any], pool: Any) -> None:
    cfg = _config
    if cfg is None:
        raise ElementStateConfigError(f"{ENV_VAR} not configured")
    asset_id = decoded.get("asset_id") or key
    if not asset_id:
        return
    elements = decoded.get("elements") or []
    if not isinstance(elements, list):
        return

    st = _assets.get(asset_id)
    if st is None:
        loaded = await pool.fetch_element_state(asset_id)
        st = _AssetState(elements={r["element_path"]: dict(r) for r in loaded})
        _assets[asset_id] = st

    observed_at = _observed_at_from_ns(decoded.get("observed_at_ns"))
    prov = resolve_origin_or_derive(None, asset_id, HANDLER_LABEL)
    record_labels = await pool.fetch_asset_labels(asset_id)
    labels = record_labels or {}
    if labels and st.stamped_labels is None:
        st.stamped_labels = dict(labels)  # first envelope after start: no restamp

    rows, classes, undeclared = diff_snapshot(cfg, st.elements, elements, observed_at)
    for layer, n in undeclared.items():
        ELEMENT_UNDECLARED_KIND.labels(layer_name=layer).inc(n)
        if layer not in _logged_undeclared:
            _logged_undeclared.add(layer)
            log.warning("element layer %r has no declared deadband; its elements "
                        "are skipped (logged once per layer)", layer)

    operational = decoded.get("operational") or {}
    sig = (str(decoded.get("platform_variant") or ""), str(decoded.get("profile_name") or ""),
           json.dumps(operational, sort_keys=True, default=str),
           json.dumps(labels or st.stamped_labels or {}, sort_keys=True, default=str))
    now = _clock()
    asset_write: Write | None = None
    if st.asset_sig != sig or now - st.asset_written_at >= cfg.heartbeat_s:
        asset_write = Write(
            table=TABLE, mode="upsert", key_columns=["asset_id"],
            row={
                "asset_id": asset_id,
                "platform_variant": sig[0],
                "profile_name": sig[1],
                "operational": operational,
                "observed_at": observed_at,
                "updated_at": now_utc(),
                **prov,
                **labels,
            },
            jsonb_columns={"operational"},
        )
    if asset_write is None and not rows:
        return

    if not labels:
        reason = "no_label_record" if record_labels is None else "label_record_unlabelled"
        refuse_row(HANDLER_LABEL, reason, f"asset_id={asset_id}")
        ELEMENT_WRITES_REFUSED.labels(reason=reason).inc()
        return
    restamp = st.stamped_labels is not None and labels != st.stamped_labels

    stamp = now_utc()
    full_rows = [
        {"asset_id": asset_id, **r, "updated_at": stamp,
         "originator_nation": labels.get("originator_nation"),
         "releasable_to": labels.get("releasable_to"),
         "provenance": dict(prov)}
        for r in rows.values()
    ]
    await pool.write_element_state(asset_write, full_rows,
                                   restamp_labels=dict(labels) if restamp else None)

    # Durable: only now advance the in-memory last-written state.
    st.elements.update(rows)
    st.stamped_labels = dict(labels)
    if restamp:
        ELEMENT_LABEL_RESTAMPS.inc()
    if asset_write is not None:
        st.asset_sig = sig
        st.asset_written_at = now
    for cls_set in classes.values():
        for c in cls_set:
            ELEMENT_CHANGES_EMITTED.labels(**{"class": c}).inc()
