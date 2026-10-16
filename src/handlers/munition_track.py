"""Handler: munition-tracks -> munition_track, the in-flight track table.

Mode "custom" like effector_launch: `handle(key, decoded, pool)` is async and
owns its Postgres I/O, because the write depends on the row's existing state
(newer vs older sample, already terminal) and on a join read of
effector_launch. See `PostgresPool.upsert_munition_track`.

A munition is tracked by its own DIS id, never as an asset (ADR-0049). The
launcher and labels are joined from the Fire row when it exists; a Fire that
arrives later back-fills them (effector_launch.py). That back-fill can lose a
commit race with the first sample (each side commits before the other can see
it), so every later track write re-joins while the launcher is NULL, and the
sweep heals any row left over (munition_track_sweep.py). Terminal states come from
the Detonation path in effector_launch.py, or from the silence sweep, which
runs on this tier's own arrival clock and is a track state only -- never a
launch outcome (ADR-0048 section 4 is untouched).

Wire shape: `munition_urn`, `munition_type` (kind/domain/country/category/
subcategory/specific/extra), `location_ecef` {x,y,z}, `linear_velocity_ecef`,
`force_id`, `marking`, `sample_time` (ISO 8601) and a nested
`provenance: {edge_id, region_id}`.
"""
from __future__ import annotations

import logging
from typing import Any

from metrics import (MUNITION_TRACK_JOINED, MUNITION_TRACK_REFUSED,
                     MUNITION_TRACK_UPSERTS)
from persistence import PostgresPool

from .base import (now_utc, parse_timestamp, refuse_row,
                    resolve_provenance_from_dict)
from .effector_launch import munition_type_key

log = logging.getLogger("projector.handlers.munition_track")

HANDLER_LABEL = "munition_track"


def _refuse(reason: str, detail: str) -> None:
    MUNITION_TRACK_REFUSED.labels(reason=reason).inc()
    refuse_row(HANDLER_LABEL, reason, detail)


def _ecef(loc: Any) -> tuple[float, float, float] | None:
    if not isinstance(loc, dict):
        return None
    try:
        xyz = (float(loc["x"]), float(loc["y"]), float(loc["z"]))
    except (KeyError, TypeError, ValueError):
        return None
    # NaN compares unequal to itself; an unlocatable sample is no sample.
    if any(v != v for v in xyz):
        return None
    return xyz


async def handle(key: str, decoded: dict[str, Any], pool: PostgresPool) -> None:
    urn = decoded.get("munition_urn")
    if not urn:
        _refuse("no_munition_urn", "munition-tracks record with no munition_urn")
        return
    xyz = _ecef(decoded.get("location_ecef"))
    if xyz is None:
        _refuse("no_location", f"track {urn} has no usable location_ecef")
        return
    sample_time = parse_timestamp(decoded.get("sample_time"))
    if sample_time is None:
        _refuse("bad_sample_time", f"track {urn} sample_time={decoded.get('sample_time')!r}")
        return

    labels = resolve_provenance_from_dict(
        decoded.get("provenance") or {}, urn, HANDLER_LABEL)
    outcome, joined = await pool.upsert_munition_track(
        munition_asset_id=urn,
        munition_type=munition_type_key(decoded.get("munition_type")),
        ecef=xyz,
        sample_time=sample_time,
        edge_id=labels["edge_id"],
        region_id=labels["region_id"],
        now=now_utc(),
    )
    MUNITION_TRACK_UPSERTS.labels(outcome=outcome).inc()
    if joined:
        MUNITION_TRACK_JOINED.labels(via="track").inc()
