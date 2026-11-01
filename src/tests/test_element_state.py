"""Change-only element state (asset_element_state) and its per-asset row.

Pure diff tests plus the custom handler against a fake pool. The fake pool
keeps a dict standing in for the table so restart cases can reload from it.
"""
from __future__ import annotations

import copy
import json
import logging
import socket
import urllib.request

import pytest
from prometheus_client import REGISTRY

import metrics
from config import Mapping
from edge_assignment import FallbackAssignment, configure as configure_edge
from handlers import asset_element_telemetry as h
from main import build_prune_targets
from persistence.postgres import PARENT_OWNED_PRUNE, PostgresPool

CFG_JSON = json.dumps({
    "deadbands": {
        "UNIT": {"temp_c": 5.0, "load_pct": 10.0},
        "ARRAY": {"temp_c": 5.0, "load_pct": 10.0},
        "TILE": {"temp_c": 4.0, "load_pct": 10.0},
        "CHIP": {"temp_c": 2.0, "load_pct": 10.0},
    },
    "bands": {"health": {"limits": [0.3, 0.7], "names": ["red", "amber", "green"],
                         "hysteresis": 0.05}},
    "heartbeat_s": 60,
})

# depth 0..3 chain plus a second chip under the same tile.
CHAIN = [("U", "UNIT"), ("U/A", "ARRAY"), ("U/A/T", "TILE"),
         ("U/A/T/C1", "CHIP"), ("U/A/T/C2", "CHIP")]


class FakePool:
    def __init__(self):
        self.db: dict[tuple[str, str], dict] = {}
        self.calls: list[tuple] = []  # (asset_write, rows)
        self.fetches: list[str] = []
        # the asset label record: per-asset override, else `default_labels`
        # (None = no record, {} = unlabelled record)
        self.labels: dict[str, dict | None] = {}
        self.default_labels: dict | None = {"originator_nation": "ATL",
                                            "releasable_to": ["BDR"]}
        self.restamps: list[dict | None] = []

    async def fetch_asset_labels(self, asset_id):
        return self.labels.get(asset_id, self.default_labels)

    async def fetch_element_state(self, asset_id):
        self.fetches.append(asset_id)
        return [copy.deepcopy(r) for (a, _), r in self.db.items() if a == asset_id]

    async def write_element_state(self, asset_write, rows, *, restamp_labels=None):
        self.restamps.append(restamp_labels)
        self.calls.append((asset_write, copy.deepcopy(rows)))
        if restamp_labels is not None:
            for r in self.db.values():
                r.update(restamp_labels)
        for r in rows:
            self.db[(r["asset_id"], r["element_path"])] = copy.deepcopy(r)

    @property
    def element_rows(self):
        return [r for _, rows in self.calls for r in rows]


def snapshot(asset="A1", *, temp=50.0, health=0.9, tx=False, operational=None,
             t_ns=1_000_000_000, layers=CHAIN, per_path=None):
    els = []
    for path, layer in layers:
        el = {"element_id": path, "layer_name": layer, "health": health,
              "temp_c": temp, "load_pct": 40.0, "tx_active": tx, "rx_active": False}
        el.update((per_path or {}).get(path, {}))
        els.append(el)
    return {"asset_id": asset, "platform_variant": "v", "profile_name": "p",
            "observed_at_ns": t_ns, "operational": operational or {"degraded": False},
            "originator_nation": "ATL", "releasable_to": ["BDR"], "elements": els}


@pytest.fixture(autouse=True)
def _setup(monkeypatch):
    configure_edge(strategy=lambda ctx: None,
                   fallback=FallbackAssignment("edge-01", "region-01"))
    h.configure(CFG_JSON)
    h.reset_state()
    clock = {"t": 1000.0}
    monkeypatch.setattr(h, "_clock", lambda: clock["t"])
    yield clock
    h._config = None
    h.reset_state()


async def run(pool, snap):
    await h.handle(snap["asset_id"], snap, pool)


def paths(rows):
    return [r["element_path"] for r in rows]


# -- stationary / first snapshot / restart -----------------------------------

async def test_first_snapshot_writes_every_element_once():
    pool = FakePool()
    await run(pool, snapshot())
    assert sorted(paths(pool.element_rows)) == sorted(p for p, _ in CHAIN)
    asset_write, rows = pool.calls[0]
    assert asset_write is not None and "elements" not in asset_write.row
    r = {x["element_path"]: x for x in rows}["U/A/T/C1"]
    assert (r["depth"], r["parent_path"], r["layer_name"]) == (3, "U/A/T", "CHIP")
    assert r["value_changes"] == 0
    assert r["originator_nation"] == "ATL" and r["releasable_to"] == ["BDR"]
    assert pool.fetches == ["A1"]


async def test_stationary_jitter_inside_deadband_writes_nothing(_setup):
    pool = FakePool()
    await run(pool, snapshot(temp=50.0))
    n = len(pool.calls)
    for i, temp in enumerate([50.5, 49.2, 51.0, 49.1, 50.9]):
        _setup["t"] += 1
        await run(pool, snapshot(temp=temp, t_ns=2_000_000_000 + i))
    assert len(pool.calls) == n  # 0 element rows, 0 parent rows, no asset row


async def test_restart_with_rows_present_writes_nothing():
    pool = FakePool()
    await run(pool, snapshot())
    h.reset_state()  # a restart: memory gone, table stays
    pool.calls.clear()
    await run(pool, snapshot(temp=50.4))
    assert pool.fetches == ["A1", "A1"]
    assert pool.element_rows == []
    # the per-asset row has no memory across a restart: one write, no elements
    assert len(pool.calls) == 1 and pool.calls[0][1] == []
    await run(pool, snapshot(temp=50.4))
    assert len(pool.calls) == 1


# -- crossing and subtree counts ---------------------------------------------

async def test_chip_crossing_writes_chip_and_three_ancestors_then_zero_once():
    pool = FakePool()
    await run(pool, snapshot(temp=50.0))
    pool.calls.clear()
    await run(pool, snapshot(temp=50.0, per_path={"U/A/T/C1": {"temp_c": 53.0}}))
    rows = {r["element_path"]: r for r in pool.element_rows}
    assert sorted(rows) == ["U", "U/A", "U/A/T", "U/A/T/C1"]
    assert rows["U/A/T/C1"]["value_changes"] == 1
    assert rows["U/A/T/C1"]["temp_c"] == 53.0
    for anc in ("U", "U/A", "U/A/T"):
        assert rows[anc]["subtree_changes_window"] == 1
        assert rows[anc]["subtree_changes"] == 1
        assert rows[anc]["temp_c"] == 50.0  # stored value kept, not re-sampled
    pool.calls.clear()
    same = snapshot(temp=50.0, per_path={"U/A/T/C1": {"temp_c": 53.0}})
    await run(pool, same)
    rows = {r["element_path"]: r for r in pool.element_rows}
    assert sorted(rows) == ["U", "U/A", "U/A/T"]
    assert all(r["subtree_changes_window"] == 0 and r["subtree_changes"] == 1
               for r in rows.values())
    pool.calls.clear()
    await run(pool, same)
    assert pool.calls == []


async def test_dither_around_last_written_value_never_writes():
    pool = FakePool()
    await run(pool, snapshot(temp=50.0, per_path={"U/A/T/C1": {"temp_c": 53.0}}))
    pool.calls.clear()
    for t in (54.9, 51.1, 54.0, 51.5):  # all within +-2 of the stored 53.0
        await run(pool, snapshot(temp=50.0, per_path={"U/A/T/C1": {"temp_c": t}}))
    assert pool.calls == []


# -- health band, transitions --------------------------------------------------

async def test_health_band_rise_and_hysteresis_dither():
    pool = FakePool()
    leaf = [("X", "CHIP")]
    await run(pool, snapshot(health=0.5, layers=leaf))
    assert pool.element_rows[0]["health_band"] == "amber"
    pool.calls.clear()
    await run(pool, snapshot(health=0.69, layers=leaf))   # inside amber
    assert pool.calls == []
    await run(pool, snapshot(health=0.71, layers=leaf))   # rise > 0.7
    assert [r["health_band"] for r in pool.element_rows] == ["green"]
    assert pool.element_rows[0]["health"] == 0.71
    pool.calls.clear()
    for hv in (0.68, 0.66, 0.72, 0.651):                  # above 0.7 - 0.05
        await run(pool, snapshot(health=hv, layers=leaf))
    assert pool.calls == []
    await run(pool, snapshot(health=0.64, layers=leaf))   # fall < 0.65
    assert [r["health_band"] for r in pool.element_rows] == ["amber"]


async def test_tx_active_flip_writes_one_transition_row():
    pool = FakePool()
    await run(pool, snapshot(layers=[("X", "CHIP")], tx=False))
    pool.calls.clear()
    before = metrics.ELEMENT_CHANGES_EMITTED.labels(**{"class": "transition"})._value.get()
    await run(pool, snapshot(layers=[("X", "CHIP")], tx=True))
    assert paths(pool.element_rows) == ["X"]
    assert pool.element_rows[0]["tx_active"] is True
    assert pool.element_rows[0]["value_changes"] == 0
    after = metrics.ELEMENT_CHANGES_EMITTED.labels(**{"class": "transition"})._value.get()
    assert after == before + 1


# -- per-asset row ---------------------------------------------------------------

async def test_asset_row_waits_for_heartbeat_then_heartbeats(_setup):
    pool = FakePool()
    await run(pool, snapshot())
    assert pool.calls[0][0] is not None
    pool.calls.clear()
    _setup["t"] += 59
    await run(pool, snapshot(t_ns=5_000_000_000))
    assert pool.calls == []
    _setup["t"] += 2
    await run(pool, snapshot(t_ns=6_000_000_000))
    assert len(pool.calls) == 1
    asset_write, rows = pool.calls[0]
    assert rows == [] and asset_write.row["observed_at"].timestamp() == 6.0
    assert "updated_at" in asset_write.row and "elements" not in asset_write.row
    pool.calls.clear()
    await run(pool, snapshot(t_ns=7_000_000_000))
    assert pool.calls == []


async def test_asset_row_written_when_operational_changes():
    pool = FakePool()
    await run(pool, snapshot())
    pool.calls.clear()
    await run(pool, snapshot(operational={"degraded": True}))
    assert len(pool.calls) == 1 and pool.calls[0][1] == []
    assert pool.calls[0][0].row["operational"] == {"degraded": True}
    assert pool.calls[0][0].row["edge_id"] == "edge-01"


def test_element_provenance_keys_are_edge_and_region():
    # what resolve_origin_or_derive yields, carried verbatim into jsonb
    pool = FakePool()
    import asyncio
    asyncio.run(run(pool, snapshot(layers=[("X", "CHIP")])))
    assert pool.element_rows[0]["provenance"] == {"edge_id": "edge-01",
                                                  "region_id": "region-01"}


def _sample(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _refused(reason):
    return _sample("openddil_element_writes_refused_total", reason=reason)


async def test_labels_come_from_the_record_not_the_envelope():
    pool = FakePool()
    pool.labels["A1"] = {"originator_nation": "QRS", "releasable_to": ["TUV"]}
    snap = snapshot(layers=[("X", "CHIP")])  # envelope says ATL / BDR
    snap["provenance"] = {"originator_nation": "WXY", "releasable_to": []}
    await run(pool, snap)
    asset_write, rows = pool.calls[0]
    assert asset_write.row["originator_nation"] == "QRS"
    assert asset_write.row["releasable_to"] == ["TUV"]
    assert (rows[0]["originator_nation"], rows[0]["releasable_to"]) == ("QRS", ["TUV"])


async def test_no_label_record_refuses_without_advancing_then_retries_in_full():
    pool = FakePool()
    pool.default_labels = None
    before = _refused("no_label_record")
    await run(pool, snapshot())
    assert pool.calls == []
    assert _refused("no_label_record") == before + 1
    st = h._assets["A1"]
    assert st.elements == {} and st.asset_sig is None and st.asset_written_at == 0.0
    pool.default_labels = {"originator_nation": "ATL", "releasable_to": ["BDR"]}
    await run(pool, snapshot())
    asset_write, rows = pool.calls[0]
    assert asset_write is not None
    assert sorted(paths(rows)) == sorted(p for p, _ in CHAIN)


async def test_unlabelled_record_refuses_without_advancing():
    pool = FakePool()
    pool.default_labels = {}
    before = _refused("label_record_unlabelled")
    await run(pool, snapshot())
    assert pool.calls == []
    assert _refused("label_record_unlabelled") == before + 1
    st = h._assets["A1"]
    assert st.elements == {} and st.asset_sig is None
    pool.default_labels = {"originator_nation": "ATL", "releasable_to": []}
    await run(pool, snapshot())
    assert sorted(paths(pool.element_rows)) == sorted(p for p, _ in CHAIN)


async def test_nothing_to_write_without_a_record_counts_no_refusal():
    pool = FakePool()
    await run(pool, snapshot())
    pool.default_labels = None
    pool.calls.clear()
    before = _refused("no_label_record")
    await run(pool, snapshot(t_ns=2_000_000_000))
    assert pool.calls == [] and _refused("no_label_record") == before


async def test_label_change_restamps_once_and_rewrites_the_asset_row():
    pool = FakePool()
    await run(pool, snapshot())
    assert pool.restamps == [None]
    pool.default_labels = {"originator_nation": "QRS", "releasable_to": []}
    before = _sample("openddil_element_label_restamps_total")
    await run(pool, snapshot(temp=60.0, t_ns=2_000_000_000))
    asset_write, rows = pool.calls[-1]
    assert pool.restamps[-1] == {"originator_nation": "QRS", "releasable_to": []}
    assert asset_write is not None and asset_write.row["originator_nation"] == "QRS"
    assert _sample("openddil_element_label_restamps_total") == before + 1
    assert all(r["originator_nation"] == "QRS" for r in pool.db.values())
    await run(pool, snapshot(temp=70.0, t_ns=3_000_000_000))
    assert pool.restamps[-1] is None  # already stamped with the new labels


async def test_first_envelope_after_restart_does_not_restamp():
    pool = FakePool()
    await run(pool, snapshot())
    h.reset_state()
    pool.default_labels = {"originator_nation": "QRS", "releasable_to": []}
    pool.calls.clear()
    pool.restamps.clear()
    before = _sample("openddil_element_label_restamps_total")
    await run(pool, snapshot())
    assert len(pool.calls) == 1  # the per-asset row, carrying the new labels
    assert pool.restamps == [None]
    assert _sample("openddil_element_label_restamps_total") == before


def test_restamp_sql_is_label_only_and_skips_matching_rows():
    sql = PostgresPool.build_element_restamp_sql()
    assert sql == (
        'UPDATE "asset_element_state" SET "originator_nation" = $2, '
        '"releasable_to" = $3 WHERE "asset_id" = $1 AND '
        '("originator_nation", "releasable_to") IS DISTINCT FROM ($2, $3)')
    assert "updated_at" not in sql


# -- undeclared layer --------------------------------------------------------------

async def test_undeclared_layer_skipped_logged_once_counted(caplog):
    pool = FakePool()
    layers = [("X", "CHIP"), ("Y", "MYSTERY")]
    c = metrics.ELEMENT_UNDECLARED_KIND.labels(layer_name="MYSTERY")
    before = c._value.get()
    with caplog.at_level(logging.WARNING, logger="projector.element_state"):
        await run(pool, snapshot(layers=layers))
        await run(pool, snapshot(layers=layers, temp=90.0))
    assert paths(pool.element_rows) == ["X", "X"]
    assert sum("MYSTERY" in r.getMessage() for r in caplog.records) == 1
    assert c._value.get() == before + 2


# -- config validation -----------------------------------------------------------------

def _doc(**over):
    d = json.loads(CFG_JSON)
    d.update(over)
    return json.dumps(d)


@pytest.mark.parametrize("raw", [
    None, "", "not json", "[]", "{}",
    _doc(heartbeat_s=0), _doc(heartbeat_s="60"), _doc(heartbeat_s=True),
    _doc(deadbands={}),
    _doc(deadbands={"CHIP": {"temp_c": 1}}),
    _doc(deadbands={"CHIP": {"temp_c": -1, "load_pct": 1}}),
    _doc(deadbands={"CHIP": {"temp_c": "1", "load_pct": 1}}),
    _doc(bands={}),
    _doc(bands={"health": {"limits": [0.7, 0.3], "names": ["a", "b", "c"], "hysteresis": 0.1}}),
    _doc(bands={"health": {"limits": [0.3], "names": ["a"], "hysteresis": 0.1}}),
    _doc(bands={"health": {"limits": [0.3], "names": ["a", "a"], "hysteresis": 0.1}}),
    _doc(bands={"health": {"limits": [0.3], "names": ["a", "b"]}}),
])
def test_config_rejections(raw):
    with pytest.raises(h.ElementStateConfigError):
        h.parse_config(raw)


def test_config_accepts_declared_shape():
    cfg = h.parse_config(CFG_JSON)
    assert cfg.heartbeat_s == 60 and cfg.names == ("red", "amber", "green")


async def test_unconfigured_handler_refuses():
    h._config = None
    with pytest.raises(h.ElementStateConfigError):
        await h.handle("A1", snapshot(), FakePool())


# -- prune -------------------------------------------------------------------------------

def _mapping(table, mode, ttl=24):
    return Mapping(topic=f"{table}-t", handler="x", table=table, consumer_group="g",
                   decode_as="json", mode=mode, asset_ttl_hours=ttl)


def test_prune_keeps_telemetry_target_and_adds_element_target_after_it():
    ms = [_mapping("telemetry_latest_state", "upsert"),
          _mapping("asset_element_telemetry", "custom"),
          _mapping("asset_cm_state", "upsert"),
          _mapping("effector_launch", "custom", ttl=None)]
    targets = build_prune_targets(ms)
    names = [t[0] for t in targets]
    assert names.index("asset_element_telemetry") + 1 == names.index("asset_element_state")
    assert names[-1] == "telemetry_latest_state"
    assert "effector_launch" not in names
    tel = next(t for t in targets if t[0] == "asset_element_telemetry")
    assert tel == ("asset_element_telemetry", "updated_at", 24, True)
    el = next(t for t in targets if t[0] == "asset_element_state")
    assert el[1] == ""  # not aged on the element row's own column


def test_element_state_prune_sql_has_no_age_predicate():
    sql = PostgresPool.build_prune_sql("asset_element_state", "", asset_id_keyed=True)
    assert "updated_at" not in sql and "interval" not in sql and "$1" not in sql
    assert "NOT EXISTS" in sql and '"asset_element_telemetry"' in sql
    assert PARENT_OWNED_PRUNE["asset_element_state"] == "asset_element_telemetry"


def test_stationary_asset_with_live_heartbeat_keeps_element_rows():
    """The heartbeat keeps the per-asset row young, so its own prune (age on
    updated_at) does not fire; the element prune keys only on that row's
    existence, so quiet element rows survive past the TTL."""
    el_sql = PostgresPool.build_prune_sql("asset_element_state", "", asset_id_keyed=True)
    tel_sql = PostgresPool.build_prune_sql("asset_element_telemetry", "updated_at",
                                           asset_id_keyed=True)
    assert 'now() - ($1 || \' hours\')' in tel_sql  # per-asset row aged on updated_at
    assert "updated_at" not in el_sql                # element rows never are
    targets = build_prune_targets([_mapping("asset_element_telemetry", "custom")])
    assert [t[0] for t in targets] == ["asset_element_telemetry", "asset_element_state"]


def test_env_ttl_override_reaches_custom_element_mapping(tmp_path, monkeypatch):
    from config import load_config
    p = tmp_path / "c.yaml"
    p.write_text(
        "mappings:\n"
        "  - {topic: a, handler: asset_element_telemetry, table: asset_element_telemetry,"
        " consumer_group: g, decode_as: json, mode: custom, asset_ttl_hours: 24}\n"
        "  - {topic: b, handler: effector_launch, table: effector_launch,"
        " consumer_group: g2, decode_as: json, mode: custom}\n")
    monkeypatch.setenv("PROJECTOR_ASSET_TTL_HOURS", "0.5")
    cfg = load_config(p)
    by = {m.table: m for m in cfg.mappings}
    assert by["asset_element_telemetry"].asset_ttl_hours == 0.5
    assert by["effector_launch"].asset_ttl_hours is None


def test_upsert_sql_shape():
    sql = PostgresPool.build_element_state_upsert_sql()
    assert sql.startswith('INSERT INTO "asset_element_state"')
    assert 'ON CONFLICT ("asset_id", "element_path") DO UPDATE' in sql
    assert "$20::jsonb" in sql


# -- metric as exported -----------------------------------------------------------------------

async def test_metric_names_and_labels_as_exported(monkeypatch):
    pool = FakePool()
    await run(pool, snapshot(layers=[("X", "CHIP"), ("Z", "NOPE")]))
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    monkeypatch.setenv("METRICS_PORT", str(port))
    assert metrics.start_metrics_server() == port
    text = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    assert "# TYPE openddil_element_changes_emitted_total counter" in text
    assert 'openddil_element_changes_emitted_total{class="value"}' in text
    assert 'openddil_element_changes_emitted_total{class="transition"}' in text
    assert "# TYPE openddil_element_undeclared_kind_total counter" in text
    assert 'openddil_element_undeclared_kind_total{layer_name="NOPE"}' in text
    assert "# TYPE openddil_element_writes_refused_total counter" in text
    assert 'openddil_element_writes_refused_total{reason="no_label_record"}' in text
    assert 'openddil_element_writes_refused_total{reason="label_record_unlabelled"}' in text
    assert "# TYPE openddil_element_label_restamps_total counter" in text
    line = next(ln for ln in text.splitlines()
                if ln.startswith("openddil_element_changes_emitted_total{"))
    assert "tier" not in line


def test_element_state_prune_sql_returns_distinct_asset_ids():
    sql = PostgresPool.build_prune_sql("asset_element_state", "", asset_id_keyed=True)
    assert sql.startswith('WITH d AS (DELETE FROM "asset_element_state" AS s')
    assert 'RETURNING s."asset_id"' in sql
    assert sql.endswith('SELECT DISTINCT "asset_id" FROM d')


def test_age_predicate_prune_sql_unchanged():
    assert PostgresPool.build_prune_sql("asset_element_telemetry", "updated_at",
                                        asset_id_keyed=False) == (
        'DELETE FROM "asset_element_telemetry" WHERE '
        '"updated_at" < now() - ($1 || \' hours\')::interval')
    assert "RETURNING" not in PostgresPool.build_prune_sql(
        "asset_element_telemetry", "updated_at", asset_id_keyed=True)


async def test_evicted_asset_next_snapshot_writes_full_tree_and_asset_row():
    pool = FakePool()
    await run(pool, snapshot())
    pool.db.clear()  # the prune deleted the rows
    pool.calls.clear()
    assert h.evict_assets(["A1", "never-seen"]) == 1
    await run(pool, snapshot())
    assert len(pool.calls) == 1
    asset_write, rows = pool.calls[0]
    assert asset_write is not None
    assert sorted(paths(rows)) == sorted(p for p, _ in CHAIN)


async def test_unevicted_asset_same_snapshot_writes_nothing():
    pool = FakePool()
    await run(pool, snapshot())
    pool.calls.clear()
    await run(pool, snapshot())
    assert pool.calls == []
