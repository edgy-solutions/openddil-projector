"""Unit tests for handlers — pure field mapping, no live infra.

Each handler takes (kafka_key, decoded_dict) and returns a Write. The
decoded dicts below mirror what MessageToDict(preserving_proto_field_name=
True) produces from each topic's proto (or the CloudEvents JSON envelope).
"""
from __future__ import annotations

from datetime import datetime

from handlers import get_handler
from handlers.base import (
    duration_to_seconds,
    parse_ns_timestamp,
    parse_timestamp,
)


# -- base helpers -------------------------------------------------------------

def test_parse_timestamp_rfc3339_z():
    dt = parse_timestamp("2026-05-14T03:00:00Z")
    assert isinstance(dt, datetime)
    assert dt.year == 2026 and dt.tzinfo is not None


def test_parse_timestamp_handles_missing():
    assert parse_timestamp(None) is None
    assert parse_timestamp("") is None
    assert parse_timestamp("not-a-date") is None


def test_parse_ns_timestamp():
    # 1778738710276935936 ns -> 2026-05-14T...
    dt = parse_ns_timestamp(1778738710276935936)
    assert isinstance(dt, datetime)
    assert dt.year == 2026 and dt.tzinfo is not None
    # 0 / None / negative are 'unset'
    assert parse_ns_timestamp(0) is None
    assert parse_ns_timestamp(None) is None
    assert parse_ns_timestamp("not-a-number") is None


def test_duration_to_seconds():
    assert duration_to_seconds("120s") == 120
    assert duration_to_seconds("3.5s") == 3
    assert duration_to_seconds("90") == 90
    assert duration_to_seconds(None) is None
    assert duration_to_seconds("") is None


# -- cm_state -----------------------------------------------------------------
# cm-service emits asset-cm-state as JSON with integer enum values and *_ns
# integer timestamps (not protobuf). These fixtures mirror that real shape.

def test_cm_state_maps_json_shape_with_integer_enums():
    decoded = {
        "asset_id": "dis:1:1:4773",
        "baseline_id": "M1A2-SEPv3-Baseline-2024.2",
        "lifecycle": 2,                 # LIFECYCLE_ACTIVE
        "overall_status": 4,            # CONFIG_STATUS_NOT_MISSION_CAPABLE
        "last_alerted_status": 4,
        "as_of_ns": 1778738710276935936,
        "last_observed_at_ns": 1778738710276935936,
        "discrepancies": [{"discrepancy_id": "D1", "type": 5, "severity": 3}],
        "manual_discrepancies": [],
        "mod_status": [{"mod_id": "MWO-2024-117", "state": 2}],
        "installed": [{"slot_id": "engine", "ci_id": ""}],
    }
    write = get_handler("cm_state")("dis:1:1:4773", decoded)
    assert write is not None
    assert write.table == "asset_cm_state"
    assert write.mode == "upsert"
    assert write.key_columns == ["asset_id"]
    # integer enums mapped back to proto names
    assert write.row["lifecycle"] == "LIFECYCLE_ACTIVE"
    assert write.row["overall_status"] == "CONFIG_STATUS_NOT_MISSION_CAPABLE"
    assert write.row["last_alerted_status"] == "CONFIG_STATUS_NOT_MISSION_CAPABLE"
    # *_ns integer timestamps parsed to datetime
    assert isinstance(write.row["as_of"], datetime)
    assert isinstance(write.row["last_observed_at"], datetime)
    # both discrepancy lists preserved
    assert write.row["discrepancies"] == [
        {"discrepancy_id": "D1", "type": 5, "severity": 3}
    ]
    assert write.row["manual_discrepancies"] == []
    assert {"installed", "mod_status", "discrepancies",
            "manual_discrepancies"} <= write.jsonb_columns


def test_cm_state_already_string_enum_passes_through():
    # Defensive: if a producer ever emits string enums, don't double-map.
    decoded = {"asset_id": "A1", "lifecycle": "LIFECYCLE_STALE",
               "overall_status": "CONFIG_STATUS_IN_COMPLIANCE"}
    write = get_handler("cm_state")("A1", decoded)
    assert write is not None
    assert write.row["lifecycle"] == "LIFECYCLE_STALE"
    assert write.row["overall_status"] == "CONFIG_STATUS_IN_COMPLIANCE"


def test_cm_state_falls_back_to_kafka_key_for_asset_id():
    write = get_handler("cm_state")("KEY-ASSET", {"lifecycle": 2})
    assert write is not None
    assert write.row["asset_id"] == "KEY-ASSET"


def test_cm_state_skips_when_no_asset_id_anywhere():
    assert get_handler("cm_state")("", {"lifecycle": 2}) is None


# -- logistics_status ---------------------------------------------------------

def test_logistics_status_unwraps_envelope():
    decoded = {
        "status": {
            "asset_id": "USA-ARMY-1HBCT-M1A2-4773",
            "platform_variant": "M1A2-SEPv3",
            "overall_severity": "LOGISTICS_SEVERITY_CRITICAL",
            "constraining_factors": [{"factor_id": "fuel"}],
            "projected_mission_capable_remaining": "3600s",
            "status_revision": 4,
            "computed_at": "2026-05-14T03:00:00Z",
        },
        "previous_severity": "LOGISTICS_SEVERITY_DEGRADED",
        "is_transition": True,
        "is_initial": False,
    }
    write = get_handler("logistics_status")("USA-ARMY-1HBCT-M1A2-4773", decoded)
    assert write is not None
    assert write.table == "asset_logistics_status"
    assert write.row["overall_severity"] == "LOGISTICS_SEVERITY_CRITICAL"
    assert write.row["previous_severity"] == "LOGISTICS_SEVERITY_DEGRADED"
    assert write.row["is_transition"] is True
    assert write.row["projected_mission_capable_remaining_seconds"] == 3600
    assert write.row["status_revision"] == 4
    assert write.row["constraining_factors"] == [{"factor_id": "fuel"}]


def test_logistics_status_skips_when_no_asset_id():
    assert get_handler("logistics_status")("", {"status": {}}) is None


# -- telemetry_latest ---------------------------------------------------------

def test_telemetry_latest_flattens_identity_keeps_blobs():
    decoded = {
        "asset": {
            "asset_id": "USA-ARMY-1HBCT-M1A2-4773",
            "callsign": "IRON-6",
            "platform_variant": "M1A2-SEPv3",
            "force": "FORCE_FRIENDLY",
        },
        "kinematics": {"position": {"ecef": {"x": {"value": 1.0, "unit": "m"}}}},
        "sustainment": {"thermal": {"component_temperature":
                                    {"value": 92.0, "unit": "Cel"}}},
        "provenance": {"producer_id": "vrforces-01",
                       "source_protocol": "DIS/IEEE-1278.1",
                       "sample_time": "2026-05-14T03:00:00Z"},
        "schema_revision": 1,
    }
    write = get_handler("telemetry_latest")("USA-ARMY-1HBCT-M1A2-4773", decoded)
    assert write is not None
    assert write.table == "telemetry_latest_state"
    assert write.row["callsign"] == "IRON-6"
    assert write.row["platform_variant"] == "M1A2-SEPv3"
    assert write.row["force_id"] == "FORCE_FRIENDLY"
    # nested blocks kept verbatim as jsonb
    assert write.row["kinematics"]["position"]["ecef"]["x"]["unit"] == "m"
    assert isinstance(write.row["last_sample_at"], datetime)
    assert {"kinematics", "sustainment", "provenance"} <= write.jsonb_columns
    # Phase 5: operational_state absent in this DIS-shape decoded message —
    # all five op_state columns should be None so postgres stores NULLs.
    assert write.row["power_state"] is None
    assert write.row["functional_mode"] is None
    assert write.row["health_state"] is None
    assert write.row["actively_receiving"] is None
    assert write.row["actively_transmitting"] is None


def test_telemetry_latest_extracts_operational_state():
    """Phase 5: when EntityTelemetryEvent carries operational_state (the
    customer1 sensor branch is the first producer; future DIS / AFSim /
    VRForces adapters too), the 3 enum axes + 2 activity booleans land
    in their own columns for direct SQL filtering and SPA rendering."""
    decoded = {
        "asset": {
            "asset_id": "prop:BEL_Antwerp_Ghent_MRAD2_radar_MRAD_Sensor",
            "platform_variant": "MRAD_Sensor",
            "force": "FORCE_FRIENDLY",
        },
        "kinematics": {"position": {"wgs84": {
            "lat": {"value": 51.17, "unit": "deg"},
            "lon": {"value": 4.21, "unit": "deg"},
        }}},
        "operational_state": {
            "power_state":           "POWER_STATE_OPERATE",
            "functional_mode":       "FUNCTIONAL_MODE_ACTIVE",
            "health_state":          "HEALTH_STATE_DEGRADED",
            "actively_receiving":    True,
            "actively_transmitting": False,
        },
        "provenance": {"producer_id": "proprietary-amqp",
                       "source_protocol": "proprietary-v1",
                       "sample_time": "2026-05-29T00:00:00Z"},
        "schema_revision": 2,
    }
    write = get_handler("telemetry_latest")(
        "prop:BEL_Antwerp_Ghent_MRAD2_radar_MRAD_Sensor", decoded)
    assert write is not None
    assert write.row["power_state"] == "POWER_STATE_OPERATE"
    assert write.row["functional_mode"] == "FUNCTIONAL_MODE_ACTIVE"
    assert write.row["health_state"] == "HEALTH_STATE_DEGRADED"
    assert write.row["actively_receiving"] is True
    assert write.row["actively_transmitting"] is False
    # op_state columns are NOT in jsonb_columns — they're scalar text/bool.
    assert "power_state" not in write.jsonb_columns
    assert "actively_receiving" not in write.jsonb_columns


# -- tactical_events ----------------------------------------------------------

def test_tactical_events_maps_cloudevent_and_extracts_severity():
    # cm-service config alerts express status under `current_status`.
    envelope = {
        "id": "ce-42",
        "source": "/openddil/cm-service",
        "type": "openddil.configuration.discrepancy.detected",
        "subject": "dis:1:1:4773",
        "time": "2026-05-14T03:00:00Z",
        "data": {"current_status": "CONFIG_STATUS_NOT_MISSION_CAPABLE"},
    }
    write = get_handler("tactical_events")("dis:1:1:4773", envelope)
    assert write is not None
    assert write.table == "tactical_events"
    assert write.mode == "append"
    assert write.key_columns == ["id"]
    assert write.row["id"] == "ce-42"
    # severity pulled best-effort from data.current_status
    assert write.row["severity"] == "CONFIG_STATUS_NOT_MISSION_CAPABLE"
    assert write.jsonb_columns == {"data"}


def test_tactical_events_extracts_logistics_severity():
    # logistics alerts use `overall_severity` instead.
    envelope = {
        "id": "ce-99", "source": "/openddil/logistics-fusion",
        "type": "openddil.logistics.severity.transition",
        "subject": "dis:1:1:4773", "time": "2026-05-14T03:00:00Z",
        "data": {"overall_severity": "LOGISTICS_SEVERITY_CRITICAL"},
    }
    write = get_handler("tactical_events")("dis:1:1:4773", envelope)
    assert write is not None
    assert write.row["severity"] == "LOGISTICS_SEVERITY_CRITICAL"


def test_tactical_events_subject_falls_back_to_key():
    envelope = {"id": "ce-1", "source": "x", "type": "y",
                "time": "2026-05-14T03:00:00Z"}
    write = get_handler("tactical_events")("FALLBACK-ASSET", envelope)
    assert write is not None
    assert write.row["subject"] == "FALLBACK-ASSET"


# -- telemetry_windows --------------------------------------------------------

def test_telemetry_windows_renames_wear_trends_column():
    decoded = {
        "asset_id": "USA-ARMY-1HBCT-M1A2-4773",
        "platform_variant": "M1A2-SEPv3",
        "fluid_trends": {"fuel_remaining": {"slope": -1.2}},
        "consumable_trends": [{"slot": "main_gun"}],
        "wear_trends": [{"component": "transmission"}],
        "window": {"sample_count": 30, "duration": "900s"},
        "computed_at": "2026-05-14T03:00:00Z",
    }
    write = get_handler("telemetry_windows")("USA-ARMY-1HBCT-M1A2-4773", decoded)
    assert write is not None
    assert write.table == "asset_telemetry_windows"
    # proto field `wear_trends` -> column `component_wear_trends`
    assert write.row["component_wear_trends"] == [{"component": "transmission"}]
    assert write.row["fluid_trends"] == {"fuel_remaining": {"slope": -1.2}}
    assert write.row["window_duration_seconds"] == 900
    assert write.row["sample_count"] == 30


# -- origin-node provenance (ADR-0022) ----------------------------------------
# Every per-asset projection row carries edge_id / region_id. Single-tier
# today (constant defaults), but the handler output is shaped for the
# edge->regional->HQ hierarchy from the start. If a handler stops emitting
# these, a flat-topology assumption has hardened — that is what this guards.

def test_every_per_asset_handler_emits_origin_provenance():
    cases = [
        ("cm_state", "A1", {"asset_id": "A1", "lifecycle": 2}),
        ("logistics_status", "A1", {"status": {"asset_id": "A1"}}),
        ("telemetry_latest", "A1", {"asset": {"asset_id": "A1"}}),
        ("telemetry_windows", "A1", {"asset_id": "A1"}),
        ("tactical_events", "A1", {
            "id": "ce-1", "source": "x", "type": "y",
            "subject": "A1", "time": "2026-05-14T03:00:00Z",
        }),
    ]
    for handler_name, key, decoded in cases:
        write = get_handler(handler_name)(key, decoded)
        assert write is not None, handler_name
        assert write.row["edge_id"] == "edge-01", handler_name
        assert write.row["region_id"] == "region-01", handler_name
