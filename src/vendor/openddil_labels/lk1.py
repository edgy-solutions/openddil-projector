"""The lk1 label key: canonical form, key, body, verification, golden vectors.

Implements DESIGN-2026-11-06-adr0055-compatibility-slice section 1.2 (the
canonical form and the key; the lk1 prefix is the form's version) and
ADR-0055 section 2 (declared-empty and absent differ; no originator is
unlabelled): SPEC-labels.md, lk1.py.

Nothing calls this module yet.
"""

import hashlib
import json
import pathlib
import re
import sys

POLICY_RE = r"[a-z0-9][a-z0-9:._-]{0,62}"
NATION_RE = r"[A-Z]{3}"

_COLLECTIONS = (list, tuple, set, frozenset)


def canonical_lk1(policy_id, originator_nation, releasable_to):
    """Return the canonical string, or None when there is no key."""
    if policy_id is None or policy_id == "":
        return None
    if not isinstance(policy_id, str) or not re.fullmatch(POLICY_RE, policy_id):
        raise ValueError("invalid policy_id: %r" % (policy_id,))
    if originator_nation is None or originator_nation == "":
        return None
    if not isinstance(originator_nation, str) or not re.fullmatch(NATION_RE, originator_nation):
        return None
    if releasable_to is None:
        tail = "~"
    else:
        if not isinstance(releasable_to, _COLLECTIONS):
            return None
        for x in releasable_to:
            if not isinstance(x, str) or not re.fullmatch(NATION_RE, x):
                return None
        tail = ",".join(sorted(set(releasable_to)))
    return "lk1|" + policy_id + "|nation|" + originator_nation + "|" + tail


def compile_lk1(policy_id, originator_nation, releasable_to):
    """Return (label_key, body), or None when there is no key."""
    canonical = canonical_lk1(policy_id, originator_nation, releasable_to)
    if canonical is None:
        return None
    key = "lk1:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    release = None if releasable_to is None else sorted(set(releasable_to))
    body = {
        "policy": policy_id,
        "v": 1,
        "dims": {"nation": {"author": originator_nation, "release": release}},
    }
    return key, body


def verify(label_key, body):
    """True only when body is the canonical lk1 body for label_key. Never raises."""
    try:
        if not isinstance(body, dict) or set(body.keys()) != {"policy", "v", "dims"}:
            return False
        v = body["v"]
        if type(v) is not int or v != 1:
            return False
        dims = body["dims"]
        if not isinstance(dims, dict) or set(dims.keys()) != {"nation"}:
            return False
        nation = dims["nation"]
        if not isinstance(nation, dict) or set(nation.keys()) != {"author", "release"}:
            return False
        got = compile_lk1(body["policy"], nation["author"], nation["release"])
        return got is not None and got == (label_key, body)
    except Exception:
        return False


def generate_vectors():
    """Golden vectors for lk1, as a list of dicts."""
    p1, p2 = "example:policy-1", "example:policy-2"
    cases = [
        ("atl_absent", p1, "ATL", None),
        ("atl_empty", p1, "ATL", []),
        ("atl_bdr", p1, "ATL", ["BDR"]),
        ("atl_bdr_atl", p1, "ATL", ["BDR", "ATL"]),
        ("atl_atl_bdr_atl", p1, "ATL", ["ATL", "BDR", "ATL"]),
        ("atl_bdr_crd", p1, "ATL", ["BDR", "CRD"]),
        ("atl_crd_bdr", p1, "ATL", ["CRD", "BDR"]),
        ("bdr_atl", p1, "BDR", ["ATL"]),
        ("atl_absent_policy_2", p2, "ATL", None),
        ("no_originator_absent", p1, None, None),
        ("no_originator_empty", p1, None, []),
        ("empty_originator_bdr", p1, "", ["BDR"]),
        ("lowercase_originator", p1, "atl", None),
        ("long_originator", p1, "ATLX", None),
        ("release_has_none", p1, "ATL", [None]),
        ("release_lowercase", p1, "ATL", ["bdr"]),
        ("release_empty_string", p1, "ATL", [""]),
        ("no_policy", None, "ATL", ["BDR"]),
    ]
    out = []
    for name, policy, orig, rel in cases:
        canonical = canonical_lk1(policy, orig, rel)
        compiled = compile_lk1(policy, orig, rel)
        out.append(
            {
                "name": name,
                "policy_id": policy,
                "originator_nation": orig,
                "releasable_to": rel,
                "canonical": canonical,
                "label_key": None if compiled is None else compiled[0],
                "label": None if compiled is None else compiled[1],
            }
        )
    return out


def _vectors_path():
    return pathlib.Path(__file__).resolve().parent / "vectors" / "lk1.json"


def _write_vectors():
    path = _vectors_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(generate_vectors(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


if __name__ == "__main__":
    if sys.argv[1:] == ["--write"]:
        _write_vectors()
    else:
        sys.stderr.write("usage: python -m openddil_labels.lk1 --write\n")
        sys.exit(2)
