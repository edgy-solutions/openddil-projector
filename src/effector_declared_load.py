"""Startup loader for the `effector_declared_load` table.

A launcher's declared load has no streaming producer -- there is no topic
that would tell this projector "an M1A1 carries 8 of this munition". It is
operator-supplied fixture/overlay data, loaded wholesale from a YAML file at
startup and replaced in one transaction, the same way `edge_assignment`'s
overlay file replaces that block at startup rather than being merged into it.

No file, or the env var unset -- the table is emptied and every
`effector_launcher_counts.remaining` then reads NULL (unknown declared load
is not the same claim as "zero rounds declared" -- see the view's own
comment in the migration).

A malformed entry -- a negative count, or a munition key not shaped
`int.int.int.int.int.int.int` -- refuses the WHOLE file at startup, naming
the offending entry. This is a config-load failure, same discipline as
`config.load_config`: a bad config is fatal at startup, not a per-row skip
discovered later as a silently-wrong `remaining`.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml

from persistence import PostgresPool

log = logging.getLogger("projector.effector_declared_load")

ENV_PATH = "EFFECTOR_DECLARED_LOAD_PATH"

# The wire/schema munition key: "k.d.c.cat.sub.spec.extra", seven dot-joined
# non-negative integers. Same shape handlers/effector_launch.py builds from
# the decoded DIS 7-tuple -- a key here that cannot match a key the handler
# writes would silently never resolve as "declared" for anything.
_MUNITION_KEY_RE = re.compile(r"^\d+\.\d+\.\d+\.\d+\.\d+\.\d+\.\d+$")

_KEY_KINDS = ("asset", "variant")


class DeclaredLoadConfigError(Exception):
    """A readable EFFECTOR_DECLARED_LOAD_PATH file failed to validate.
    Fatal at startup -- see module docstring."""


def _validate_and_flatten(raw: dict[str, Any]) -> list[tuple[str, str, str, int]]:
    """`{"asset": {...}, "variant": {...}}` -> [(load_key, key_kind,
    munition_type, declared), ...]. Raises `DeclaredLoadConfigError` naming
    the offending entry on the first problem found."""
    rows: list[tuple[str, str, str, int]] = []
    if not isinstance(raw, dict):
        raise DeclaredLoadConfigError(
            f"declared-load file must be a mapping at the top level, got "
            f"{type(raw).__name__}"
        )
    for key_kind in _KEY_KINDS:
        block = raw.get(key_kind)
        if block is None:
            continue
        if not isinstance(block, dict):
            raise DeclaredLoadConfigError(
                f"declared-load '{key_kind}' block must be a mapping, got "
                f"{type(block).__name__}"
            )
        for load_key, munitions in block.items():
            if not isinstance(munitions, dict):
                raise DeclaredLoadConfigError(
                    f"declared-load entry '{key_kind}.{load_key}' must map "
                    f"munition keys to counts, got {type(munitions).__name__}"
                )
            for munition_key, declared in munitions.items():
                entry_name = f"{key_kind}.{load_key}.{munition_key}"
                if not _MUNITION_KEY_RE.match(str(munition_key)):
                    raise DeclaredLoadConfigError(
                        f"declared-load entry '{entry_name}' has a malformed "
                        "munition key (want int.int.int.int.int.int.int)"
                    )
                if isinstance(declared, bool) or not isinstance(declared, int) or declared < 0:
                    raise DeclaredLoadConfigError(
                        f"declared-load entry '{entry_name}' has a negative "
                        f"or non-integer declared count: {declared!r}"
                    )
                rows.append((str(load_key), key_kind, str(munition_key), declared))
    return rows


async def load_declared_load(pool: PostgresPool) -> int:
    """Replace `effector_declared_load`'s contents from EFFECTOR_DECLARED_
    LOAD_PATH. Returns the number of rows loaded (0 when emptied). Raises
    `DeclaredLoadConfigError` on a malformed file -- the caller (main.py)
    treats that as a fatal startup error, same as a malformed
    projector_config.yaml."""
    path = os.getenv(ENV_PATH, "").strip()
    rows: list[tuple[str, str, str, int]] = []
    if not path:
        log.info(
            "%s not set -- effector_declared_load will be emptied "
            "(every launcher's declared/remaining reads NULL)", ENV_PATH,
        )
    else:
        file = Path(path)
        if not file.is_file():
            log.warning(
                "%s=%s does not name a readable file -- effector_declared_load "
                "will be emptied (every launcher's declared/remaining reads NULL)",
                ENV_PATH, path,
            )
        else:
            raw = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
            rows = _validate_and_flatten(raw)
    await pool.replace_effector_declared_load(rows)
    log.info("effector_declared_load: loaded %d row(s) from %s",
              len(rows), path or "(none configured)")
    return len(rows)
