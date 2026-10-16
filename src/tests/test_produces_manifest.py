"""The code must produce exactly the topics produces.yaml lists.

AST scan of the shipped code under src/ (tests excluded); everything under src/
is what the image runs, so nothing else is scanned and there are no operator
tools, bootstrap scripts or migrations in this repo to exclude.

Producer call sites: `.send(`, `.send_and_wait(` and `.produce(`. The repo has
no publish wrapper of its own: the one producer is the link heartbeat
(`producer.produce(LINK_HEARTBEAT_TOPIC, ...)` in edge_buffer_monitor.py). Each
site's topic must resolve statically (literal, module constant,
os.environ.get / os.getenv) or carry `# produces: <manifest entry>`.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import produces_manifest as pm  # noqa: E402

MANIFEST = pm.load(ROOT / "produces.yaml")
METHODS = {"send", "send_and_wait", "produce"}
ANNOTATION = re.compile(r"#\s*produces:\s*(.+?)\s*$")
ENV = {}  # fixture env: every code default applies


def _parents(tree):
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node


def _env_template(call):
    """os.environ.get("X", "d") / os.getenv("X", "d") -> '${X:-d}', else None."""
    f = call.func
    ok = isinstance(f, ast.Attribute) and (
        (f.attr == "getenv" and isinstance(f.value, ast.Name) and f.value.id == "os")
        or (f.attr == "get" and isinstance(f.value, ast.Attribute) and f.value.attr == "environ"))
    if not ok or not call.args or not isinstance(call.args[0], ast.Constant):
        return None
    var = call.args[0].value
    if len(call.args) > 1 and isinstance(call.args[1], ast.Constant) and isinstance(call.args[1].value, str):
        return f"${{{var}:-{call.args[1].value}}}"
    return f"${{{var}}}"


def _scan():
    templates, bad, sites = set(), [], {}
    for path in sorted((ROOT / "src").rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if "/tests/" in rel or path.name.startswith("test_"):
            continue
        src = path.read_text(encoding="utf-8")
        lines = src.splitlines()
        tree = ast.parse(src)
        _parents(tree)
        consts = {}
        for n in tree.body:
            if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
                if isinstance(n.value, ast.Constant) and isinstance(n.value.value, str):
                    consts[n.targets[0].id] = n.value.value
                elif isinstance(n.value, ast.Call) and _env_template(n.value):
                    consts[n.targets[0].id] = _env_template(n.value)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if not (isinstance(f, ast.Attribute) and f.attr in METHODS):
                continue
            sites[rel] = sites.get(rel, 0) + 1
            span = lines[max(node.lineno - 2, 0):node.end_lineno]
            note = next((m.group(1).strip("\"'") for m in map(ANNOTATION.search, span) if m), None)
            if note:
                templates.add(note)
                continue
            kw = {k.arg: k.value for k in node.keywords}
            arg = kw.get("topic") or (node.args[0] if node.args else None)
            got = None
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                got = arg.value
            elif isinstance(arg, ast.Name):
                got = consts.get(arg.id)
            elif isinstance(arg, ast.Call):
                got = _env_template(arg)
            if got:
                templates.add(got)
            else:
                bad.append(f"{rel}:{node.lineno}")
    return templates, bad, sites


def test_code_produces_exactly_the_manifest():
    templates, bad, sites = _scan()
    assert sites, "no producer call sites found: the scan is not looking at the code"
    assert not bad, ("unresolvable producer call sites (annotate with '# produces: <entry>'): "
                     + ", ".join(bad))
    got = {pm.expand(t, ENV) for t in templates}
    declared = {pm.expand(t, ENV) for w in MANIFEST.values() for t in w["topics"]}
    msg = [f"manifest lists a topic the code does not produce: {t}" for t in sorted(declared - got)]
    msg += [f"code produces a topic the manifest does not list: {t}" for t in sorted(got - declared)]
    assert not msg, "\n".join(msg)


def test_manifest_is_exactly_the_the_projector_image():
    assert set(MANIFEST) == {"projector"}
    assert {w["image"] for w in MANIFEST.values()} == {"openddil/projector"}
