"""
Test environment setup.

The generated protobuf bindings are not installed as a package; they are
produced into `openddil-contracts/gen/python` and imported from there. In the
container they arrive on PYTHONPATH via the image build; on a host running
pytest, nothing puts them there — so `from openddil.… import …` fails AT
COLLECTION, and pytest aborts the ENTIRE run rather than skipping the file.

That failure mode is why this file exists rather than a note in a README: a
collection error is not a test failure, it is the absence of a test run, and
it reads to a newcomer as an environment problem on their machine.

The path is expressed relative to the sibling contracts repo, the same layout
assumption the other services make. If the workspace is rearranged, this
breaks visibly at collection rather than silently skipping coverage.
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]

_GEN = _REPO_ROOT / "openddil-contracts" / "gen" / "python"
if _GEN.is_dir():
    sys.path.insert(0, str(_GEN))

# ---------------------------------------------------------------------------
# THE STUBS MUST EXIST, AND THEIR ABSENCE MUST SAY SO
# ---------------------------------------------------------------------------
# `if _GEN.is_dir(): sys.path.insert(...)` was silent when the directory was
# absent, and silence is the whole problem: what the developer then saw was
# either `ModuleNotFoundError: No module named 'openddil'` at collection —
# which aborts pytest before any test runs, so it is NO SIGNAL rather than a
# set of failures — or, worse, a suite that ran and failed one test with
# `assert '2' == 'LIFECYCLE_ACTIVE'`, which reads like a real defect in the
# handler and is not.
#
# Neither message names the cause. This does, and it names the command.
#
# The check is on the GENERATED MODULES, not on the directory: `gen/python`
# exists and is empty after a fresh checkout of openddil-contracts, because
# `gen/` is gitignored there and built on demand. A directory test passes in
# exactly the situation this guard exists to catch.
_stub = _GEN / "openddil" / "telemetry" / "v1" / "telemetry_pb2.py"
if not _stub.exists():
    raise RuntimeError(
        "the generated protobuf bindings are missing, so this suite cannot "
        "collect.\n\n"
        "  expected: " + str(_stub) + "\n\n"
        "  generate them with:\n"
        "    cd " + str(_GEN.parents[1]) + "\n"
        "    python -m pip install grpcio-tools\n"
        "    mkdir -p gen/python\n"
        "    python -m grpc_tools.protoc --proto_path=proto "
        "--python_out=gen/python $(find proto -name '*.proto')\n\n"
        "`gen/` is gitignored in openddil-contracts and built on demand, so a "
        "fresh checkout has an EMPTY tree there. This is GD-13: the generated "
        "code is a contract artifact consumed across repository boundaries "
        "and is not published as a package, so every consumer reproduces this "
        "step."
    )

# The projector's modules are imported as top-level packages by the tests.
_SRC = Path(__file__).resolve().parents[1]
if _SRC.is_dir():
    sys.path.insert(0, str(_SRC))


import pytest  # noqa: E402  (must follow the sys.path setup above)


@pytest.fixture(autouse=True)
def _edge_assignment_default():
    """Install a known edge_assignment resolver before every test.

    `edge_assignment` holds a module-level singleton that `main.py` installs
    at startup. Under pytest nothing installs it, so its state was whatever
    the last test to call `configure*()` had left behind — a cross-FILE
    dependency that produced two different failures for one missing setup:
    run after `test_edge_assignment.py` a handler test inherited that file's
    no-op resolver (`edge-unspecified`); run alone it raised "configure() not
    called". Neither failure was about the behaviour under test.

    Order-dependent suites fail in the way that is hardest to read: the same
    test passes or fails depending on what else ran, so the failure appears
    to come from an unrelated file. Tests needing a specific strategy still
    call `configure*()` themselves and override this.

    No test asserts the unconfigured RuntimeError — checked — so installing a
    default here removes a fragility without hiding a behaviour.
    """
    try:
        from edge_assignment import FallbackAssignment, configure
    except Exception:  # pragma: no cover - module not on the path
        return
    configure(strategy=lambda ctx: None,
              fallback=FallbackAssignment("edge-unspecified",
                                          "region-unspecified"))
