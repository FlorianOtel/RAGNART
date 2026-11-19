"""
Import contract for the ingestion side: no heavy hub dependency may load on import.

The ingestion side (the code an endpoint host runs, and the hub's planning code) must import
without docling, qdrant_client or the embedder. Each module is checked in a fresh interpreter,
so modules already loaded by the test session cannot hide a heavy import.

INGESTION_SIDE_MODULES is the ingestion-side contract. The endpoint client (ragnart.endpoint_client)
is on it, and so are the endpoint runner (ragnart.endpoint) and its state database (ragnart.endpoint_state).

The interpreter runs with -I (isolated mode), which drops the working directory from sys.path,
so the repository root is inserted explicitly.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

HEAVY_MODULES = ("docling", "qdrant_client", "ragnart.embeddings")
"""Top-level packages or modules that must not be loaded by an ingestion-side import."""

INGESTION_SIDE_MODULES = [
    "ragnart.chat_render",
    "ragnart.sync_plan_core",
    "ragnart.scanner",
    "ragnart.state",
    "ragnart.registry",
    "ragnart.schema",
    "ragnart.hashing",
    "ragnart.moves",
    "ragnart.projects",
    "ragnart.ragignore",
    "ragnart.endpoint_client",
    "ragnart.endpoint_state",
    "ragnart.endpoint",
]


def _heavy_modules_loaded_after_import(module: str) -> list[str]:
    """
    Import `module` in a fresh interpreter and return the heavy modules now in sys.modules.

    The child prints one JSON list on its last stdout line. A non-zero exit code means the
    import itself failed, and the test reports that before any heavy-module check.
    """
    code = (
        "import json, sys\n"
        f"sys.path.insert(0, {str(REPO_ROOT)!r})\n"
        f"import {module}\n"
        f"heavy = [n for n in sys.modules if any(n == h or n.startswith(h + '.') for h in {HEAVY_MODULES!r})]\n"
        "print(json.dumps(heavy))\n"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"import of {module} failed:\n{result.stderr}"
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("module", INGESTION_SIDE_MODULES)
def test_ingestion_side_module_loads_no_heavy_dependency(module: str) -> None:
    """Importing an ingestion-side module loads none of docling, qdrant_client, ragnart.embeddings."""
    assert _heavy_modules_loaded_after_import(module) == []


def test_detector_sees_heavy_dependency_in_search_module() -> None:
    """
    Control case: ragnart.search does load qdrant_client and the embedder.

    If this failed, the detector above would be checking nothing, so the parametrized
    test would pass vacuously.
    """
    loaded = _heavy_modules_loaded_after_import("ragnart.search")
    assert "qdrant_client" in loaded
    assert "ragnart.embeddings" in loaded
