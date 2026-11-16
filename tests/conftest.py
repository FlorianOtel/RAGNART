"""
Pytest configuration shared by every test module.

The tests import the `ragnart` package and the `utils/` scripts directly from the
repository checkout. The repository root is not installed into the shared venv
(an editable install is a separate, operator-approved step), so we put the root
on sys.path here. This mirrors the `sys.path.insert(...)` pattern used at the top
of the `utils/rag_*.py` scripts.
"""

import sys
from pathlib import Path

import pytest

# tests/conftest.py -> parent is tests/, parent.parent is the repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Insert at the front so the checkout's `ragnart` package wins over any installed copy.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))



@pytest.fixture
def service_state_db(tmp_path, monkeypatch):
    """
    Give the search service a real, tmp queue database.

    The service's lifespan opens the queue read-only through
    ragnart.service.open_queue_read_only. This fixture seeds a writable StateDB
    at tmp_path (schema and migrations applied, the global row present), and
    replaces the opener so the lifespan opens that same file read-only.

    Returns the writable StateDB so a test can seed projects and items through
    the public StateDB methods.
    """
    # Imported here, not at module level, so that loading conftest does not import the service.
    import ragnart.service as service
    from ragnart.state import StateDB

    db_path = str(tmp_path / "rag_state.db")
    writable = StateDB(db_path)
    monkeypatch.setattr(
        service,
        "open_queue_read_only",
        lambda config: StateDB(db_path, read_only=True),
    )
    yield writable
    writable.close()
