"""
Hub-scope tests for utils/rag_reconcile.py (v0.3, session B1, A.7).

The hub reconciles only its own sources. These tests put an endpoint source's points (here
'MBP') into the same Qdrant collection as the hub's points, and check that the reconciliation
neither counts, reports nor deletes them, and that the mass-delete guard counts hub points only.

The queue is a real StateDB on a temporary SQLite file. Qdrant is a fake whose facet() and
delete() apply the source_id filter the way the server does, so a missing or wrong filter changes
the answer. No Qdrant server is contacted, and the live queue and registry are never opened.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from qdrant_client.models import FacetResponse, FacetValueHit

from ragnart.registry import RegistryDB
from ragnart.schema import FIELD_SOURCE_ID, FIELD_SOURCE_PATH
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent
MAX_DELETE_FRACTION = 0.10
OWNER = "tester"

# The hub's own sources, as the registry seeds them.
HUB = {"nas", "claude-linux-shared-nfs", "opencode-k11"}

# An endpoint source and one of its paths. Its points carry this source_id, never a hub one.
ENDPOINT = "MBP"
ENDPOINT_KEY = "/Users/fotel/MBP/notes.md"


class ScopedQdrant:
    """
    Fake collection of points. Each point is a (source_id, source_path) pair.

    facet() counts the points by source_path (or by source_id), after applying its facet_filter's
    source_id condition. delete() removes the points that its selector matches, and records the
    (source_id, source_path) pair it deleted. Only the source_id condition of a filter is applied,
    because that is the only condition the reconciliation uses.
    """

    def __init__(self, points: list[tuple[str, str]]) -> None:
        self.points = list(points)
        self.deleted: list[tuple[str, str]] = []

    @staticmethod
    def _allowed_source_ids(flt) -> set[str] | None:
        """The source_ids a facet filter admits, or None when the filter has no source_id condition."""
        if flt is None:
            return None
        for condition in flt.must:
            if condition.key == FIELD_SOURCE_ID:
                return set(condition.match.any)
        return None

    def facet(self, collection_name, key, exact, limit, facet_filter=None):
        allowed = self._allowed_source_ids(facet_filter)
        counts: Counter = Counter()
        for source_id, path in self.points:
            if allowed is not None and source_id not in allowed:
                continue
            counts[path if key == FIELD_SOURCE_PATH else source_id] += 1
        return FacetResponse(hits=[FacetValueHit(value=v, count=c) for v, c in counts.items()])

    def delete(self, collection_name, points_selector, wait=True):
        flt = points_selector.filter
        source_id = next(c.match.value for c in flt.must if c.key == FIELD_SOURCE_ID)
        path = next(c.match.value for c in flt.must if c.key == FIELD_SOURCE_PATH)
        self.deleted.append((source_id, path))
        self.points = [point for point in self.points if point != (source_id, path)]


def _points(source_id: str, path: str, count: int) -> list[tuple[str, str]]:
    """`count` points stored under one source_path of one source."""
    return [(source_id, path)] * count


@pytest.fixture
def reconcile_module():
    """Load utils/rag_reconcile.py as a module. It is a script, not a package."""
    spec = importlib.util.spec_from_file_location("rag_reconcile", REPO_ROOT / "utils" / "rag_reconcile.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    yield db
    db.close()


def _seed_completed(state_db: StateDB, path: str, chunk_count: int) -> None:
    """Add one completed queue row, a hub row, with its chunk_count."""
    state_db.discover_or_update(path, OWNER, last_modified=1.0)
    state_db.mark_completed(path, chunk_count=chunk_count)


def _run(reconcile_module, state_db, client, dry_run: bool = False) -> dict:
    return reconcile_module.run_reconciliation(
        state_db, client, dry_run, MAX_DELETE_FRACTION, HUB
    )


# ---------------------------------------------------------------------------
# run_reconciliation: endpoint points are invisible to the hub's checks
# ---------------------------------------------------------------------------

def test_endpoint_point_without_a_queue_row_is_not_a_zombie_and_is_kept(reconcile_module, state_db):
    _seed_completed(state_db, "/nas/a.md", 2)
    client = ScopedQdrant(_points("nas", "/nas/a.md", 2) + _points(ENDPOINT, ENDPOINT_KEY, 3))

    summary = _run(reconcile_module, state_db, client)

    assert summary["zombie_keys"] == {}
    assert summary["zombie_points"] == 0
    assert client.deleted == []
    assert sum(1 for source_id, _ in client.points if source_id == ENDPOINT) == 3


def test_hub_zombie_point_is_still_deleted(reconcile_module, state_db):
    _seed_completed(state_db, "/nas/a.md", 20)
    client = ScopedQdrant(_points("nas", "/nas/a.md", 20) + _points("nas", "/nas/gone.md", 1))

    summary = _run(reconcile_module, state_db, client)

    assert summary["zombie_keys"] == {"/nas/gone.md": 1}
    assert client.deleted == [("nas", "/nas/gone.md")]


def test_endpoint_source_with_points_and_no_rows_gets_no_warning_and_no_entry(
    reconcile_module, state_db, caplog
):
    _seed_completed(state_db, "/nas/a.md", 2)
    client = ScopedQdrant(_points("nas", "/nas/a.md", 2) + _points(ENDPOINT, ENDPOINT_KEY, 5))

    with caplog.at_level(logging.WARNING):
        summary = _run(reconcile_module, state_db, client, dry_run=True)

    # The per-source table lists the hub sources only, in sorted order, and all of them match.
    assert [check["source_id"] for check in summary["sources"]] == sorted(HUB)
    assert summary["source_mismatches"] == 0
    assert [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING] == []
    # The endpoint's points do not reach the hub's totals either.
    assert summary["total_points"] == 2


def test_endpoint_points_are_not_counted_in_the_mass_delete_total(reconcile_module, state_db):
    # Hub points: 8 healthy + 2 zombie = 10, so the limit is 10% of 10 = 1.0 and 2 zombie points
    # trip the guard. Counting the endpoint's 1000 points would raise the limit to 101 and let it through.
    _seed_completed(state_db, "/nas/a.md", 8)
    client = ScopedQdrant(
        _points("nas", "/nas/a.md", 8)
        + _points("nas", "/nas/gone.md", 2)
        + _points(ENDPOINT, ENDPOINT_KEY, 1000)
    )

    summary = _run(reconcile_module, state_db, client)

    assert summary["total_points"] == 10
    assert summary["zombie_points"] == 2
    assert summary["guard_tripped"] is True
    assert client.deleted == []


def test_empty_hub_scope_is_refused(reconcile_module, state_db):
    with pytest.raises(ValueError, match="hub_source_ids is empty"):
        reconcile_module.run_reconciliation(state_db, ScopedQdrant([]), False, MAX_DELETE_FRACTION, set())


def test_source_path_facet_carries_the_hub_source_filter(reconcile_module, state_db):
    client = MagicMock(name="qdrant")
    client.facet.return_value = FacetResponse(hits=[])

    reconcile_module.run_reconciliation(state_db, client, True, MAX_DELETE_FRACTION, HUB)

    path_call = next(c for c in client.facet.call_args_list if c.kwargs["key"] == FIELD_SOURCE_PATH)
    condition = path_call.kwargs["facet_filter"].must[0]
    assert condition.key == FIELD_SOURCE_ID
    assert condition.match.any == sorted(HUB)


# ---------------------------------------------------------------------------
# main(): the hub ids come from the registry, in both modes
# ---------------------------------------------------------------------------

@pytest.fixture
def hub_install(reconcile_module, tmp_path, monkeypatch):
    """
    Point main() at a temporary database base with a seeded registry and a fake Qdrant client.

    yaml.safe_load is replaced, so main() never reads the live ragnart.yaml values. The client
    holds one endpoint point and no hub point, so any hub-scope mistake shows as a deletion.
    """
    db_base = tmp_path / "db"
    StateDB(str(db_base / "sqlite" / "rag_state.db")).close()
    RegistryDB(str(db_base / "sqlite" / "hub_registry.db")).close()
    config = {
        "db_base_path": str(db_base),
        "rag": {
            "qdrant_url": "http://unused:6333",
            "ingest_lock": str(db_base / "rag-ingest.lock"),
            "sync": {"max_delete_fraction": MAX_DELETE_FRACTION},
        },
    }
    monkeypatch.setattr(reconcile_module.yaml, "safe_load", lambda handle: config)
    client = ScopedQdrant(_points(ENDPOINT, ENDPOINT_KEY, 5))
    monkeypatch.setattr(reconcile_module, "get_client", lambda url, timeout=60: client)
    # The apply path takes the ingestion lock. The lock is not what these tests check.
    monkeypatch.setattr(reconcile_module, "acquire_or_exit", MagicMock(name="acquire_or_exit"))
    return client


def test_main_dry_run_reports_the_hub_sources_and_never_the_endpoint(reconcile_module, hub_install, capsys):
    code = reconcile_module.main(["--dry-run"])

    out = capsys.readouterr().out
    assert code == 0
    for source_id in sorted(HUB):
        assert source_id in out
    assert ENDPOINT not in out
    assert hub_install.deleted == []


def test_main_apply_leaves_the_endpoint_points_alone(reconcile_module, hub_install):
    code = reconcile_module.main(["--apply"])

    assert code == 0
    assert hub_install.deleted == []
    assert len(hub_install.points) == 5
