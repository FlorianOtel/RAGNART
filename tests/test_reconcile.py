"""
Tests for utils/rag_reconcile.py: row classification, repairs, zombie points and the guard.

The queue is a real StateDB on a temporary SQLite file. Qdrant is a MagicMock whose facet()
returns real qdrant-client FacetResponse models, so the parsing code sees the same shape the
server returns. No Qdrant server is contacted and the live queue is never opened.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from qdrant_client.models import FacetResponse, FacetValueHit, FilterSelector

from ragnart.schema import source_id_for_key
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent
OWNER = "tester"
MAX_DELETE_FRACTION = 0.10
MISSING_AT = "2026-11-15T00:00:00+00:00"


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


def _qdrant(points_by_key: dict[str, int], points_by_source: dict[str, int] | None = None) -> MagicMock:
    """
    A Qdrant mock whose facet() answers per key: on source_path with points_by_key, on source_id
    with points_by_source. When points_by_source is not given it is derived from the keys, so the
    two facets agree unless a test says otherwise.
    """
    if points_by_source is None:
        points_by_source = {}
        for key, count in points_by_key.items():
            source = source_id_for_key(key)
            points_by_source[source] = points_by_source.get(source, 0) + count

    def facet(collection_name, key, exact, limit):
        table = points_by_key if key == "source_path" else points_by_source
        return FacetResponse(hits=[FacetValueHit(value=k, count=c) for k, c in table.items()])

    client = MagicMock(name="qdrant")
    client.facet.side_effect = facet
    return client


def _seed(state_db: StateDB, path: str, chunk_count: int | None = None,
          status: str = "completed", missing: bool = False) -> None:
    """Add one queue row. A completed row gets chunk_count (None leaves it NULL)."""
    state_db.discover_or_update(path, OWNER, last_modified=1.0)
    if status == "completed":
        state_db.mark_completed(path, chunk_count=chunk_count)
    if missing:
        state_db.mark_missing(path, MISSING_AT)


def _row(state_db: StateDB, path: str) -> dict:
    return dict(state_db._conn.execute(
        "SELECT file_path, status, chunk_count, missing_since FROM ingestion_queue WHERE file_path = ?",
        (path,),
    ).fetchone())


def _run(reconcile_module, state_db, client, dry_run: bool = False) -> dict:
    return reconcile_module.run_reconciliation(state_db, client, dry_run, MAX_DELETE_FRACTION)


# ---------------------------------------------------------------------------
# classify_row: the decision table, one line per case
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "chunk_count, points, missing_since, expected",
    [
        (None, 3, None, "fill_null_chunk_count"),
        (None, 0, None, "fill_null_chunk_count"),
        (4, 4, None, "ok"),
        (0, 0, None, "ok_zero"),
        (4, 2, None, "requeue"),
        (4, 0, None, "requeue"),
        (2, 5, None, "anomaly"),
        (0, 4, None, "anomaly"),
        (3, 3, MISSING_AT, "tombstoned"),
        (3, 0, MISSING_AT, "tombstoned"),
    ],
)
def test_classify_row_decision_table(reconcile_module, chunk_count, points, missing_since, expected):
    row = {"chunk_count": chunk_count, "missing_since": missing_since}
    assert reconcile_module.classify_row(row, points) == expected


# ---------------------------------------------------------------------------
# Repairs (--apply)
# ---------------------------------------------------------------------------

def test_null_chunk_count_is_filled_from_qdrant_count(reconcile_module, state_db):
    _seed(state_db, "/nas/a.md", chunk_count=None)
    client = _qdrant({"/nas/a.md": 3})

    summary = _run(reconcile_module, state_db, client)

    assert _row(state_db, "/nas/a.md")["chunk_count"] == 3
    assert summary["filled"] == 1


def test_fewer_points_than_chunk_count_requeues_the_row(reconcile_module, state_db):
    _seed(state_db, "/nas/b.md", chunk_count=5)
    client = _qdrant({"/nas/b.md": 2})

    summary = _run(reconcile_module, state_db, client)

    assert _row(state_db, "/nas/b.md")["status"] == "pending"
    assert summary["requeued"] == 1


def test_no_points_at_all_for_a_row_with_chunks_requeues_it(reconcile_module, state_db):
    # A key missing from the facet has zero points, which is fewer than chunk_count > 0.
    _seed(state_db, "/nas/c.md", chunk_count=4)

    _run(reconcile_module, state_db, _qdrant({}))

    assert _row(state_db, "/nas/c.md")["status"] == "pending"


def test_zero_chunk_count_zero_points_is_untouched(reconcile_module, state_db):
    _seed(state_db, "/nas/empty.md", chunk_count=0)
    client = _qdrant({})

    summary = _run(reconcile_module, state_db, client)

    row = _row(state_db, "/nas/empty.md")
    assert (row["status"], row["chunk_count"]) == ("completed", 0)
    assert summary["kinds"]["ok_zero"] == 1
    client.delete.assert_not_called()


def test_zero_chunk_count_with_points_is_logged_not_repaired(reconcile_module, state_db, caplog):
    _seed(state_db, "/nas/odd.md", chunk_count=0)
    client = _qdrant({"/nas/odd.md": 4})

    with caplog.at_level(logging.ERROR, logger="rag_reconcile"):
        summary = _run(reconcile_module, state_db, client)

    row = _row(state_db, "/nas/odd.md")
    assert (row["status"], row["chunk_count"]) == ("completed", 0)
    assert [path for path, _, _ in summary["anomalies"]] == ["/nas/odd.md"]
    assert "/nas/odd.md" in caplog.text
    assert reconcile_module.exit_code(summary) == 1


def test_points_above_chunk_count_is_an_anomaly_not_repaired(reconcile_module, state_db):
    _seed(state_db, "/nas/extra.md", chunk_count=2)

    summary = _run(reconcile_module, state_db, _qdrant({"/nas/extra.md": 5}))

    assert _row(state_db, "/nas/extra.md")["chunk_count"] == 2
    assert len(summary["anomalies"]) == 1


def test_tombstoned_rows_are_never_requeued(reconcile_module, state_db):
    # A vanished file keeps its points for the grace period. Its row must not be requeued
    # (the file is gone), and its state is not an anomaly, with or without points.
    _seed(state_db, "/nas/gone-with-points.md", chunk_count=3, missing=True)
    _seed(state_db, "/nas/gone-no-points.md", chunk_count=3, missing=True)
    client = _qdrant({"/nas/gone-with-points.md": 3})

    summary = _run(reconcile_module, state_db, client)

    assert _row(state_db, "/nas/gone-with-points.md")["status"] == "completed"
    assert _row(state_db, "/nas/gone-no-points.md")["status"] == "completed"
    assert summary["kinds"]["tombstoned"] == 2
    assert summary["anomalies"] == []
    assert reconcile_module.exit_code(summary) == 0


def test_non_completed_rows_are_not_reconciled(reconcile_module, state_db):
    _seed(state_db, "/nas/pending.md", chunk_count=None, status="pending")

    summary = _run(reconcile_module, state_db, _qdrant({}))

    assert _row(state_db, "/nas/pending.md")["chunk_count"] is None
    assert summary["kinds"]["not_completed"] == 1
    assert summary["zombie_keys"] == {}


# ---------------------------------------------------------------------------
# Zombie points and the mass-delete guard
# ---------------------------------------------------------------------------

def test_zombie_points_are_deleted_under_the_guard(reconcile_module, state_db):
    _seed(state_db, "/nas/live.md", chunk_count=100)
    # 3 zombie points against 103 total: 3 is under 10% of 103, so the guard allows the delete.
    client = _qdrant({"/nas/live.md": 100, "/nas/zombie.md": 3})

    summary = _run(reconcile_module, state_db, client)

    client.delete.assert_called_once()
    kwargs = client.delete.call_args.kwargs
    assert kwargs["wait"] is True
    assert isinstance(kwargs["points_selector"], FilterSelector)
    assert kwargs["points_selector"].filter.must[1].match.value == "/nas/zombie.md"
    assert summary["zombie_points_deleted"] == 3
    assert summary["guard_tripped"] is False
    assert reconcile_module.exit_code(summary) == 0


def test_zombie_delete_is_refused_when_the_guard_trips(reconcile_module, state_db):
    _seed(state_db, "/nas/live.md", chunk_count=10)
    # 5 zombie points against 15 total: 5 is over 10% of 15 (limit 1.5). Nothing is deleted.
    client = _qdrant({"/nas/live.md": 10, "/nas/zombie.md": 5})

    summary = _run(reconcile_module, state_db, client)

    client.delete.assert_not_called()
    assert summary["guard_tripped"] is True
    assert summary["zombie_points_deleted"] == 0
    assert reconcile_module.exit_code(summary) == 1


def test_zombie_delete_filter_names_source_id_and_source_path(reconcile_module, state_db):
    # The item filter is two conditions: the source_id derived from the key AND the source_path.
    _seed(state_db, "/nas/live.md", chunk_count=100)
    client = _qdrant({"/nas/live.md": 100, "claude-session://gone": 3})

    _run(reconcile_module, state_db, client)

    flt = client.delete.call_args.kwargs["points_selector"].filter
    assert [(c.key, c.match.value) for c in flt.must] == [
        ("source_id", "claude-linux-shared-nfs"),
        ("source_path", "claude-session://gone"),
    ]


def test_zero_count_facet_values_are_ignored(reconcile_module, state_db):
    # A facet value with count 0 has no points. It must not count as a zombie key.
    _seed(state_db, "/nas/live.md", chunk_count=1)
    client = _qdrant({"/nas/live.md": 1, "/nas/ghost.md": 0})

    summary = _run(reconcile_module, state_db, client)

    client.delete.assert_not_called()
    assert summary["zombie_keys"] == {}


def test_facet_is_one_call_per_field_not_one_count_per_row(reconcile_module, state_db):
    # One facet on source_path (per key) and one on source_id (per source). Never a count per row.
    for index in range(3):
        _seed(state_db, f"/nas/f{index}.md", chunk_count=1)
    client = _qdrant({f"/nas/f{index}.md": 1 for index in range(3)})

    _run(reconcile_module, state_db, client)

    keys = [call.kwargs["key"] for call in client.facet.call_args_list]
    assert sorted(keys) == ["source_id", "source_path"]
    assert all(call.kwargs["exact"] is True for call in client.facet.call_args_list)
    client.count.assert_not_called()


def test_per_source_check_matches_when_chunk_sums_equal_points(reconcile_module, state_db):
    _seed(state_db, "/nas/a.md", chunk_count=2)
    _seed(state_db, "claude-session://u1", chunk_count=3)
    client = _qdrant({"/nas/a.md": 2, "claude-session://u1": 3})

    summary = _run(reconcile_module, state_db, client)

    assert summary["sources"] == [
        {"source_id": "claude-linux-shared-nfs", "chunk_sum": 3, "points": 3, "match": True},
        {"source_id": "nas", "chunk_sum": 2, "points": 2, "match": True},
    ]
    assert summary["source_mismatches"] == 0


def test_per_source_mismatch_is_reported_but_changes_neither_repairs_nor_exit_code(
    reconcile_module, state_db, caplog
):
    # Every key agrees with its own chunk_count, but the source_id facet says the nas source holds
    # 9 points where its completed rows sum to 4. That is a WARN: no row is touched, exit code 0.
    _seed(state_db, "/nas/a.md", chunk_count=4)
    client = _qdrant({"/nas/a.md": 4}, points_by_source={"nas": 9})

    with caplog.at_level(logging.WARNING):
        summary = _run(reconcile_module, state_db, client)

    assert summary["sources"] == [{"source_id": "nas", "chunk_sum": 4, "points": 9, "match": False}]
    assert summary["source_mismatches"] == 1
    assert summary["anomalies"] == []
    assert summary["filled"] == 0 and summary["requeued"] == 0
    assert reconcile_module.exit_code(summary) == 0
    assert "Per-source check mismatch: source_id=nas chunk_sum=4 points=9" in caplog.text


def test_per_source_check_uses_the_key_not_the_source_id_column(reconcile_module, state_db):
    # The sum is grouped by the key's prefix. The source_id column is not set in this test
    # (the backfill has not run), and the check still groups the claude row under its own source.
    _seed(state_db, "claude-session://u2", chunk_count=5)
    client = _qdrant({"claude-session://u2": 5})

    summary = _run(reconcile_module, state_db, client)

    assert summary["sources"] == [
        {"source_id": "claude-linux-shared-nfs", "chunk_sum": 5, "points": 5, "match": True},
    ]


def test_truncated_facet_aborts_the_run(reconcile_module, state_db, monkeypatch):
    # A truncated answer would make real rows look empty and requeue them. The run must stop.
    monkeypatch.setattr(reconcile_module, "FACET_LIMIT", 2)
    _seed(state_db, "/nas/a.md", chunk_count=1)
    client = _qdrant({"/nas/a.md": 1, "/nas/b.md": 1})

    with pytest.raises(RuntimeError, match="truncated"):
        _run(reconcile_module, state_db, client)
    assert _row(state_db, "/nas/a.md")["status"] == "completed"


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def test_dry_run_makes_no_writes(reconcile_module, state_db):
    _seed(state_db, "/nas/null.md", chunk_count=None)
    _seed(state_db, "/nas/short.md", chunk_count=4)
    client = _qdrant({"/nas/short.md": 1, "/nas/zombie.md": 2})

    summary = _run(reconcile_module, state_db, client, dry_run=True)

    assert _row(state_db, "/nas/null.md")["chunk_count"] is None
    assert _row(state_db, "/nas/short.md")["status"] == "completed"
    client.delete.assert_not_called()
    assert summary["filled"] == 1
    assert summary["requeued"] == 1
    assert summary["zombie_points"] == 2
    assert summary["zombie_points_deleted"] == 0


def test_print_report_shows_the_per_source_rows(reconcile_module, state_db, capsys):
    _seed(state_db, "/nas/a.md", chunk_count=2)
    client = _qdrant({"/nas/a.md": 2}, points_by_source={"nas": 5})
    summary = _run(reconcile_module, state_db, client, dry_run=True)

    reconcile_module.print_report(summary)

    out = capsys.readouterr().out
    assert "per-source check (WARN only): 1 mismatch(es)" in out
    assert "nas" in out and "NO" in out
