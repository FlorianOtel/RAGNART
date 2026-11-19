"""
Tests for utils/rag_source_backfill.py.

The queue is a real StateDB on a temporary SQLite file. Qdrant is a small in-memory fake
that stands in for the client: it evaluates the filters the script builds against point
payloads, and records every set_payload call. No Qdrant server is contacted and the live
database is never opened.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from qdrant_client.models import FieldCondition, Filter, IsEmptyCondition, MatchAny, MatchValue

from ragnart.schema import (
    FIELD_PIPELINE_VERSION,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_TYPE,
    PIPELINE_VERSION_P1,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
)
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent
OWNER = "tester"

NAS_PATH = "/mnt/nfs/Florian/notes.md"
CLAUDE_KEY = "claude-session://u1"
OPENCODE_KEY = "opencode://ses_1"
PRESET_KEY = "claude-session://u2"  # queue row that already carries a source_id: must not change


@pytest.fixture
def backfill(monkeypatch):
    """Load utils/rag_source_backfill.py as a module. It is a script, not a package."""
    name = "rag_source_backfill"
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "utils" / "rag_source_backfill.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

def _condition_holds(payload: dict, condition) -> bool:
    """Whether one payload satisfies one FieldCondition or IsEmptyCondition."""
    if isinstance(condition, IsEmptyCondition):
        return condition.is_empty.key not in payload
    value = payload.get(condition.key)
    if isinstance(condition.match, MatchValue):
        return value is not None and value == condition.match.value
    if isinstance(condition.match, MatchAny):
        return value is not None and value in condition.match.any
    raise AssertionError(f"fake does not evaluate {condition!r}")


def _payload_matches(payload: dict, flt: Filter | None) -> bool:
    """Whether one payload matches a Filter: must (AND), must_not (NONE), should (OR)."""
    if flt is None:
        return True
    must_ok = all(_condition_holds(payload, c) for c in (flt.must or []))
    must_not_ok = not any(_condition_holds(payload, c) for c in (flt.must_not or []))
    should_ok = not flt.should or any(_condition_holds(payload, c) for c in flt.should)
    return must_ok and must_not_ok and should_ok


class FakeQdrant:
    """
    Stand-in for the Qdrant client over an in-memory list of point payloads.

    count() answers from the payloads through the real filter objects, so the tests check the
    filters the script builds. set_payload() merges into the matching payloads and records the
    call (payload, points filter, wait flag).
    """

    def __init__(self, payloads: list[dict]) -> None:
        self.points = [dict(p) for p in payloads]
        self.set_payload_calls: list[dict] = []

    def count(self, collection_name, count_filter=None, exact=True):
        assert exact is True
        return SimpleNamespace(count=sum(1 for p in self.points if _payload_matches(p, count_filter)))

    def set_payload(self, collection_name, payload, points, wait=True):
        self.set_payload_calls.append({"payload": payload, "points": points, "wait": wait})
        for point in self.points:
            if _payload_matches(point, points):
                point.update(payload)


def _points() -> list[dict]:
    """Points of each source_type, none carrying source_id yet (pre-v0.3)."""
    return [
        {FIELD_SOURCE_TYPE: "file"},
        {FIELD_SOURCE_TYPE: "file"},
        {FIELD_SOURCE_TYPE: "claude_session"},
        {FIELD_SOURCE_TYPE: "opencode_session"},
    ]


@pytest.fixture
def state_db(tmp_path):
    """A writable StateDB with one queue row of each kind, plus one row that already has a source_id."""
    db = StateDB(str(tmp_path / "sqlite" / "rag_state.db"))
    db.discover_or_update(NAS_PATH, OWNER, last_modified=1.0)
    db.discover_or_update(CLAUDE_KEY, OWNER, last_modified=1.0)
    db.discover_or_update(OPENCODE_KEY, OWNER, last_modified=1.0)
    # Already carries a source_id: the backfill must leave it alone (its key says claude).
    db.discover_or_update(PRESET_KEY, OWNER, last_modified=1.0, source_id=SOURCE_ID_OPENCODE_K11)
    yield db
    db.close()


def _source_id_by_key(db: StateDB) -> dict[str, str | None]:
    rows = db._conn.execute("SELECT file_path, source_id FROM ingestion_queue").fetchall()
    return {row[0]: row[1] for row in rows}


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def test_dry_run_writes_nothing_and_reports_the_survey(backfill, state_db):
    qdrant = FakeQdrant(_points())
    before_points = [dict(p) for p in qdrant.points]

    summary = backfill.run_backfill(state_db, qdrant, dry_run=True)

    assert qdrant.set_payload_calls == []
    assert qdrant.points == before_points
    assert _source_id_by_key(state_db)[NAS_PATH] is None
    assert summary["refused"] is False
    assert summary["before"]["qdrant"]["per_source_type"] == {"file": 2, "claude_session": 1, "opencode_session": 1}
    # The preset row already has a source_id, so it is not counted as NULL.
    assert summary["before"]["queue"]["null_rows"] == 3
    assert summary["before"]["queue"]["per_source_id"] == {
        SOURCE_ID_NAS: 1,
        SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: 1,
        SOURCE_ID_OPENCODE_K11: 1,
    }
    assert backfill.exit_code(summary) == 0


def test_dry_run_on_a_database_without_the_source_id_column(backfill, tmp_path):
    # Before migration 3 the column does not exist, and a read-only StateDB does not migrate.
    # The dry-run must still count every row as NULL, without touching the file.
    path = tmp_path / "old.db"
    raw = sqlite3.connect(str(path))
    raw.execute("CREATE TABLE ingestion_queue (file_path TEXT PRIMARY KEY, owner TEXT NOT NULL)")
    raw.executemany(
        "INSERT INTO ingestion_queue (file_path, owner) VALUES (?, ?)",
        [(NAS_PATH, OWNER), (CLAUDE_KEY, OWNER)],
    )
    raw.commit()
    raw.close()

    db = StateDB(str(path), read_only=True)
    try:
        survey = backfill.survey_queue(db)
    finally:
        db.close()

    assert survey == {
        "null_rows": 2,
        "per_source_id": {SOURCE_ID_NAS: 1, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: 1},
    }


def test_main_dry_run_never_takes_the_lock_and_never_writes(backfill, tmp_path, monkeypatch):
    StateDB(str(tmp_path / "sqlite" / "rag_state.db")).close()
    qdrant = FakeQdrant(_points())
    lock_mock = MagicMock(name="acquire_or_exit")
    config = {
        "rag": {"qdrant_url": "http://unused:6333", "ingest_lock": str(tmp_path / "rag.lock")},
        "db_base_path": str(tmp_path),
    }
    monkeypatch.setattr(backfill.yaml, "safe_load", lambda handle: config)
    monkeypatch.setattr(backfill, "get_client", lambda url, timeout=60: qdrant)
    monkeypatch.setattr(backfill, "acquire_or_exit", lock_mock)

    assert backfill.main(["--dry-run"]) == 0

    lock_mock.assert_not_called()
    assert qdrant.set_payload_calls == []


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def test_apply_makes_exactly_three_set_payload_calls_with_source_type_filters(backfill, state_db):
    qdrant = FakeQdrant(_points())

    backfill.run_backfill(state_db, qdrant, dry_run=False)

    calls = qdrant.set_payload_calls
    assert len(calls) == 3
    assert [
        ([(c.key, c.match.value) for c in call["points"].must], call["payload"], call["wait"])
        for call in calls
    ] == [
        ([(FIELD_SOURCE_TYPE, "file")],
         {FIELD_SOURCE_ID: SOURCE_ID_NAS, FIELD_PIPELINE_VERSION: PIPELINE_VERSION_P1}, True),
        ([(FIELD_SOURCE_TYPE, "claude_session")],
         {FIELD_SOURCE_ID: SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, FIELD_PIPELINE_VERSION: PIPELINE_VERSION_P1}, True),
        ([(FIELD_SOURCE_TYPE, "opencode_session")],
         {FIELD_SOURCE_ID: SOURCE_ID_OPENCODE_K11, FIELD_PIPELINE_VERSION: PIPELINE_VERSION_P1}, True),
    ]
    # Every point now carries the source_id of its source_type.
    assert [p[FIELD_SOURCE_ID] for p in qdrant.points] == [
        SOURCE_ID_NAS, SOURCE_ID_NAS, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11,
    ]


def test_apply_sets_source_id_on_every_null_queue_row_and_leaves_the_preset_row(backfill, state_db):
    backfill.run_backfill(state_db, FakeQdrant(_points()), dry_run=False)

    assert _source_id_by_key(state_db) == {
        NAS_PATH: SOURCE_ID_NAS,
        CLAUDE_KEY: SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
        OPENCODE_KEY: SOURCE_ID_OPENCODE_K11,
        PRESET_KEY: SOURCE_ID_OPENCODE_K11,  # untouched: it was not NULL
    }


def test_apply_verification_sums_to_the_total_and_has_no_missing_fields(backfill, state_db):
    summary = backfill.run_backfill(state_db, FakeQdrant(_points()), dry_run=False)

    after = summary["after"]
    assert after["points_per_source_id"] == {
        SOURCE_ID_NAS: 2,
        SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: 1,
        SOURCE_ID_OPENCODE_K11: 1,
    }
    assert after["points_sum"] == after["points_total"] == 4
    assert after["points_missing_fields"] == 0
    assert after["queue_per_source_id"] == {
        SOURCE_ID_NAS: 1,
        SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: 1,
        SOURCE_ID_OPENCODE_K11: 2,
    }
    assert backfill.exit_code(summary) == 0


def test_apply_is_idempotent(backfill, state_db):
    qdrant = FakeQdrant(_points())
    first = backfill.run_backfill(state_db, qdrant, dry_run=False)
    points_after_first = [dict(p) for p in qdrant.points]
    keys_after_first = _source_id_by_key(state_db)

    second = backfill.run_backfill(state_db, qdrant, dry_run=False)

    assert qdrant.points == points_after_first
    assert _source_id_by_key(state_db) == keys_after_first
    assert second["queue_rows_set"] == 0
    assert second["before"]["queue"]["null_rows"] == 0
    assert second["after"]["points_sum"] == first["after"]["points_sum"]
    assert backfill.exit_code(second) == 0


# ---------------------------------------------------------------------------
# Refusal
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stray", [{}, {FIELD_SOURCE_TYPE: "document"}])
def test_points_without_a_known_source_type_refuse_the_apply(backfill, state_db, stray):
    # A point with no source_type, or an unexpected one, has no source to be assigned to.
    qdrant = FakeQdrant(_points() + [stray])

    summary = backfill.run_backfill(state_db, qdrant, dry_run=False)

    assert summary["refused"] is True
    assert summary["before"]["qdrant"]["no_source_type"] == 1
    assert qdrant.set_payload_calls == []
    assert _source_id_by_key(state_db)[NAS_PATH] is None
    assert backfill.exit_code(summary) == 1


def test_refusal_is_reported_by_the_dry_run_too(backfill, state_db):
    qdrant = FakeQdrant(_points() + [{}])

    summary = backfill.run_backfill(state_db, qdrant, dry_run=True)

    assert summary["refused"] is True
    assert qdrant.set_payload_calls == []
    assert backfill.exit_code(summary) == 1


def _apply_config(tmp_path):
    """A config with both lock keys. Both are required by the apply path."""
    return {
        "rag": {
            "qdrant_url": "http://unused:6333",
            "ingest_lock": str(tmp_path / "rag.lock"),
            "nas_ingest": {"lock": str(tmp_path / "rag-nas.lock")},
        },
        "db_base_path": str(tmp_path),
    }


def test_main_apply_takes_the_index_lock_then_the_nas_lock_before_writing(backfill, tmp_path, monkeypatch):
    StateDB(str(tmp_path / "sqlite" / "rag_state.db")).close()
    qdrant = FakeQdrant(_points())
    # Each stub returns None, which is what acquire_or_exit returns under an ancestor's lock.
    taken = []
    monkeypatch.setattr(backfill.yaml, "safe_load", lambda handle: _apply_config(tmp_path))
    monkeypatch.setattr(backfill, "get_client", lambda url, timeout=60: qdrant)
    monkeypatch.setattr(backfill, "acquire_or_exit", lambda path: taken.append(path))

    assert backfill.main(["--apply"]) == 0

    assert taken == [str(tmp_path / "rag.lock"), str(tmp_path / "rag-nas.lock")]
    assert len(qdrant.set_payload_calls) == 3


def test_main_apply_with_nas_lock_held_exits_75_and_writes_nothing(backfill, tmp_path, monkeypatch):
    from ragnart.lock import EXIT_LOCK_HELD

    StateDB(str(tmp_path / "sqlite" / "rag_state.db")).close()
    qdrant = FakeQdrant(_points())
    nas_lock = str(tmp_path / "rag-nas.lock")

    # The index lock is free. The NAS lock is held, so main() must stop before any write.
    def _nas_held(lock_path):
        if lock_path == nas_lock:
            sys.exit(EXIT_LOCK_HELD)

    monkeypatch.setattr(backfill.yaml, "safe_load", lambda handle: _apply_config(tmp_path))
    monkeypatch.setattr(backfill, "get_client", lambda url, timeout=60: qdrant)
    monkeypatch.setattr(backfill, "acquire_or_exit", _nas_held)

    with pytest.raises(SystemExit) as exc:
        backfill.main(["--apply"])

    assert exc.value.code == EXIT_LOCK_HELD
    assert qdrant.set_payload_calls == []


def test_print_report_covers_dry_run_refusal_and_apply(backfill, state_db, capsys):
    backfill.print_report(backfill.run_backfill(state_db, FakeQdrant(_points()), dry_run=True))
    assert "DRY-RUN" in capsys.readouterr().out

    backfill.print_report(backfill.run_backfill(state_db, FakeQdrant(_points() + [{}]), dry_run=False))
    assert "REFUSED" in capsys.readouterr().out

    backfill.print_report(backfill.run_backfill(state_db, FakeQdrant(_points()), dry_run=False))
    out = capsys.readouterr().out
    assert "points sum 4 vs total 4: match" in out
    assert "missing source_id or pipeline_version: 0" in out
