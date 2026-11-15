"""
Tests for the continuous worker pool in utils/rag_ingest_daemon.py run(), and for
StateDB.claim_next_pending() in ragnart/state.py.

Everything runs on a temporary SQLite file. ingest_file is replaced by a fake that
only sleeps and marks the row completed, so no parser, embedding server or Qdrant
client is touched. qdrant_client is passed as None because the fake never uses it.
"""

from __future__ import annotations

import asyncio
import importlib.util
import time
from pathlib import Path

import pytest

from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent
OWNER = "tester"


@pytest.fixture
def daemon_module():
    """Load utils/rag_ingest_daemon.py as a module. It is a script, not a package."""
    spec = importlib.util.spec_from_file_location("rag_ingest_daemon", REPO_ROOT / "utils" / "rag_ingest_daemon.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    yield db
    db.close()


def _queue(state_db: StateDB, count: int) -> list[str]:
    """Insert `count` pending rows. paths[0] is the oldest (lowest rowid)."""
    paths = [f"/fake/nfs/file{index:03d}.md" for index in range(count)]
    for index, path in enumerate(paths):
        state_db.discover_or_update(path, OWNER, float(index))
    return paths


def _fake_ingest(delay_for, calls: list[str], finished: list[str] | None = None):
    """
    Build a stand-in for ingest_file. It records the call, waits delay_for(file_path)
    seconds, then marks the row completed through the real StateDB, so get_counts()
    reflects what the pool did.
    """

    async def fake_ingest_file(*, file_path, owner, rag_cfg, state_db, qdrant_client, embed_concurrency):
        calls.append(file_path)
        await asyncio.sleep(delay_for(file_path))
        state_db.mark_completed(file_path)
        if finished is not None:
            finished.append(file_path)

    return fake_ingest_file


def test_fast_items_complete_while_slow_item_is_still_running(daemon_module, state_db, monkeypatch):
    """
    With 3 workers and a slow first file, the other files finish first. A batch barrier
    would have made the fast files wait for the slow one.
    """
    paths = _queue(state_db, 4)
    finished: list[str] = []
    calls: list[str] = []

    def delay(file_path: str) -> float:
        return 0.3 if file_path == paths[0] else 0.01

    monkeypatch.setattr(daemon_module, "ingest_file", _fake_ingest(delay, calls, finished))

    asyncio.run(daemon_module.run(state_db, None, {}, embed_concurrency=1, file_workers=3))

    assert finished[-1] == paths[0]
    assert set(finished) == set(paths)


def test_uniform_items_throughput_scales_with_workers(daemon_module, state_db, monkeypatch):
    """
    12 files of 0.1s each with 3 workers take about 0.4s. Run one at a time they would
    take about 1.2s, so the bound of 0.8s shows that the files run concurrently.
    """
    _queue(state_db, 12)
    calls: list[str] = []

    monkeypatch.setattr(daemon_module, "ingest_file", _fake_ingest(lambda _p: 0.1, calls))

    started = time.monotonic()
    asyncio.run(daemon_module.run(state_db, None, {}, embed_concurrency=1, file_workers=3))
    elapsed = time.monotonic() - started

    assert elapsed < 0.8
    assert state_db.get_counts()["completed"] == 12


def test_each_file_ingested_exactly_once(daemon_module, state_db, monkeypatch):
    """Across 4 workers and 20 files, every file is ingested once and none is skipped."""
    paths = _queue(state_db, 20)
    calls: list[str] = []

    monkeypatch.setattr(daemon_module, "ingest_file", _fake_ingest(lambda _p: 0.001, calls))

    asyncio.run(daemon_module.run(state_db, None, {}, embed_concurrency=1, file_workers=4))

    assert sorted(calls) == sorted(paths)
    assert len(calls) == len(set(calls))


def test_claim_next_pending_never_returns_the_same_row_twice(state_db):
    """Claiming 50 pending rows yields 50 distinct files; the 51st claim finds nothing."""
    _queue(state_db, 50)

    claims = [state_db.claim_next_pending() for _ in range(50)]

    assert all(claim is not None for claim in claims)
    assert len({claim["file_path"] for claim in claims}) == 50
    assert state_db.claim_next_pending() is None
