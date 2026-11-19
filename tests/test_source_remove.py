"""
Tests for utils/rag_source_remove.py: the dry-run lists exactly one source's items and writes
nothing; a hub source is refused without --allow-hub-source; --apply deletes only that source's
points (the delete filter is checked), its intake rows and its spool files, and keeps the source
registered; the ingestion lock held means exit 75 and nothing removed.

The hub registry and ragnart.yaml are tmp files. Qdrant is a small fake that stores (source_id,
source_path) pairs and honours the source_id filter of scroll and delete, so the tests check which
points the tool selects, not just which filter it built. The ingestion lock is a recorder or a
refusal, so the production lock file is never taken.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from ragnart.cli import registry_db_path
from ragnart.lock import EXIT_LOCK_HELD
from ragnart.registry import RegistryDB

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _filter_source_id(flt) -> str:
    """The value of the source_id condition of a Qdrant filter. The tests use only this one condition."""
    conditions = [c for c in flt.must if c.key == "source_id"]
    assert len(conditions) == 1, "the removal filter must name exactly one source_id"
    return conditions[0].match.value


class FakeQdrant:
    """
    Stores points as (source_id, source_path) pairs. scroll and delete select by the source_id
    condition of their filter, with exact (case-sensitive) matching, as Qdrant does for a keyword.
    """

    def __init__(self, points):
        self.points = list(points)
        self.scroll_sources: list[str] = []
        self.delete_sources: list[str] = []
        self.delete_wait: list[bool] = []

    def scroll(self, collection_name, scroll_filter, with_payload, with_vectors, limit, offset):
        wanted = _filter_source_id(scroll_filter)
        self.scroll_sources.append(wanted)
        payloads = [
            SimpleNamespace(payload={"source_path": path, "content_hash": "h", "pipeline_version": "v"})
            for source_id, path in self.points if source_id == wanted
        ]
        return payloads, None

    def delete(self, collection_name, points_selector, wait):
        wanted = _filter_source_id(points_selector.filter)
        self.delete_sources.append(wanted)
        self.delete_wait.append(wait)
        self.points = [(sid, path) for sid, path in self.points if sid != wanted]


@pytest.fixture
def tool(monkeypatch):
    """Load utils/rag_source_remove.py as a module. It is a script, not a package."""
    name = "rag_source_remove"
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "utils" / "rag_source_remove.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def qdrant_points():
    """The points the fake Qdrant starts with: two items of MBP, one of claude-MBP, one of nas."""
    return [
        ("MBP", "/Users/fotel/MBP/notes/a.md"),
        ("MBP", "/Users/fotel/MBP/notes/a.md"),
        ("MBP", "/Users/fotel/MBP/b.pdf"),
        ("claude-MBP", "claude-session://s1"),
        ("nas", "/mnt/nfs/Florian/x.md"),
    ]


@pytest.fixture
def env(tmp_path, tool, monkeypatch, qdrant_points):
    """
    A tmp ragnart.yaml, a hub registry with MBP and claude-MBP (endpoint) plus the migrated hub
    sources, intake rows and spool files for each, a fake Qdrant, and a recorder in place of the lock.
    """
    cfg = {
        "db_base_path": str(tmp_path / "db"),
        "rag": {"ingest_lock": str(tmp_path / "rag-ingest.lock"), "qdrant_url": "http://unused:6333"},
    }
    config_path = tmp_path / "ragnart.yaml"
    config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(tool, "CONFIG_PATH", config_path)

    lock_calls: list[str] = []
    monkeypatch.setattr(tool, "acquire_or_exit", lambda path: lock_calls.append(path))

    qdrant = FakeQdrant(qdrant_points)
    monkeypatch.setattr(tool, "get_client", lambda url: qdrant)

    registry_path = registry_db_path(cfg)
    registry = RegistryDB(registry_path)
    registry.register_source("MBP", "filesystem", "endpoint", "florian", hashlib.sha256(b"s1").hexdigest())
    registry.register_source("claude-MBP", "claude_code", "endpoint", "florian", hashlib.sha256(b"s2").hexdigest())

    # Intake rows: MBP has a bytes upload (spooled) and a delete; claude-MBP has a bytes upload too.
    spool_dir = tmp_path / "db" / "spool"
    spool_dir.mkdir(parents=True)
    mbp_spool = spool_dir / "req-mbp-1"
    mbp_spool.write_bytes(b"mbp bytes")
    claude_spool = spool_dir / "req-claude-1"
    claude_spool.write_bytes(b"claude bytes")
    registry.insert_intake("MBP", "upsert", "/Users/fotel/MBP/b.pdf", file_type="pdf",
                           spool_path=str(mbp_spool), request_id="req-mbp-1")
    registry.insert_intake("MBP", "delete", "/Users/fotel/MBP/old.md", request_id="req-mbp-2")
    registry.insert_intake("claude-MBP", "upsert", "claude-session://s1", file_type="md",
                           spool_path=str(claude_spool), request_id="req-claude-1")
    registry.close()

    return {
        "config": cfg,
        "registry_path": registry_path,
        "qdrant": qdrant,
        "lock_calls": lock_calls,
        "mbp_spool": mbp_spool,
        "claude_spool": claude_spool,
    }


def _registry(env) -> RegistryDB:
    return RegistryDB(env["registry_path"])


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def test_dry_run_lists_exactly_that_sources_items_and_writes_nothing(tool, env, capsys):
    """The dry-run reads only MBP's points (filter asserted), prints its items, and changes nothing."""
    rc = tool.main(["--source", "MBP"])
    out = capsys.readouterr().out

    assert rc == 0
    assert env["qdrant"].scroll_sources == ["MBP"]
    assert "/Users/fotel/MBP/notes/a.md" in out
    assert "/Users/fotel/MBP/b.pdf" in out
    assert "claude-session://s1" not in out
    assert "/mnt/nfs/Florian/x.md" not in out
    assert "items: 2, points: 3, intake rows: 2" in out
    # Nothing written: no delete, no lock, every point and intake row still there.
    assert env["qdrant"].delete_sources == []
    assert env["lock_calls"] == []
    assert len(env["qdrant"].points) == 5
    assert env["mbp_spool"].exists()
    assert len(_registry(env).intake_for_source("MBP")) == 2


def test_lookup_is_case_insensitive_and_the_filter_uses_the_stored_id(tool, env, capsys):
    """'mbp' finds MBP, and the Qdrant filter carries the stored id 'MBP', not the typed one."""
    rc = tool.main(["--source", "mbp"])

    assert rc == 0
    assert env["qdrant"].scroll_sources == ["MBP"]
    assert "source: id=MBP" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "argv, lock_calls",
    [(["--source", "nas"], 0), (["--source", "nas", "--apply"], 1)],
    ids=["dry-run", "apply"],
)
def test_hub_source_is_refused_without_the_flag_in_both_modes(tool, env, argv, lock_calls, capsys):
    """
    A hub source is refused by the dry-run and by the apply. Neither reads Qdrant or deletes. The
    apply takes the lock first (as every write does), then refuses, so it records one lock call.
    """
    rc = tool.main(argv)

    assert rc == 1
    assert "hub source" in capsys.readouterr().out
    assert env["qdrant"].scroll_sources == []
    assert env["qdrant"].delete_sources == []
    assert len(env["lock_calls"]) == lock_calls
    assert any(sid == "nas" for sid, _ in env["qdrant"].points)


def test_hub_source_is_listed_with_the_flag_and_applied_only_with_it(tool, env, capsys):
    """With --allow-hub-source the dry-run lists nas, and apply removes nas points only."""
    assert tool.main(["--source", "nas", "--allow-hub-source"]) == 0
    assert "/mnt/nfs/Florian/x.md" in capsys.readouterr().out

    assert tool.main(["--source", "nas", "--allow-hub-source", "--apply"]) == 0
    assert env["qdrant"].delete_sources == ["nas"]
    assert all(sid != "nas" for sid, _ in env["qdrant"].points)
    assert any(sid == "MBP" for sid, _ in env["qdrant"].points)


def test_unregistered_source_is_refused(tool, env, capsys):
    """A name that is not registered is refused, with nothing read from Qdrant."""
    rc = tool.main(["--source", "nobody"])

    assert rc == 1
    assert "no source is registered" in capsys.readouterr().out
    assert env["qdrant"].scroll_sources == []


def test_absent_registry_is_refused(tool, env, monkeypatch, tmp_path, capsys):
    """With no registry file the tool refuses and creates nothing."""
    missing = tmp_path / "absent" / "hub_registry.db"
    monkeypatch.setattr(tool, "registry_db_path", lambda config: str(missing))

    rc = tool.main(["--source", "MBP", "--apply"])

    assert rc == 1
    assert "absent" in capsys.readouterr().out
    assert not missing.exists()
    assert env["qdrant"].delete_sources == []


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def test_apply_deletes_only_that_sources_points_intake_rows_and_spool_files(tool, env, capsys):
    """
    --apply under the lock: MBP's points are deleted with the filter asserted and wait=True, its
    intake rows and spool file are removed, claude-MBP is untouched, and MBP stays registered.
    """
    rc = tool.main(["--source", "MBP", "--apply"])
    out = capsys.readouterr().out

    assert rc == 0
    assert len(env["lock_calls"]) == 1
    assert env["qdrant"].delete_sources == ["MBP"]
    assert env["qdrant"].delete_wait == [True]
    assert env["qdrant"].points == [
        ("claude-MBP", "claude-session://s1"),
        ("nas", "/mnt/nfs/Florian/x.md"),
    ]
    assert "points 3 (items 2), intake rows 2, spool files 1" in out
    assert "stays registered" in out

    registry = _registry(env)
    try:
        assert registry.intake_for_source("MBP") == []
        assert len(registry.intake_for_source("claude-MBP")) == 1
        # The registry row and its secret hash stay: the source is removed from Qdrant, not from the registry.
        assert registry.source_by_id("MBP") is not None
    finally:
        registry.close()
    assert not env["mbp_spool"].exists()
    assert env["claude_spool"].exists()


def test_apply_removes_points_whatever_the_case_of_the_typed_id(tool, env):
    """--source mbp --apply deletes the stored MBP points, not an empty set or another source's."""
    assert tool.main(["--source", "mbp", "--apply"]) == 0

    assert env["qdrant"].delete_sources == ["MBP"]
    assert [sid for sid, _ in env["qdrant"].points].count("MBP") == 0


def test_lock_held_exits_75_and_nothing_is_removed(tool, env, monkeypatch):
    """When the ingestion lock is held the apply exits 75 before any delete, row or file is touched."""
    def refuse_lock(lock_path: str) -> None:
        sys.exit(EXIT_LOCK_HELD)

    monkeypatch.setattr(tool, "acquire_or_exit", refuse_lock)
    points_before = list(env["qdrant"].points)

    with pytest.raises(SystemExit) as exc_info:
        tool.main(["--source", "MBP", "--apply"])

    assert exc_info.value.code == EXIT_LOCK_HELD
    assert env["qdrant"].delete_sources == []
    assert env["qdrant"].points == points_before
    assert env["mbp_spool"].exists()
    assert len(_registry(env).intake_for_source("MBP")) == 2
