"""
Tests for ragnart/sync_plan.py: build_sync_plan(), apply_sync_plan(), format_plan().

A real temporary StateDB holds the queue and a real temporary RegistryDB holds the
projects. The Qdrant client is a MagicMock, so no server is contacted. The scan result
is built by hand, so no filesystem is walked.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from ragnart.moves import EMPTY_FILE_MD5
from ragnart.registry import RegistryDB
from ragnart.schema import (
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    GLOBAL_PROJECT_ID,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
)
from ragnart.state import StateDB
from ragnart.sync_plan import apply_sync_plan, build_sync_plan, format_plan

OWNER = "tester"
NOW = "2026-11-16T12:00:00+00:00"
# Older than the 24 h grace period when measured from NOW.
EXPIRED = "2026-11-15T11:00:00+00:00"
# Inside the grace period.
FRESH = "2026-11-15T13:00:00+00:00"


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    yield db
    db.close()


@pytest.fixture
def registry_db(tmp_path):
    db = RegistryDB(str(tmp_path / "registry" / "hub_registry.db"))
    yield db
    db.close()


@pytest.fixture
def qdrant() -> MagicMock:
    return MagicMock(name="qdrant")


def _config(max_delete_fraction: float = 0.10) -> dict:
    return {
        "rag": {
            "scanner": {"include_extensions": [".md"]},
            "sync": {"max_delete_fraction": max_delete_fraction},
        },
    }


def _scan(
    existing=(),
    new_hashes: dict | None = None,
    markers=(),
    skipped=(),
    opencode_existing=set(),
) -> dict:
    """
    A scan_result as the three scanners return it. `existing` is every path the scan saw,
    new files included. `opencode_existing=None` means the OpenCode API was unreachable.
    """
    return {
        "nfs": {
            "scanned": len(existing),
            "existing_paths": set(existing),
            "skipped_roots": list(skipped),
            "project_markers": list(markers),
            "new_hashes": dict(new_hashes or {}),
        },
        "chats": {"scanned": 0, "existing_paths": set(), "skipped_roots": []},
        "opencode": {"scanned": 0, "existing_paths": opencode_existing},
    }


def _marker(path: str, project_id: str, name: str = "P", tags: dict | None = None, owner: str = OWNER) -> dict:
    return {
        "path": path,
        "owner": owner,
        "name": name,
        "id": project_id,
        "inherit_access": True,
        "mode": "crawl",
        "tags": tags or {},
    }


def _completed(
    state_db: StateDB,
    path: str,
    *,
    content_hash: str | None = None,
    chunk_count: int = 3,
    project_id: str = GLOBAL_PROJECT_ID,
    launch_dir: str | None = None,
    source_type: str = "file",
    owner: str = OWNER,
) -> None:
    """Queue a row and mark it completed, as if ingested. Session rows carry no origin_path."""
    state_db.discover_or_update(
        path, owner, 1.0,
        origin_path=path if source_type == "file" else None,
        source_type=source_type,
    )
    state_db.mark_completed(
        path,
        chunk_count=chunk_count,
        source_type=source_type,
        content_hash=content_hash,
        launch_dir=launch_dir,
        project_id=project_id,
    )


def _tombstone(state_db: StateDB, path: str, when: str, **kwargs) -> None:
    """A completed row that has already been marked missing at `when`."""
    _completed(state_db, path, **kwargs)
    state_db.mark_missing(path, when)


def _row(state_db: StateDB, path: str) -> dict | None:
    row = state_db._conn.execute("SELECT * FROM ingestion_queue WHERE file_path = ?", (path,)).fetchone()
    return dict(row) if row else None


def _filter_value(flt, key: str) -> str:
    """The value a Qdrant filter matches for one payload key. The filter must name that key exactly once."""
    (condition,) = [c for c in flt.must if c.key == key]
    return condition.match.value


def _set_payload_paths(qdrant: MagicMock) -> list[tuple[str, dict]]:
    """(source_path filter value, payload) for every set_payload call, in call order."""
    return [
        (_filter_value(call.kwargs["points"], FIELD_SOURCE_PATH), call.kwargs["payload"])
        for call in qdrant.set_payload.call_args_list
    ]


def _deleted_paths(qdrant: MagicMock) -> list[str]:
    return [
        _filter_value(call.kwargs["points_selector"].filter, FIELD_SOURCE_PATH)
        for call in qdrant.delete.call_args_list
    ]


# ----------------------------------------------------------------------------
# Moves
# ----------------------------------------------------------------------------


def test_move_renames_key_in_sqlite_and_qdrant_and_deletes_the_scanned_pending_row(state_db, registry_db, qdrant):
    old, new = "/nas/old.md", "/nas/new.md"
    _completed(state_db, old, content_hash="h1", chunk_count=7)
    # The scan has already queued the new path as a pending row.
    state_db.discover_or_update(new, OWNER, 2.0, origin_path=new, source_type="file")
    scan = _scan(existing={new}, new_hashes={new: "h1"})

    plan = build_sync_plan(state_db, registry_db, scan, _config(), NOW)
    assert plan.moves == {old: new}
    assert plan.newly_missing == []

    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)

    # Qdrant: the old key's points now carry the new source_path, and nothing else changed.
    assert _set_payload_paths(qdrant) == [(old, {"source_path": new})]
    assert qdrant.set_payload.call_args.kwargs["wait"] is True
    qdrant.delete.assert_not_called()

    # SQLite: one row, at the new key, keeping chunk_count and content_hash.
    assert _row(state_db, old) is None
    moved = _row(state_db, new)
    assert moved["status"] == "completed"
    assert moved["chunk_count"] == 7
    assert moved["content_hash"] == "h1"
    assert moved["origin_path"] == new


def test_move_excludes_empty_file_hash(state_db, registry_db, qdrant):
    old, new = "/nas/empty-a.md", "/nas/empty-b.md"
    _completed(state_db, old, content_hash=EMPTY_FILE_MD5)
    state_db.discover_or_update(new, OWNER, 2.0, origin_path=new, source_type="file")

    plan = build_sync_plan(state_db, registry_db, _scan(existing={new}, new_hashes={new: EMPTY_FILE_MD5}), _config(1.0), NOW)

    assert plan.moves == {}
    assert plan.newly_missing == [old]


def test_ambiguous_hash_match_is_not_a_move(state_db, registry_db, qdrant):
    """Two vanished files share a hash with one new file: none of the three pairs is a move."""
    old_a, old_b, new = "/nas/a.md", "/nas/b.md", "/nas/c.md"
    _completed(state_db, old_a, content_hash="same")
    _completed(state_db, old_b, content_hash="same")
    _completed(state_db, "/nas/keep.md", content_hash="other")
    state_db.discover_or_update(new, OWNER, 2.0, origin_path=new, source_type="file")
    scan = _scan(existing={new, "/nas/keep.md"}, new_hashes={new: "same"})

    plan = build_sync_plan(state_db, registry_db, scan, _config(1.0), NOW)

    assert plan.moves == {}
    assert sorted(plan.newly_missing) == sorted([old_a, old_b])


def test_session_key_is_never_a_move_source(state_db, registry_db, qdrant):
    """A vanished Claude session must not be re-keyed onto a NAS file with the same content."""
    session = "claude-session://47cfe354-39f7-45a0-993e-1df37c4fb88e"
    new = "/nas/copy.md"
    _completed(state_db, session, content_hash="h1", source_type="claude_session")
    state_db.discover_or_update(new, OWNER, 2.0, origin_path=new, source_type="file")

    plan = build_sync_plan(state_db, registry_db, _scan(existing={new}, new_hashes={new: "h1"}), _config(1.0), NOW)

    assert plan.moves == {}
    assert plan.newly_missing == [session]


def test_move_without_pending_row_is_refused(state_db, registry_db, qdrant):
    """The move target must have the pending row the scan created. Without it, stop rather than guess."""
    old, new = "/nas/old.md", "/nas/new.md"
    _completed(state_db, old, content_hash="h1")
    plan = build_sync_plan(state_db, registry_db, _scan(existing={new}, new_hashes={new: "h1"}), _config(), NOW)
    assert plan.moves == {old: new}

    with pytest.raises(RuntimeError, match="no pending row"):
        apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)
    qdrant.set_payload.assert_not_called()


# ----------------------------------------------------------------------------
# Tombstones, reappearance, purge
# ----------------------------------------------------------------------------


def test_newly_missing_item_keeps_its_qdrant_points(state_db, registry_db, qdrant):
    gone = "/nas/gone.md"
    _completed(state_db, gone, content_hash="h-gone")
    _completed(state_db, "/nas/kept.md", content_hash="h-kept")
    scan = _scan(existing={"/nas/kept.md"})

    plan = build_sync_plan(state_db, registry_db, scan, _config(0.9), NOW)
    assert plan.newly_missing == [gone]
    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)

    qdrant.delete.assert_not_called()
    assert _row(state_db, gone)["missing_since"] == NOW
    assert _row(state_db, gone)["status"] == "completed"


def test_tombstone_keeps_first_missing_time_across_syncs(state_db, registry_db, qdrant):
    """A file still missing on a later sync keeps the time it was first seen missing."""
    gone = "/nas/gone.md"
    _completed(state_db, gone)
    _completed(state_db, "/nas/kept.md")

    # The first sync is 23 h before the second, so the row is still inside the grace period on the second.
    first = build_sync_plan(state_db, registry_db, _scan(existing={"/nas/kept.md"}), _config(0.9), FRESH)
    apply_sync_plan(state_db, registry_db, qdrant, first, FRESH)
    later_plan = build_sync_plan(state_db, registry_db, _scan(existing={"/nas/kept.md"}), _config(0.9), NOW)
    apply_sync_plan(state_db, registry_db, qdrant, later_plan, NOW)

    assert later_plan.newly_missing == []
    assert later_plan.purge_ready == []
    assert _row(state_db, gone)["missing_since"] == FRESH


def test_tombstoned_rows_are_not_counted_again_by_the_guard(state_db, registry_db, qdrant):
    """A row tombstoned on an earlier sync and still gone is not vanished again, so the guard ignores it."""
    gone = "/nas/gone.md"
    _completed(state_db, gone)
    kept = [f"/nas/k{i}.md" for i in range(9)]
    for path in kept:
        _completed(state_db, path)
    _tombstone(state_db, "/nas/old-gone.md", FRESH)

    plan = build_sync_plan(state_db, registry_db, _scan(existing=set(kept) | {gone}), _config(0.1), NOW)

    assert plan.guard_tripped is False
    assert gone not in plan.newly_missing
    assert "/nas/old-gone.md" not in plan.newly_missing


def test_reappeared_item_clears_missing_since(state_db, registry_db, qdrant):
    path = "/nas/back.md"
    _tombstone(state_db, path, FRESH)

    plan = build_sync_plan(state_db, registry_db, _scan(existing={path}), _config(), NOW)
    assert plan.reappeared == [path]
    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)

    assert _row(state_db, path)["missing_since"] is None
    qdrant.delete.assert_not_called()


def test_purge_ready_deletes_points_then_row(state_db, registry_db, qdrant):
    """Qdrant is written first. The row still exists while the delete runs, so a crash leaves it to retry."""
    path = "/nas/expired.md"
    _tombstone(state_db, path, EXPIRED, chunk_count=3)
    row_seen_during_delete: list[bool] = []

    def record_row_then_succeed(**kwargs):
        row_seen_during_delete.append(_row(state_db, path) is not None)

    qdrant.delete.side_effect = record_row_then_succeed

    plan = build_sync_plan(state_db, registry_db, _scan(existing=set()), _config(1.0), NOW)
    assert plan.purge_ready == [path]
    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)

    assert _deleted_paths(qdrant) == [path]
    assert qdrant.delete.call_args.kwargs["wait"] is True
    assert row_seen_during_delete == [True]
    assert _row(state_db, path) is None


def test_tombstone_inside_the_grace_period_is_not_purged(state_db, registry_db, qdrant):
    path = "/nas/fresh.md"
    _tombstone(state_db, path, FRESH)

    plan = build_sync_plan(state_db, registry_db, _scan(existing=set()), _config(1.0), NOW)

    assert plan.purge_ready == []


def test_purge_ready_is_not_subject_to_the_mass_delete_guard(state_db, registry_db, qdrant):
    """Five expired tombstones are 50% of the completed rows. They still purge; the guard only gates new tombstones."""
    expired = [f"/nas/e{i}.md" for i in range(5)]
    for path in expired:
        _tombstone(state_db, path, EXPIRED)
    active = [f"/nas/a{i}.md" for i in range(4)]
    for path in active:
        _completed(state_db, path)
    just_vanished = "/nas/just.md"
    _completed(state_db, just_vanished)
    # 10 completed rows in all, so the 10% limit is 1. One new tombstone is within it.
    scan = _scan(existing=set(active))

    plan = build_sync_plan(state_db, registry_db, scan, _config(0.10), NOW)
    assert plan.guard_tripped is False
    assert plan.newly_missing == [just_vanished]
    assert sorted(plan.purge_ready) == sorted(expired)

    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)
    assert sorted(_deleted_paths(qdrant)) == sorted(expired)
    for path in expired:
        assert _row(state_db, path) is None


def test_mass_delete_guard_blocks_newly_missing_not_purge(state_db, registry_db, qdrant):
    """Three of eleven completed rows vanish (27%, over 10%). They are not tombstoned; the expired tombstone still purges."""
    vanished = [f"/nas/v{i}.md" for i in range(3)]
    for path in vanished:
        _completed(state_db, path)
    expired = "/nas/expired.md"
    _tombstone(state_db, expired, EXPIRED)
    active = [f"/nas/a{i}.md" for i in range(7)]
    for path in active:
        _completed(state_db, path)

    plan = build_sync_plan(state_db, registry_db, _scan(existing=set(active)), _config(0.10), NOW)

    assert plan.guard_tripped is True
    assert plan.newly_missing == []
    assert plan.purge_ready == [expired]

    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)
    assert _deleted_paths(qdrant) == [expired]
    for path in vanished:
        assert _row(state_db, path)["missing_since"] is None


def test_allow_mass_delete_tombstones_the_vanished_rows(state_db, registry_db, qdrant):
    vanished = [f"/nas/v{i}.md" for i in range(3)]
    for path in vanished:
        _completed(state_db, path)
    active = [f"/nas/a{i}.md" for i in range(7)]
    for path in active:
        _completed(state_db, path)

    plan = build_sync_plan(
        state_db, registry_db, _scan(existing=set(active)), _config(0.10), NOW, allow_mass_delete=True,
    )

    assert plan.guard_tripped is False
    assert sorted(plan.newly_missing) == sorted(vanished)


# ----------------------------------------------------------------------------
# Skipped roots, projects and attachment
# ----------------------------------------------------------------------------


def test_skipped_root_blocks_moves_and_newly_missing_but_not_purge_or_attachment(state_db, registry_db, qdrant):
    old_moved, new = "/nas/moved.md", "/nas/moved-new.md"
    _completed(state_db, old_moved, content_hash="h1")
    state_db.discover_or_update(new, OWNER, 2.0, origin_path=new, source_type="file")
    _completed(state_db, "/nas/vanished.md", content_hash="h2")
    expired = "/nas/expired.md"
    _tombstone(state_db, expired, EXPIRED)
    inside_project = "/nas/proj/doc.md"
    _completed(state_db, inside_project, project_id=GLOBAL_PROJECT_ID)
    scan = _scan(
        existing={new, inside_project},
        new_hashes={new: "h1"},
        markers=[_marker("/nas/proj", "pid-proj")],
        skipped=["/nas/unmounted"],
    )

    plan = build_sync_plan(state_db, registry_db, scan, _config(1.0), NOW)

    assert plan.skipped is True
    assert plan.moves == {}
    assert plan.newly_missing == []
    assert plan.purge_ready == [expired]
    assert plan.attachment_changes == {inside_project: "pid-proj"}
    assert [m["id"] for m in plan.project_upserts] == ["pid-proj"]


def test_skipped_root_keeps_projects_whose_marker_is_missing(state_db, registry_db, qdrant):
    """A skipped root may hide a marker, so no project is removed in that run."""
    registry_db.upsert_project("pid-old", "/nas/old-proj", "Old", OWNER, SOURCE_ID_NAS)

    plan = build_sync_plan(state_db, registry_db, _scan(skipped=["/nas/unmounted"]), _config(), NOW)

    assert plan.project_removals == []


def test_project_marker_is_upserted_with_its_home_source_and_tags(state_db, registry_db, qdrant):
    """Projects and their tags are written to the registry. The NAS crawl is their home source."""
    scan = _scan(markers=[
        _marker("/root/outer", "pid-outer", name="Outer", tags={"kind": "customer"}),
        _marker("/root/outer/inner", "pid-inner", name="Inner"),
    ])

    plan = build_sync_plan(state_db, registry_db, scan, _config(), NOW)
    assert [m["id"] for m in plan.project_upserts] == ["pid-outer", "pid-inner"]

    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)
    stored = {p["id"]: p for p in registry_db.list_projects()}
    assert stored["pid-outer"]["path"] == "/root/outer"
    assert stored["pid-outer"]["owner"] == OWNER
    assert stored["pid-outer"]["home_source_id"] == SOURCE_ID_NAS
    assert registry_db.project_tags("pid-outer") == {"kind": "customer"}


def test_duplicate_marker_id_and_reserved_id_are_ignored(state_db, registry_db, qdrant, caplog):
    scan = _scan(markers=[
        _marker("/root/first", "pid-dup"),
        _marker("/root/copy", "pid-dup"),
        _marker("/root/global", GLOBAL_PROJECT_ID),
    ])

    with caplog.at_level(logging.ERROR, logger="ragnart.sync_plan"):
        plan = build_sync_plan(state_db, registry_db, scan, _config(), NOW)

    assert [m["path"] for m in plan.project_upserts] == ["/root/first"]
    assert "reuses id pid-dup" in caplog.text
    assert "reserved id" in caplog.text


def test_removed_marker_removes_project_and_reattaches_its_items(state_db, registry_db, qdrant):
    """First sync: the marker exists and its file attaches. Second sync: the marker is gone."""
    doc = "/root/p/doc.md"
    _completed(state_db, doc, project_id=GLOBAL_PROJECT_ID)
    first = build_sync_plan(state_db, registry_db, _scan(existing={doc}, markers=[_marker("/root/p", "pid-p")]), _config(), NOW)
    apply_sync_plan(state_db, registry_db, qdrant, first, NOW)
    assert _row(state_db, doc)["project_id"] == "pid-p"
    qdrant.reset_mock()

    second = build_sync_plan(state_db, registry_db, _scan(existing={doc}), _config(), NOW)
    assert second.project_removals == ["pid-p"]
    assert second.attachment_changes == {doc: GLOBAL_PROJECT_ID}
    apply_sync_plan(state_db, registry_db, qdrant, second, NOW)

    assert "pid-p" not in [p["id"] for p in registry_db.list_projects()]
    assert _row(state_db, doc)["project_id"] == GLOBAL_PROJECT_ID
    assert _set_payload_paths(qdrant) == [(doc, {"project_id": GLOBAL_PROJECT_ID})]


def test_removal_is_limited_to_the_scanned_owner(state_db, registry_db, qdrant):
    """A --user run scanned only florian. La-Familia's project is not scanned, so it is not removed."""
    registry_db.upsert_project("pid-laf", "/nas/shared/proj", "Shared", "la-familia", SOURCE_ID_NAS)

    plan = build_sync_plan(state_db, registry_db, _scan(), _config(), NOW, owners=[OWNER])

    assert plan.project_removals == []


def test_attachment_changes_only_rows_whose_project_id_actually_differs(state_db, registry_db, qdrant):
    _completed(state_db, "/root/p/a.md", project_id=GLOBAL_PROJECT_ID)  # should attach
    _completed(state_db, "/root/p/b.md", project_id="pid-p")            # already attached
    _completed(state_db, "/elsewhere/c.md", project_id=GLOBAL_PROJECT_ID)  # not under the project
    scan = _scan(
        existing={"/root/p/a.md", "/root/p/b.md", "/elsewhere/c.md"},
        markers=[_marker("/root/p", "pid-p")],
    )

    plan = build_sync_plan(state_db, registry_db, scan, _config(), NOW)

    assert plan.attachment_changes == {"/root/p/a.md": "pid-p"}


def test_attachment_uses_launch_dir_for_sessions_not_file_path(state_db, registry_db, qdrant):
    inside = "claude-session://in-project"
    outside = "claude-session://elsewhere"
    no_dir = "claude-session://no-launch-dir"
    for key, launch_dir in ((inside, "/root/p/sub"), (outside, "/elsewhere"), (no_dir, None)):
        _completed(state_db, key, source_type="claude_session", launch_dir=launch_dir)
    scan = _scan(markers=[_marker("/root/p", "pid-p")])

    plan = build_sync_plan(state_db, registry_db, scan, _config(), NOW)

    assert plan.attachment_changes == {inside: "pid-p"}


def test_moved_item_is_attached_once_under_its_new_path(state_db, registry_db, qdrant):
    """A file moved from outside a project into it: the attachment is keyed by the new path."""
    old, new = "/outside/a.md", "/root/p/a.md"
    _completed(state_db, old, content_hash="h1", project_id=GLOBAL_PROJECT_ID)
    state_db.discover_or_update(new, OWNER, 2.0, origin_path=new, source_type="file")
    scan = _scan(existing={new}, new_hashes={new: "h1"}, markers=[_marker("/root/p", "pid-p")])

    plan = build_sync_plan(state_db, registry_db, scan, _config(), NOW)
    assert plan.moves == {old: new}
    assert plan.attachment_changes == {new: "pid-p"}

    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)
    assert _set_payload_paths(qdrant) == [(old, {"source_path": new}), (new, {"project_id": "pid-p"})]
    assert _row(state_db, new)["project_id"] == "pid-p"


def test_point_filters_name_both_source_id_and_source_path(state_db, registry_db, qdrant):
    """
    Every point write names the item's source_id AND its source_path. The source comes from the key:
    a NAS move filters on the NAS source, and a Claude session purge on the Claude Code source.
    """
    old, new = "/nas/old.md", "/nas/new.md"
    _completed(state_db, old, content_hash="h1")
    state_db.discover_or_update(new, OWNER, 2.0, origin_path=new, source_type="file")
    session = "claude-session://0c3e9f1a-5b7d-4c2e-9f1a-7d6b5e4c3a21"
    _tombstone(state_db, session, EXPIRED, source_type="claude_session")
    scan = _scan(existing={new}, new_hashes={new: "h1"})

    plan = build_sync_plan(state_db, registry_db, scan, _config(1.0), NOW)
    apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)

    move_call = qdrant.set_payload.call_args
    assert _filter_value(move_call.kwargs["points"], FIELD_SOURCE_ID) == SOURCE_ID_NAS
    assert _filter_value(move_call.kwargs["points"], FIELD_SOURCE_PATH) == old

    purge_call = qdrant.delete.call_args
    assert _filter_value(purge_call.kwargs["points_selector"].filter, FIELD_SOURCE_ID) == SOURCE_ID_CLAUDE_LINUX_SHARED_NFS
    assert _filter_value(purge_call.kwargs["points_selector"].filter, FIELD_SOURCE_PATH) == session


# ----------------------------------------------------------------------------
# Logs and rendering
# ----------------------------------------------------------------------------


def test_purge_logs_one_line_with_key_chunks_reason_and_missing_since(state_db, registry_db, qdrant, caplog):
    path = "/nas/expired.md"
    _tombstone(state_db, path, EXPIRED, chunk_count=5)
    plan = build_sync_plan(state_db, registry_db, _scan(), _config(1.0), NOW)

    with caplog.at_level(logging.INFO, logger="ragnart.sync_plan"):
        apply_sync_plan(state_db, registry_db, qdrant, plan, NOW)

    purge_lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("purged ")]
    assert purge_lines == [
        f"purged {path} chunks=5 reason=tombstone grace period expired missing_since={EXPIRED}"
    ]


def test_format_plan_lists_moves_and_counts_each_category(state_db, registry_db, qdrant):
    _completed(state_db, "/nas/old.md", content_hash="h1")
    state_db.discover_or_update("/nas/new.md", OWNER, 2.0, origin_path="/nas/new.md", source_type="file")
    plan = build_sync_plan(state_db, registry_db, _scan(existing={"/nas/new.md"}, new_hashes={"/nas/new.md": "h1"}), _config(), NOW)

    text = format_plan(plan)

    assert "moves: 1" in text
    assert "/nas/old.md -> /nas/new.md" in text
    assert "newly missing (tombstoned, points kept): 0" in text
    assert "roots skipped (no moves, tombstones or removals): no" in text


def test_purge_is_limited_to_the_scanned_owner(state_db, registry_db, qdrant):
    """A --user run must not purge another owner's expired tombstone, even though that row is not seen."""
    shared = "/nas/shared/expired.md"
    _tombstone(state_db, shared, EXPIRED, owner="la-familia")
    own = "/nas/own/expired.md"
    _tombstone(state_db, own, EXPIRED, owner=OWNER)

    plan = build_sync_plan(state_db, registry_db, _scan(), _config(1.0), NOW, owners=[OWNER])

    assert plan.purge_ready == [own]
