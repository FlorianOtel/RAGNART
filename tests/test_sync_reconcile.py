"""
Tests for ragnart/sync_reconcile.py (session B2, steps A.10 and A.10b): reconcile against the hub listing, and
rebuild of a lost rag_state.db from it.

The hub is a MagicMock HubClient whose items() and info() answers are set per test. The state is a real StateDB
in a tmp file. The Claude transcripts are tmp .jsonl files rendered by the real parser. Nothing live is touched:
no hub, Qdrant, OpenCode server or NFS path.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ragnart.chat_render import _parse_claude_chat
from ragnart.endpoint_client import HubClient, HubError
from ragnart.hashing import hash_file, hash_text
from ragnart.schema import (
    DEFAULT_RESUBMIT_BATCH_SIZE,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
    pipeline_version_is_older,
)
from ragnart.state import StateDB
from ragnart.sync_reconcile import (
    claude_session_paths,
    present_keys,
    rebuild_from_hub,
    reconcile_source,
)

NAS_A = "/mnt/nfs/Florian/a.md"
NAS_B = "/mnt/nfs/Florian/b.md"
NAS_C = "/mnt/nfs/Florian/c.md"
CLAUDE_UUID = "11111111-2222-3333-4444-555555555555"
CLAUDE_KEY = f"claude-session://{CLAUDE_UUID}"
OPENCODE_KEY = "opencode://ses_abc"
MTIME = 1_700_000_000.0
FRACTION = 0.10  # rag.sync.max_delete_fraction in ragnart.yaml


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def state(tmp_path) -> StateDB:
    db = StateDB(str(tmp_path / "rag_state.db"))
    yield db
    db.close()


@pytest.fixture
def hub() -> MagicMock:
    """A hub client with the pipeline version the API reports. items() is set per test."""
    client = MagicMock(spec=HubClient)
    client.info.return_value = {"pipeline_version": "p1"}
    client.items.return_value = []
    return client


def _item(key, *, owner="florian", project_id="global", pipeline_version="p1", content_hash="h",
          chunk_count=2, source_type="file", file_type="md", inconsistent=()) -> dict:
    """One item as GET /v1/items returns it (ragnart/ingest_api.py group_points_by_key)."""
    return {
        "key": key, "chunk_count": chunk_count, "content_hash": content_hash,
        "pipeline_version": pipeline_version, "owner": owner, "project_id": project_id,
        "source_type": source_type, "file_type": file_type, "inconsistent": list(inconsistent),
    }


def _completed(state: StateDB, key: str, *, source_id=SOURCE_ID_NAS, owner="florian", project_id="global") -> None:
    """A row as the NAS side leaves it after a successful upsert: completed, with two chunks."""
    state.discover_or_update(key, owner, MTIME, origin_path=key, source_type="file", file_type="md",
                             source_id=source_id)
    state.mark_completed(key, chunk_count=2, pipeline_version="p1", content_hash="h", project_id=project_id)


def _fill(state: StateDB, count: int) -> None:
    """Completed NAS rows that the hub also lists, so they only raise the completed total (the guard's limit)."""
    for index in range(count):
        _completed(state, f"/mnt/nfs/Florian/filler-{index}.md")


def _statuses(state: StateDB) -> dict[str, str]:
    return {row["file_path"]: row["status"] for row in state.rows_for_reconcile()}


def _reconcile(state, hub, present=(), *, source=SOURCE_ID_NAS, **kwargs) -> Counter:
    return reconcile_source(state, hub, source, set(present), FRACTION, **kwargs)


# ---------------------------------------------------------------------------
# present_keys
# ---------------------------------------------------------------------------

def test_present_keys_unions_the_three_scans_and_skips_an_unreachable_opencode():
    """The keys the scan saw: NAS files and sessions always, OpenCode sessions only when the API answered."""
    scan = {
        "nfs": {"existing_paths": {NAS_A}},
        "chats": {"existing_paths": {CLAUDE_KEY}},
        "opencode": {"existing_paths": {OPENCODE_KEY}},
    }
    assert present_keys(scan) == {NAS_A, CLAUDE_KEY, OPENCODE_KEY}
    scan["opencode"]["existing_paths"] = None
    assert present_keys(scan) == {NAS_A, CLAUDE_KEY}


# ---------------------------------------------------------------------------
# Rule (a): orphans
# ---------------------------------------------------------------------------

def test_hub_item_with_no_local_row_is_deleted_when_the_source_has_completed_rows(state, hub):
    """An orphan is deleted when the source has completed rows and the orphan count is under the guard."""
    _fill(state, 20)  # limit = 0.10 * 20 = 2 orphans
    hub.items.return_value = [_item(NAS_A)]

    counts = _reconcile(state, hub)

    hub.delete.assert_called_once_with(NAS_A)
    assert counts["orphan_deleted"] == 1


def test_orphans_are_not_deleted_when_the_source_has_no_completed_row(state, hub):
    """An empty state for the source (no completed row) never deletes the hub's items, and says so at WARNING."""
    _completed(state, NAS_B)  # a NAS row exists, but the source reconciled is claude
    hub.items.return_value = [_item(CLAUDE_KEY, source_type="claude_session", file_type="claude_chat")]

    counts = _reconcile(state, hub, source=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)

    hub.delete.assert_not_called()
    assert counts["orphan_skipped_no_completed_rows"] == 1


def test_orphans_above_the_mass_delete_limit_are_not_deleted_and_are_counted_at_warning(state, hub, caplog):
    """More orphans than max_delete_fraction of the completed rows: nothing is deleted, and the count is logged."""
    _fill(state, 20)  # limit = 2.0
    hub.items.return_value = [_item(f"/mnt/nfs/Florian/gone-{i}.md") for i in range(3)]

    with caplog.at_level(logging.WARNING, logger="ragnart.sync_reconcile"):
        counts = _reconcile(state, hub)

    hub.delete.assert_not_called()
    assert counts["orphan_guard_tripped"] == 3
    assert any(record.levelno == logging.WARNING and "3 orphan(s)" in record.getMessage()
               for record in caplog.records)


def _claude_key(index: int) -> str:
    """A distinct Claude session key for a filler row (the key's prefix names the source)."""
    return f"claude-session://00000000-0000-0000-0000-{index:012d}"


def test_orphan_guard_uses_the_source_own_completed_count_not_the_global_one(state, hub):
    """
    Two sources filled unevenly: NAS has 10 completed rows, Claude has 90 (100 in all).

    Two orphans on the NAS side are 20% of the NAS rows, above the 10% limit, so the guard must trip. Measured
    against the global count (100 rows, limit 10) they would be allowed, which is the bug this test guards against.
    """
    _fill(state, 10)  # NAS completed rows: limit = 0.10 * 10 = 1.0
    for index in range(90):
        _completed(state, _claude_key(index), source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)
    hub.items.return_value = [_item(f"/mnt/nfs/Florian/gone-{i}.md") for i in range(2)]

    counts = _reconcile(state, hub)

    hub.delete.assert_not_called()
    assert counts["orphan_guard_tripped"] == 2
    assert "orphan_deleted" not in counts


def test_orphan_guard_allows_orphans_under_the_source_own_fraction_even_when_the_source_is_small(state, hub):
    """
    The converse. Claude has 90 completed rows and NAS has 10. Two orphans on the Claude side are 2.2% of the Claude
    rows, under the 10% limit, so they are deleted. Measured against the NAS count they would not be (limit 1.0).
    """
    for index in range(90):
        _completed(state, _claude_key(index), source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)
    _fill(state, 10)
    hub.items.return_value = [_item(_claude_key(1000 + i), source_type="claude_session", file_type="claude_chat")
                              for i in range(2)]

    counts = _reconcile(state, hub, source=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)

    assert hub.delete.call_count == 2
    assert counts["orphan_deleted"] == 2
    assert "orphan_guard_tripped" not in counts


def test_rows_that_are_pending_processing_or_ignored_are_not_orphans(state, hub):
    """A local row in any non-completed status is a row: its hub item is neither an orphan nor missing."""
    _fill(state, 20)
    _completed(state, NAS_A)
    state.discover_or_update(NAS_B, "florian", MTIME, origin_path=NAS_B, source_id=SOURCE_ID_NAS)  # pending
    state.discover_or_update(NAS_C, "florian", MTIME, origin_path=NAS_C, source_id=SOURCE_ID_NAS)
    state.mark_processing(NAS_C, request_id="req-1")
    ignored = "/mnt/nfs/Florian/ignored.md"
    state.discover_or_update(ignored, "florian", MTIME, origin_path=ignored, source_id=SOURCE_ID_NAS)
    state.mark_ignored(ignored, "too large")
    hub.items.return_value = [_item(NAS_B), _item(NAS_C), _item(ignored)]

    counts = _reconcile(state, hub)

    hub.delete.assert_not_called()
    assert "orphan_deleted" not in counts
    assert "missing_resubmitted" not in counts


def test_a_failed_orphan_delete_is_counted_and_the_run_goes_on(state, hub):
    """A hub refusal for one orphan is counted; the next orphan is still deleted."""
    _fill(state, 30)  # limit = 3.0
    hub.items.return_value = [_item(NAS_A), _item(NAS_B)]
    hub.delete.side_effect = [HubError(500, "boom"), {"request_id": "req-2"}]

    counts = _reconcile(state, hub)

    assert counts["orphan_failed"] == 1
    assert counts["orphan_deleted"] == 1
    assert hub.delete.call_count == 2


def test_dry_run_counts_orphans_without_sending_a_delete(state, hub):
    """Dry run: the orphan is counted as would-delete, and no delete is sent."""
    _fill(state, 20)
    hub.items.return_value = [_item(NAS_A)]

    counts = _reconcile(state, hub, dry_run=True)

    hub.delete.assert_not_called()
    assert counts["orphan_would_delete"] == 1


# ---------------------------------------------------------------------------
# Rule (b): a completed row the hub does not list, but the scan still sees
# ---------------------------------------------------------------------------

def test_completed_present_row_the_hub_does_not_list_is_set_pending_again(state, hub):
    """A completed row whose item is on disk but not listed is set pending, so the submit path sends it."""
    _completed(state, NAS_A)
    hub.items.return_value = []

    counts = _reconcile(state, hub, present={NAS_A})

    assert _statuses(state)[NAS_A] == "pending"
    assert counts["missing_resubmitted"] == 1


def test_completed_row_that_vanished_is_not_resubmitted(state, hub):
    """A completed row whose item the scan did not see is left to the tombstone path, not resubmitted."""
    _completed(state, NAS_A)

    counts = _reconcile(state, hub, present=set())

    assert _statuses(state)[NAS_A] == "completed"
    assert "missing_resubmitted" not in counts


def test_dry_run_leaves_a_missing_row_completed(state, hub):
    """Dry run counts the resubmission and writes nothing."""
    _completed(state, NAS_A)

    counts = _reconcile(state, hub, present={NAS_A}, dry_run=True)

    assert _statuses(state)[NAS_A] == "completed"
    assert counts["missing_would_resubmit"] == 1


def test_completed_zero_chunk_row_the_hub_does_not_list_is_not_resubmitted(state, hub):
    """
    A completed row with chunk_count 0 has no points, so the hub never lists it. It is an empty item's final
    outcome, not a missing one: it stays completed and is counted as skipped, not resubmitted.
    """
    _completed(state, NAS_A)
    # mark_completed(chunk_count=0) writes 0 (only None is left out), as the poll does for a zero-chunk answer.
    state.mark_completed(NAS_A, chunk_count=0)
    hub.items.return_value = []

    counts = _reconcile(state, hub, present={NAS_A})

    assert _statuses(state)[NAS_A] == "completed"
    assert "missing_resubmitted" not in counts
    assert counts["missing_zero_chunks_skipped"] == 1


def test_completed_zero_chunk_row_is_not_resubmitted_on_a_second_run(state, hub):
    """Repeated reconciles leave a zero-chunk completion alone: no loop, every run."""
    _completed(state, NAS_A)
    state.mark_completed(NAS_A, chunk_count=0)
    hub.items.return_value = []

    for _ in range(3):
        _reconcile(state, hub, present={NAS_A})

    assert _statuses(state)[NAS_A] == "completed"
    hub.delete.assert_not_called()


def test_completed_row_with_null_chunk_count_the_hub_does_not_list_is_resubmitted(state, hub):
    """A NULL chunk_count is an unrecorded count, not zero: the row stays eligible, as before."""
    # discover_or_update then mark_completed without chunk_count leaves the column NULL.
    state.discover_or_update(NAS_A, "florian", MTIME, origin_path=NAS_A, source_type="file", file_type="md",
                             source_id=SOURCE_ID_NAS)
    state.mark_completed(NAS_A, pipeline_version="p1", content_hash="h", project_id="global")
    hub.items.return_value = []

    counts = _reconcile(state, hub, present={NAS_A})

    assert _statuses(state)[NAS_A] == "pending"
    assert counts["missing_resubmitted"] == 1


def test_zero_chunk_rows_do_not_hide_a_real_missing_row(state, hub):
    """In one run a zero-chunk row is skipped while a completed row with chunks beside it is still resubmitted."""
    _completed(state, NAS_A)
    state.mark_completed(NAS_A, chunk_count=0)
    _completed(state, NAS_B)
    hub.items.return_value = []

    counts = _reconcile(state, hub, present={NAS_A, NAS_B})

    assert _statuses(state) == {NAS_A: "completed", NAS_B: "pending"}
    assert counts["missing_resubmitted"] == 1
    assert counts["missing_zero_chunks_skipped"] == 1


# ---------------------------------------------------------------------------
# Rule (c): older pipeline versions, capped by the batch size
# ---------------------------------------------------------------------------

def test_completed_present_row_with_an_older_hub_version_is_resubmitted(state, hub):
    """With /v1/info at p2, an item the hub holds at p1 is set pending again."""
    _completed(state, NAS_A)
    hub.info.return_value = {"pipeline_version": "p2"}
    hub.items.return_value = [_item(NAS_A, pipeline_version="p1")]

    counts = _reconcile(state, hub, present={NAS_A})

    assert _statuses(state)[NAS_A] == "pending"
    assert counts["version_resubmitted"] == 1


def test_version_resubmission_is_capped_by_the_batch_size_and_the_rest_is_deferred(state, hub):
    """Five items on an older version with a batch of two: the first two by key are resubmitted, three are deferred."""
    keys = [f"/mnt/nfs/Florian/v-{index}.md" for index in range(5)]
    for key in keys:
        _completed(state, key)
    hub.info.return_value = {"pipeline_version": "p2"}
    hub.items.return_value = [_item(key, pipeline_version="p1") for key in keys]

    counts = _reconcile(state, hub, present=set(keys), resubmit_batch_size=2)

    statuses = _statuses(state)
    assert [statuses[key] for key in keys] == ["pending", "pending", "completed", "completed", "completed"]
    assert counts["version_resubmitted"] == 2
    assert counts["version_deferred"] == 3


def test_default_batch_size_is_200():
    """The default cap of the version rule is 200 items a run."""
    assert DEFAULT_RESUBMIT_BATCH_SIZE == 200


@pytest.mark.parametrize("version, current, older", [
    ("p1", "p2", True),
    ("p2", "p2", False),
    ("p3", "p2", False),
    (None, "p2", True),
    ("p10", "p9", False),   # numeric, not string, order
    ("weird", "p2", False),  # cannot be ordered: left alone
    ("p1", "weird", False),
])
def test_pipeline_version_order(version, current, older):
    """The one version rule, shared by both sides: numeric order on 'p<n>', None is older, anything else is left alone."""
    assert pipeline_version_is_older(version, current) is older


def test_equal_or_newer_or_unordered_versions_are_not_resubmitted(state, hub):
    """A current version, a newer one, and a version that cannot be ordered are all left alone."""
    keys = [NAS_A, NAS_B, NAS_C]
    for key in keys:
        _completed(state, key)
    hub.info.return_value = {"pipeline_version": "p2"}
    hub.items.return_value = [
        _item(NAS_A, pipeline_version="p2"),
        _item(NAS_B, pipeline_version="p3"),
        _item(NAS_C, pipeline_version="weird"),
    ]

    counts = _reconcile(state, hub, present=set(keys))

    assert set(_statuses(state).values()) == {"completed"}
    assert "version_resubmitted" not in counts


def test_a_point_with_no_version_is_older_and_resubmitted(state, hub):
    """A point written before the version field existed (None) counts as older."""
    _completed(state, NAS_A)
    hub.items.return_value = [_item(NAS_A, pipeline_version=None)]

    counts = _reconcile(state, hub, present={NAS_A})

    assert counts["version_resubmitted"] == 1


def test_a_batch_size_below_one_is_refused_before_anything_is_read(state, hub):
    """A resubmit batch of zero is a configuration error: nothing is read from the hub or written."""
    with pytest.raises(ValueError):
        _reconcile(state, hub, resubmit_batch_size=0)
    hub.items.assert_not_called()


# ---------------------------------------------------------------------------
# Rule (d): drift of owner and project_id
# ---------------------------------------------------------------------------

def test_hub_owner_that_differs_is_corrected_with_the_local_owner(state, hub):
    """A hub owner that differs from the local row gets change_metadata with the local owner only."""
    _completed(state, NAS_A, owner="eva")
    hub.items.return_value = [_item(NAS_A, owner="florian")]

    counts = _reconcile(state, hub, present={NAS_A})

    hub.change_metadata.assert_called_once_with(NAS_A, owner="eva")
    assert counts["drift_fixed"] == 1


def test_hub_project_that_differs_is_corrected_with_the_local_project(state, hub):
    """A hub project_id that differs gets change_metadata with the local project only."""
    _completed(state, NAS_A, project_id="proj-a")
    hub.items.return_value = [_item(NAS_A, project_id="global")]

    _reconcile(state, hub, present={NAS_A})

    hub.change_metadata.assert_called_once_with(NAS_A, project_id="proj-a")


def test_both_drifted_fields_are_sent_in_one_request(state, hub):
    """When owner and project differ, one change_metadata carries both local values."""
    _completed(state, NAS_A, owner="eva", project_id="proj-a")
    hub.items.return_value = [_item(NAS_A, owner="florian", project_id="global")]

    _reconcile(state, hub, present={NAS_A})

    hub.change_metadata.assert_called_once_with(NAS_A, owner="eva", project_id="proj-a")


def test_matching_metadata_sends_nothing(state, hub):
    """Equal owner and project: no change_metadata request."""
    _completed(state, NAS_A)
    hub.items.return_value = [_item(NAS_A)]

    counts = _reconcile(state, hub, present={NAS_A})

    hub.change_metadata.assert_not_called()
    assert "drift_fixed" not in counts


def test_a_pending_row_is_not_checked_for_drift(state, hub):
    """A row not yet applied on the hub has nothing to compare: its upsert carries the local values."""
    state.discover_or_update(NAS_A, "eva", MTIME, origin_path=NAS_A, source_id=SOURCE_ID_NAS)
    hub.items.return_value = [_item(NAS_A, owner="florian")]

    _reconcile(state, hub, present={NAS_A})

    hub.change_metadata.assert_not_called()


def test_a_failed_metadata_change_is_counted_and_retried_next_run(state, hub):
    """A refused change_metadata is counted; the row keeps its local values and the next run checks again."""
    _completed(state, NAS_A, owner="eva")
    hub.items.return_value = [_item(NAS_A, owner="florian")]
    hub.change_metadata.side_effect = HubError(500, "boom")

    counts = _reconcile(state, hub, present={NAS_A})

    assert counts["drift_failed"] == 1
    assert "drift_fixed" not in counts


def test_dry_run_counts_drift_without_sending(state, hub):
    """Dry run: drift is counted, and no change_metadata is sent."""
    _completed(state, NAS_A, owner="eva")
    hub.items.return_value = [_item(NAS_A, owner="florian")]

    counts = _reconcile(state, hub, present={NAS_A}, dry_run=True)

    hub.change_metadata.assert_not_called()
    assert counts["drift_would_fix"] == 1


# ---------------------------------------------------------------------------
# Inconsistent items and the empty-run case
# ---------------------------------------------------------------------------

def test_an_item_whose_points_disagree_is_counted_and_logged(state, hub, caplog):
    """An item flagged inconsistent by the listing is counted at WARNING; its first-point values are used as they are."""
    _completed(state, NAS_A)
    hub.items.return_value = [_item(NAS_A, inconsistent=["owner"])]

    with caplog.at_level(logging.WARNING, logger="ragnart.sync_reconcile"):
        counts = _reconcile(state, hub, present={NAS_A})

    assert counts["inconsistent_items"] == 1
    assert any("disagree" in record.getMessage() for record in caplog.records)


def test_a_clean_source_produces_no_actions(state, hub):
    """Hub and local rows agree: the counts are empty and nothing is sent."""
    _completed(state, NAS_A)
    hub.items.return_value = [_item(NAS_A)]

    counts = _reconcile(state, hub, present={NAS_A})

    assert counts == Counter()
    hub.delete.assert_not_called()
    hub.change_metadata.assert_not_called()


# ---------------------------------------------------------------------------
# Rebuild (step A.10b)
# ---------------------------------------------------------------------------

def _clients(nas_items=(), claude_items=(), opencode_items=()) -> dict:
    """One fake hub per source, each with its own listing."""
    clients = {}
    for source_id, items in (
        (SOURCE_ID_NAS, nas_items),
        (SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, claude_items),
        (SOURCE_ID_OPENCODE_K11, opencode_items),
    ):
        client = MagicMock(spec=HubClient)
        client.items.return_value = list(items)
        clients[source_id] = client
    return clients


def _config(root: Path) -> dict:
    """A config whose one claude root is `root`, as ragnart.yaml describes it."""
    return {"claude_chats": {"roots": [{"path": str(root), "owner": "florian"}]}}


def _transcript(path: Path, text: str, cwd: str = "/home/x/demo") -> Path:
    """A minimal Claude transcript whose one user message is `text`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = [
        {"type": "user", "sessionId": path.stem, "cwd": cwd, "timestamp": "2026-11-18T09:00:00Z",
         "message": {"role": "user", "content": text}},
    ]
    path.write_text("\n".join(json.dumps(entry) for entry in entries) + "\n", encoding="utf-8")
    return path


def test_rebuild_refuses_a_state_that_already_holds_rows_before_reading_the_hub(state, tmp_path):
    """A non-empty rag_state.db is never rebuilt over: the refusal comes before any hub call."""
    _completed(state, NAS_A)
    clients = _clients()

    with pytest.raises(RuntimeError, match="already holds"):
        rebuild_from_hub(state, clients, _config(tmp_path))

    for client in clients.values():
        client.items.assert_not_called()


def test_rebuild_refuses_an_unreadable_claude_root_and_writes_nothing(state, tmp_path):
    """A claude root that is not a directory would make its sessions look gone: the rebuild stops with the state empty."""
    config = _config(tmp_path / "not-mounted")

    with pytest.raises(RuntimeError, match="not accessible"):
        rebuild_from_hub(state, _clients(nas_items=[_item(NAS_A)]), config)

    assert state.get_counts()["total"] == 0


def test_a_failed_hub_listing_leaves_the_state_empty(state, tmp_path):
    """If one source's listing fails, no row was written: the rebuild can run again from scratch."""
    clients = _clients(nas_items=[_item(NAS_A)])
    clients[SOURCE_ID_OPENCODE_K11].items.side_effect = HubError(503, "unavailable")

    with pytest.raises(HubError):
        rebuild_from_hub(state, clients, _config(tmp_path))

    assert state.get_counts()["total"] == 0


def _nas_config(*, user_roots=(), shared_roots=()) -> dict:
    """ragnart.yaml's users and shared sections for the NAS crawl (the roots scan_nfs_roots walks), with no claude roots."""
    return {
        "users": {"owner@example.invalid": {"owner": "florian", "nfs_roots": [str(root) for root in user_roots]}},
        "shared": {"owner": "florian", "nfs_roots": [str(root) for root in shared_roots]},
        "claude_chats": {"roots": []},
    }


@pytest.mark.parametrize("section", ["users", "shared"])
@pytest.mark.parametrize("bad", ["missing", "file"])
def test_rebuild_refuses_an_unreadable_nas_root_and_writes_nothing(state, tmp_path, section, bad):
    """
    A NAS crawl root that is missing, or is a file, would make its files look gone and the next sync would
    tombstone them. The rebuild refuses before the hub is read, with the state empty. Covers the per-user
    roots and the shared roots.
    """
    bad_root = tmp_path / "not-mounted"
    if bad == "file":
        bad_root.write_text("not a directory", encoding="utf-8")
    config = _nas_config(user_roots=[bad_root]) if section == "users" else _nas_config(shared_roots=[bad_root])
    clients = _clients(nas_items=[_item(NAS_A)])

    with pytest.raises(RuntimeError, match="NAS crawl root not readable"):
        rebuild_from_hub(state, clients, config)

    assert state.get_counts()["total"] == 0
    clients[SOURCE_ID_NAS].items.assert_not_called()


def test_rebuild_proceeds_when_every_nas_root_is_a_readable_directory(state, tmp_path):
    """Readable NAS roots do not block the rebuild: nothing is refused, and an empty listing restores no rows."""
    counts = rebuild_from_hub(
        state, _clients(), _nas_config(user_roots=[tmp_path], shared_roots=[tmp_path]), user_filter=None,
    )

    assert counts["rows_written"] == 0


def test_user_filter_checks_only_that_owners_nas_roots(state, tmp_path):
    """With --user, only that owner's roots are walked (as scan_nfs_roots does), so another owner's bad root does not refuse."""
    config = {
        "users": {"owner@example.invalid": {"owner": "someone-else", "nfs_roots": [str(tmp_path / "not-mounted")]}},
        "shared": {"owner": "florian", "nfs_roots": []},
        "claude_chats": {"roots": []},
    }

    counts = rebuild_from_hub(state, _clients(), config, user_filter="florian")

    assert counts["rows_written"] == 0


def test_a_failed_row_write_leaves_the_state_empty_so_the_rebuild_can_run_again(state, tmp_path):
    """
    The rows are written in one transaction. The listing repeats one key, so the second insert fails after the
    first row was written. The whole batch is rolled back, and a retried rebuild is not refused.
    """
    item = _item(NAS_A)  # not on disk: restored as a completed row with last_modified 0
    with pytest.raises(sqlite3.IntegrityError):
        rebuild_from_hub(state, _clients(nas_items=[item, item]), _config(tmp_path))

    assert state.get_counts()["total"] == 0

    counts = rebuild_from_hub(state, _clients(nas_items=[item]), _config(tmp_path))
    assert counts["rows_written"] == 1


def test_nas_file_whose_md5_matches_the_hub_is_restored_with_its_fingerprint_and_current_mtime(state, tmp_path):
    """
    A file on disk whose md5 equals the hub's hash is restored as completed. The row carries the file's current
    mtime (so the scan does not re-queue it), and the size, md5 and hash time of the bytes just hashed.
    """
    path = tmp_path / "note.md"
    path.write_text("hello", encoding="utf-8")
    key = str(path)
    md5 = hash_file(key)[1]

    counts = rebuild_from_hub(
        state, _clients(nas_items=[_item(key, content_hash=md5, chunk_count=3, project_id="proj-a")]),
        _config(tmp_path),
    )

    row = next(r for r in _raw_rows(state) if r["file_path"] == key)
    assert row["status"] == "completed"
    assert row["last_modified"] == os.stat(path).st_mtime
    assert (row["size_bytes"], row["content_hash"]) == (5, md5)
    assert row["hashed_at"] is not None
    assert (row["chunk_count"], row["project_id"]) == (3, "proj-a")
    assert (row["owner"], row["origin_path"], row["source_id"]) == ("florian", key, SOURCE_ID_NAS)
    assert counts["file_on_disk"] == 1
    assert counts["rows_written"] == 1


def test_nas_file_whose_md5_differs_from_the_hub_gets_no_row_so_it_is_submitted_again(state, tmp_path):
    """A file edited since the hub's ingest has no row: the next scan queues it as new and submits it."""
    path = tmp_path / "note.md"
    path.write_text("edited after ingest", encoding="utf-8")

    counts = rebuild_from_hub(
        state, _clients(nas_items=[_item(str(path), content_hash="md5-of-the-old-bytes")]), _config(tmp_path),
    )

    assert state.get_counts()["total"] == 0
    assert counts["file_resubmit"] == 1
    assert counts["rows_written"] == 0


def test_nas_file_not_on_disk_is_restored_with_mtime_zero_to_be_tombstoned(state, tmp_path):
    """A file no longer on disk is restored with last_modified 0 (and no size or hash time), so the next sync tombstones it."""
    counts = rebuild_from_hub(state, _clients(nas_items=[_item(str(tmp_path / "gone.md"))]), _config(tmp_path))

    row = _raw_rows(state)[0]
    assert (row["status"], row["last_modified"]) == ("completed", 0.0)
    assert (row["size_bytes"], row["hashed_at"]) == (None, None)
    assert counts["file_gone"] == 1


def test_item_without_an_owner_is_skipped_and_counted(state, tmp_path):
    """The queue's owner column is NOT NULL, so an item with no owner is not restored, and is counted."""
    counts = rebuild_from_hub(state, _clients(nas_items=[_item(NAS_A, owner=None)]), _config(tmp_path))

    assert state.get_counts()["total"] == 0
    assert counts["skipped_no_owner"] == 1


def test_claude_session_whose_rendered_text_matches_is_restored_with_its_chat_fields(state, tmp_path):
    """The hub hash is the md5 of the rendered text: the session is restored with its origin and launch directory."""
    root = tmp_path / "claude"
    path = _transcript(root / f"{CLAUDE_UUID}.jsonl", "rotate the key", cwd="/home/x/demo/sub")
    text, _metadata, _launch = _parse_claude_chat(path)
    item = _item(CLAUDE_KEY, content_hash=hash_text(text), source_type="claude_session", file_type="claude_chat")

    counts = rebuild_from_hub(state, _clients(claude_items=[item]), _config(root))

    row = _raw_rows(state)[0]
    assert (row["status"], row["origin_path"], row["launch_dir"]) == ("completed", str(path), "/home/x/demo/sub")
    assert row["last_modified"] == os.stat(path).st_mtime
    assert counts["session_restored"] == 1


def test_claude_session_whose_jsonl_bytes_match_is_restored_too(state, tmp_path):
    """A point written before B2 holds the .jsonl md5. The session is restored when the bytes still match."""
    root = tmp_path / "claude"
    path = _transcript(root / f"{CLAUDE_UUID}.jsonl", "hello")
    item = _item(CLAUDE_KEY, content_hash=hash_file(str(path))[1], source_type="claude_session", file_type="claude_chat")

    counts = rebuild_from_hub(state, _clients(claude_items=[item]), _config(root))

    assert counts["session_restored"] == 1
    assert _raw_rows(state)[0]["status"] == "completed"


def test_claude_session_whose_hash_does_not_match_gets_no_row(state, tmp_path):
    """A session edited since the hub's ingest has no row: the scan queues it as new and it is submitted again."""
    root = tmp_path / "claude"
    _transcript(root / f"{CLAUDE_UUID}.jsonl", "edited after ingest")
    item = _item(CLAUDE_KEY, content_hash="not-the-hash", source_type="claude_session", file_type="claude_chat")

    counts = rebuild_from_hub(state, _clients(claude_items=[item]), _config(root))

    assert state.get_counts()["total"] == 0
    assert counts["session_resubmit"] == 1


def test_claude_session_not_on_disk_is_restored_with_mtime_zero_to_be_tombstoned(state, tmp_path):
    """A session with no transcript is restored as completed with last_modified 0 and no origin, to be tombstoned."""
    root = tmp_path / "claude"
    root.mkdir()
    item = _item(CLAUDE_KEY, source_type="claude_session", file_type="claude_chat")

    counts = rebuild_from_hub(state, _clients(claude_items=[item]), _config(root))

    row = _raw_rows(state)[0]
    assert (row["status"], row["last_modified"], row["origin_path"]) == ("completed", 0.0, None)
    assert counts["session_gone"] == 1


def test_opencode_items_are_never_restored_so_the_scan_submits_them(state, tmp_path):
    """An OpenCode item has no checkable hash: it gets no row, and is counted for resubmission."""
    counts = rebuild_from_hub(
        state,
        _clients(opencode_items=[_item(OPENCODE_KEY, source_type="opencode_session", file_type="opencode")]),
        _config(tmp_path),
    )

    assert state.get_counts()["total"] == 0
    assert counts["opencode_resubmit"] == 1


def test_a_rebuilt_state_reconciles_clean_against_the_same_listing(state, tmp_path):
    """Rebuild then reconcile against an unchanged listing: nothing is orphaned, missing, outdated or drifted."""
    root = tmp_path / "claude"
    path = _transcript(root / f"{CLAUDE_UUID}.jsonl", "round trip")
    text, _metadata, _launch = _parse_claude_chat(path)
    file_path = tmp_path / "note.md"
    file_path.write_text("hello", encoding="utf-8")
    nas_item = _item(str(file_path), content_hash=hash_file(str(file_path))[1], project_id="proj-a")
    claude_item = _item(CLAUDE_KEY, content_hash=hash_text(text), source_type="claude_session", file_type="claude_chat")
    clients = _clients(nas_items=[nas_item], claude_items=[claude_item])
    rebuild_from_hub(state, clients, _config(root))

    for source_id, item in ((SOURCE_ID_NAS, nas_item), (SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, claude_item)):
        clients[source_id].items.return_value = [item]
        clients[source_id].info.return_value = {"pipeline_version": "p1"}
    counts = reconcile_source(
        state, clients[SOURCE_ID_NAS], SOURCE_ID_NAS, {str(file_path)}, FRACTION,
    )
    counts += reconcile_source(
        state, clients[SOURCE_ID_CLAUDE_LINUX_SHARED_NFS], SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, {CLAUDE_KEY}, FRACTION,
    )

    assert counts == Counter()


# ---------------------------------------------------------------------------
# claude_session_paths: the scanner's winner rule, read without writing
# ---------------------------------------------------------------------------

def test_claude_session_paths_keeps_the_newest_copy_and_excludes_a_workflow_journal(tmp_path):
    """Two copies of one session: the newer wins. A workflow journal has no key and is not mapped."""
    root = tmp_path / "claude"
    older = _transcript(root / "a" / f"{CLAUDE_UUID}.jsonl", "old copy")
    newer = _transcript(root / "b" / f"{CLAUDE_UUID}.jsonl", "new copy")
    os.utime(older, (MTIME, MTIME))
    os.utime(newer, (MTIME + 100, MTIME + 100))
    _transcript(root / CLAUDE_UUID / "subagents" / "workflows" / "wf_1" / "journal.jsonl", "journal")

    paths = claude_session_paths(_config(root))

    assert paths == {CLAUDE_KEY: (MTIME + 100, str(newer))}


def test_claude_session_paths_refuses_a_root_that_is_not_a_directory(tmp_path):
    """A missing root raises: the caller must not read its absence as every session being gone."""
    with pytest.raises(RuntimeError, match="not accessible"):
        claude_session_paths(_config(tmp_path / "absent"))


def _raw_rows(state: StateDB) -> list[dict]:
    """Every queue row with all its columns, read through a separate connection."""
    conn = sqlite3.connect(state._db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute("SELECT * FROM ingestion_queue ORDER BY file_path")]
    finally:
        conn.close()
