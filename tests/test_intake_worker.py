"""
Tests for ragnart/intake_worker.py, the hub's intake worker (v0.3 B1).

The registry is a real RegistryDB in a temporary directory, so the intake rows, the spool files and
the status transitions are the real ones. Qdrant is a MagicMock, and the embedder is an AsyncMock:
no live service is used and the production lock file is never taken (acquire_or_exit is stubbed in
the lock test).
"""

import asyncio
import hashlib
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from ragnart import ingest, intake_worker, registry as registry_module
from ragnart.ingest import IngestResult
from ragnart.lock import EXIT_LOCK_HELD
from ragnart.registry import RegistryDB
from ragnart.schema import FIELD_FILE_NAME, FIELD_SOURCE_PATH

RAG_CFG = {
    "embedding_model": "bge-m3",
    "admission": {
        "max_text_file_size_bytes": 5_000_000,
        "fffd_ratio_threshold": 0.005,
        "max_chunks_per_item": 5000,
    },
}


@pytest.fixture(autouse=True)
def _no_live_services(monkeypatch):
    """Replace the collection check, the embedder and sparse readiness. Returns the embedder mock."""
    monkeypatch.setattr(ingest, "ensure_collection", MagicMock())
    monkeypatch.setattr(ingest, "_is_sparse_ready", lambda _client: False)
    embed = AsyncMock(side_effect=lambda texts, **_kwargs: [[0.1, 0.2, 0.3, 0.4] for _ in texts])
    monkeypatch.setattr(ingest, "embed_batch", embed)
    return embed


@pytest.fixture
def registry(tmp_path):
    """A writable hub registry with two endpoint sources, MBP (filesystem) and claude-MBP (claude_code)."""
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    db.register_source("MBP", "filesystem", "endpoint", "florian", hashlib.sha256(b"s1").hexdigest())
    db.register_source("claude-MBP", "claude_code", "endpoint", "florian", hashlib.sha256(b"s2").hexdigest())
    yield db
    db.close()


@pytest.fixture
def qdrant():
    """A mocked Qdrant client. Its count is 0 by default: the target key of a move is free."""
    client = MagicMock()
    client.count.return_value = SimpleNamespace(count=0)
    return client


def _spool(tmp_path: Path, request_id: str, data: bytes) -> str:
    """Write uploaded bytes to a spool file named after the request, as the ingestion API does."""
    spool_dir = tmp_path / "spool"
    spool_dir.mkdir(exist_ok=True)
    path = spool_dir / request_id
    path.write_bytes(data)
    return str(path)


def _drain(registry, qdrant, rag_cfg=None) -> int:
    """Run the worker over the queue, once. Returns how many requests it processed."""
    return asyncio.run(intake_worker.drain_intake(registry, rag_cfg or RAG_CFG, qdrant))


def _delete_filter_values(qdrant: MagicMock) -> dict:
    """The {key: value} of the must-conditions of the last delete's filter."""
    selector = qdrant.delete.call_args.kwargs["points_selector"]
    return {condition.key: condition.match.value for condition in selector.filter.must}


def _set_payload_filter_values(qdrant: MagicMock) -> dict:
    """The {key: value} of the must-conditions of the last set_payload's filter."""
    return {condition.key: condition.match.value for condition in qdrant.set_payload.call_args.kwargs["points"].must}


# ---------------------------------------------------------------------------
# Upserts
# ---------------------------------------------------------------------------

class TestUpsert:
    def test_bytes_upsert_completes_with_chunk_count_and_removes_spool(self, registry, qdrant, tmp_path):
        body = b"# Notes\n\nA plain markdown note from the Mac, long enough to chunk.\n"
        request_id = "req-bytes-1"
        spool = _spool(tmp_path, request_id, body)
        registry.insert_intake(
            "MBP", "upsert", "/Users/fotel/MBP/notes.md",
            file_type="md", spool_path=spool, request_id=request_id,
        )

        assert _drain(registry, qdrant) == 1

        row = registry.intake_by_id(request_id)
        assert row["status"] == "completed"
        assert row["chunk_count"] > 0
        assert row["content_hash"] == hashlib.md5(body).hexdigest()
        assert row["pipeline_version"] == "p1"
        assert row["admission_reason"] is None
        assert not Path(spool).exists()

        # The points carry the source's stored id and the source's registered owner.
        points = [p for call in qdrant.upsert.call_args_list for p in call.kwargs["points"]]
        assert len(points) == row["chunk_count"]
        assert points[0].payload["source_id"] == "MBP"
        assert points[0].payload["owner"] == "florian"

    def test_modified_at_from_the_row_is_written_to_every_point(self, registry, qdrant, tmp_path):
        # F1: the endpoint's modification time reaches the points unchanged, for bytes and for text.
        stamp = "2023-11-14T22:13:20+00:00"
        bytes_id = "req-mod-bytes"
        registry.insert_intake(
            "MBP", "upsert", "/Users/fotel/MBP/dated.md", file_type="md",
            spool_path=_spool(tmp_path, bytes_id, b"# Dated\n\nA note with a known modification time.\n"),
            modified_at=stamp, request_id=bytes_id,
        )
        text_id = "req-mod-text"
        registry.insert_intake(
            "claude-MBP", "upsert", "claude-session://dated",
            text="a session with a known modification time. " * 20,
            modified_at=stamp, request_id=text_id,
        )

        _drain(registry, qdrant)

        assert registry.intake_by_id(bytes_id)["status"] == "completed"
        assert registry.intake_by_id(text_id)["status"] == "completed"
        for call in qdrant.upsert.call_args_list:
            for point in call.kwargs["points"]:
                assert point.payload["modified_at"] == stamp

    def test_text_upsert_completes_for_claude_source(self, registry, qdrant):
        request_id = "req-text-1"
        registry.insert_intake(
            "claude-MBP", "upsert", "claude-session://abc",
            text="a rendered session about planning. " * 30,
            metadata={"session_id": "abc", "session_title": "Planning"},
            request_id=request_id,
        )

        _drain(registry, qdrant)

        row = registry.intake_by_id(request_id)
        assert row["status"] == "completed"
        assert row["chunk_count"] > 0
        points = [p for call in qdrant.upsert.call_args_list for p in call.kwargs["points"]]
        assert points[0].payload["source_type"] == "claude_session"
        assert points[0].payload["file_name"] == "Planning"

    def test_ignored_upload_sets_admission_reason_and_removes_spool(self, registry, qdrant, tmp_path):
        request_id = "req-bin-1"
        spool = _spool(tmp_path, request_id, b"text-ish\x00but binary")
        registry.insert_intake(
            "MBP", "upsert", "/Users/fotel/MBP/bin.txt",
            file_type="txt", spool_path=spool, request_id=request_id,
        )

        _drain(registry, qdrant)

        row = registry.intake_by_id(request_id)
        assert row["status"] == "ignored"
        assert row["admission_reason"] == "binary: NUL byte in first 8KB"
        assert row["chunk_count"] == 0
        assert not Path(spool).exists()
        qdrant.upsert.assert_not_called()


# ---------------------------------------------------------------------------
# Failures
# ---------------------------------------------------------------------------

class TestFailures:
    def test_failed_upload_records_error_keeps_spool_and_next_request_still_runs(
        self, registry, qdrant, tmp_path, monkeypatch,
    ):
        # The embedding of the failing upload fails; every other embedding succeeds. The failure is
        # keyed on the upload's text, not on call order: with a worker pool the requests interleave.
        async def flaky_embed(texts, **_kwargs):
            if any("failing upload" in text for text in texts):
                raise RuntimeError("embedder down")
            return [[0.1, 0.2, 0.3, 0.4] for _ in texts]

        monkeypatch.setattr(ingest, "embed_batch", flaky_embed)
        failing_id = "req-fail-1"
        spool = _spool(tmp_path, failing_id, b"plain text body for the failing upload " * 5)
        registry.insert_intake(
            "MBP", "upsert", "/Users/fotel/MBP/fails.txt",
            file_type="txt", spool_path=spool, request_id=failing_id,
        )
        next_id = "req-after-1"
        registry.insert_intake(
            "claude-MBP", "upsert", "claude-session://after",
            text="a later session that must still be processed. " * 20, request_id=next_id,
        )

        assert _drain(registry, qdrant) == 2

        failed = registry.intake_by_id(failing_id)
        assert failed["status"] == "failed"
        assert failed["error"] == "embedder down"
        assert Path(spool).exists()  # kept for diagnosis
        assert registry.intake_by_id(next_id)["status"] == "completed"

    def test_error_text_is_cut_to_500_characters(self, registry, qdrant):
        request_id = "req-long-error"
        registry.insert_intake("MBP", "delete", "/a.md", request_id=request_id)
        qdrant.delete.side_effect = RuntimeError("e" * 900)

        _drain(registry, qdrant)

        row = registry.intake_by_id(request_id)
        assert row["status"] == "failed"
        assert len(row["error"]) == 500

    def test_unknown_source_fails_the_request(self, registry, qdrant):
        request_id = "req-ghost"
        registry.insert_intake("ghost", "delete", "/a.md", request_id=request_id)

        _drain(registry, qdrant)

        row = registry.intake_by_id(request_id)
        assert row["status"] == "failed"
        assert row["error"] == "unknown source"
        qdrant.delete.assert_not_called()


# ---------------------------------------------------------------------------
# Key and metadata changes (no re-embedding)
# ---------------------------------------------------------------------------

class TestDeleteAndChanges:
    def test_delete_filters_by_source_and_key(self, registry, qdrant):
        request_id = "req-del-1"
        registry.insert_intake("MBP", "delete", "/Users/fotel/MBP/a.md", request_id=request_id)

        _drain(registry, qdrant)

        assert registry.intake_by_id(request_id)["status"] == "completed"
        qdrant.delete.assert_called_once()
        assert qdrant.delete.call_args.kwargs["wait"] is True
        # Exactly the two conditions. A sibling source holding the same key is not matched.
        assert _delete_filter_values(qdrant) == {"source_id": "MBP", FIELD_SOURCE_PATH: "/Users/fotel/MBP/a.md"}

    def test_change_key_rewrites_key_fields_without_embedding(self, registry, qdrant, _no_live_services):
        request_id = "req-move-1"
        registry.insert_intake(
            "MBP", "change_key", "/Users/fotel/MBP/a.md",
            new_key="/Users/fotel/MBP/sub/b.md", request_id=request_id,
        )

        _drain(registry, qdrant)

        assert registry.intake_by_id(request_id)["status"] == "completed"
        qdrant.set_payload.assert_called_once()
        call = qdrant.set_payload.call_args
        assert call.kwargs["payload"] == {
            FIELD_SOURCE_PATH: "/Users/fotel/MBP/sub/b.md",
            FIELD_FILE_NAME: "b.md",
        }
        assert _set_payload_filter_values(qdrant) == {"source_id": "MBP", FIELD_SOURCE_PATH: "/Users/fotel/MBP/a.md"}
        assert call.kwargs["wait"] is True
        # The vectors are kept: nothing is embedded, nothing is re-inserted.
        _no_live_services.assert_not_called()
        qdrant.upsert.assert_not_called()
        qdrant.delete.assert_not_called()

    def test_change_key_onto_a_key_with_points_fails_and_changes_nothing(self, registry, qdrant):
        # F3: the count of the target key is exact; any point there refuses the move before set_payload.
        qdrant.count.return_value = SimpleNamespace(count=1)
        request_id = "req-collide-1"
        registry.insert_intake(
            "MBP", "change_key", "/Users/fotel/MBP/a.md",
            new_key="/Users/fotel/MBP/taken.md", request_id=request_id,
        )

        _drain(registry, qdrant)

        row = registry.intake_by_id(request_id)
        assert row["status"] == "failed"
        assert row["error"] == "target key already has points"
        qdrant.set_payload.assert_not_called()
        # The count looks at the target key of this source only, with exact=True.
        assert qdrant.count.call_args.kwargs["exact"] is True
        count_filter = qdrant.count.call_args.kwargs["count_filter"]
        assert {c.key: c.match.value for c in count_filter.must} == {
            "source_id": "MBP", FIELD_SOURCE_PATH: "/Users/fotel/MBP/taken.md",
        }

    def test_change_metadata_sets_owner_on_the_items_points(self, registry, qdrant):
        request_id = "req-owner-1"
        registry.insert_intake(
            "MBP", "change_metadata", "/Users/fotel/MBP/a.md",
            metadata={"owner": "eva"}, request_id=request_id,
        )

        _drain(registry, qdrant)

        assert registry.intake_by_id(request_id)["status"] == "completed"
        assert qdrant.set_payload.call_args.kwargs["payload"] == {"owner": "eva"}
        assert _set_payload_filter_values(qdrant) == {"source_id": "MBP", FIELD_SOURCE_PATH: "/Users/fotel/MBP/a.md"}

    def test_change_metadata_refuses_fields_other_than_owner(self, registry, qdrant):
        # A source_id change in a row would move points to another source. The worker refuses it.
        request_id = "req-bad-meta"
        registry.insert_intake(
            "MBP", "change_metadata", "/Users/fotel/MBP/a.md",
            metadata={"source_id": "nas"}, request_id=request_id,
        )

        _drain(registry, qdrant)

        row = registry.intake_by_id(request_id)
        assert row["status"] == "failed"
        assert "not changeable" in row["error"]
        qdrant.set_payload.assert_not_called()


# ---------------------------------------------------------------------------
# Order and the lock
# ---------------------------------------------------------------------------

class TestOrderAndLock:
    def test_requests_are_processed_oldest_created_at_first(self, registry, qdrant, monkeypatch):
        # Insert in the order k1, k2, k3, but give them created_at times that put k2 first, then k3, then k1.
        times = iter([
            "2026-11-19T00:00:03.000000+00:00",  # k1 is the newest
            "2026-11-19T00:00:01.000000+00:00",  # k2 is the oldest
            "2026-11-19T00:00:02.000000+00:00",  # k3 is in between
        ])
        real_now = registry_module._now
        monkeypatch.setattr(registry_module, "_now", lambda: next(times))
        for key in ("k1", "k2", "k3"):
            registry.insert_intake("MBP", "delete", key, request_id=f"req-{key}")
        # Put back the real clock before the worker runs, so its own status updates use it.
        monkeypatch.setattr(registry_module, "_now", real_now)

        _drain(registry, qdrant)

        deleted_keys = [call.kwargs["points_selector"].filter.must[1].match.value
                        for call in qdrant.delete.call_args_list]
        assert deleted_keys == ["k2", "k3", "k1"]

    def test_lock_held_exits_75_and_no_request_is_touched(self, registry, monkeypatch):
        # The lock is refused, as the real acquire_or_exit does on contention.
        def refuse_lock(lock_path: str) -> None:
            sys.exit(EXIT_LOCK_HELD)

        request_id = "req-untouched"
        registry.insert_intake("MBP", "delete", "/a.md", request_id=request_id)
        monkeypatch.setattr(intake_worker, "load_config", lambda: {"rag": {"ingest_lock": "/unused", "qdrant_url": "u"}})
        monkeypatch.setattr(intake_worker, "acquire_or_exit", refuse_lock)
        opened = MagicMock(return_value=registry)
        monkeypatch.setattr(intake_worker, "open_registry", opened)

        with pytest.raises(SystemExit) as exc_info:
            intake_worker.main()

        assert exc_info.value.code == EXIT_LOCK_HELD
        opened.assert_not_called()
        assert registry.intake_by_id(request_id)["status"] == "received"

    def test_orphaned_processing_rows_are_requeued_and_processed_under_the_lock(self, registry, qdrant, monkeypatch, caplog):
        # F5: a row left 'processing' by a crashed run is set back to 'received' by main(), then processed.
        request_id = "req-orphan-1"
        registry.insert_intake("MBP", "delete", "/Users/fotel/MBP/orphan.md", request_id=request_id)
        registry.update_intake(request_id, status="processing")
        monkeypatch.setattr(intake_worker, "load_config", lambda: {"rag": {"ingest_lock": "/unused", "qdrant_url": "u"}})
        monkeypatch.setattr(intake_worker, "acquire_or_exit", lambda _path: None)
        monkeypatch.setattr(intake_worker, "open_registry", lambda _config: registry)
        monkeypatch.setattr(intake_worker, "get_client", lambda _url: qdrant)
        monkeypatch.setattr(intake_worker, "ensure_collection", lambda _client: None)
        # main() closes the registry when it ends; keep it open so the test can read the row.
        monkeypatch.setattr(registry, "close", lambda: None)

        with caplog.at_level("WARNING", logger="ragnart.intake_worker"):
            assert intake_worker.main() == 0

        assert registry.intake_by_id(request_id)["status"] == "completed"
        assert any("1 request(s) left processing" in r.getMessage() for r in caplog.records)

    def test_reset_processing_intake_counts_and_refreshes_only_processing_rows(self, registry):
        orphan_id, waiting_id = "req-reset-a", "req-reset-b"
        registry.insert_intake("MBP", "delete", "/a.md", request_id=orphan_id)
        registry.insert_intake("MBP", "delete", "/b.md", request_id=waiting_id)
        registry.update_intake(orphan_id, status="processing")
        before = registry.intake_by_id(orphan_id)["updated_at"]

        assert registry.reset_processing_intake() == 1

        orphan = registry.intake_by_id(orphan_id)
        assert orphan["status"] == "received"
        assert orphan["updated_at"] > before
        assert registry.intake_by_id(waiting_id)["status"] == "received"
        assert registry.reset_processing_intake() == 0

    def test_empty_queue_exits_zero(self, registry, monkeypatch):
        monkeypatch.setattr(intake_worker, "load_config", lambda: {"rag": {"ingest_lock": "/unused", "qdrant_url": "u"}})
        monkeypatch.setattr(intake_worker, "acquire_or_exit", lambda _path: None)
        monkeypatch.setattr(intake_worker, "open_registry", lambda _config: registry)
        monkeypatch.setattr(intake_worker, "get_client", lambda _url: MagicMock())
        monkeypatch.setattr(intake_worker, "ensure_collection", lambda _client: None)

        assert intake_worker.main() == 0


# ---------------------------------------------------------------------------
# Per-item fields: owner, tag and project_id reach ingest_content (session B2, A.4)
# ---------------------------------------------------------------------------

class TestPerItemFields:
    def test_bytes_upsert_on_a_no_fixed_owner_source_writes_the_rows_owner_tag_and_project(
        self, registry, qdrant, tmp_path,
    ):
        # nas has no fixed owner: the row names the item's owner, and carries its tag and project.
        registry.upsert_project("proj-1", "/mnt/nfs/Florian/proj", "My Project", "florian", "nas")
        request_id = "req-nas-fields"
        spool = _spool(tmp_path, request_id, b"# Certificate\n\nA note about a certificate course.\n")
        registry.insert_intake(
            "nas", "upsert", "/mnt/nfs/Florian/proj/cert.md",
            file_type="md", spool_path=spool, request_id=request_id,
            owner="florian", tag="certifications", project_id="proj-1",
        )

        assert _drain(registry, qdrant) == 1

        assert registry.intake_by_id(request_id)["status"] == "completed"
        points = [p for call in qdrant.upsert.call_args_list for p in call.kwargs["points"]]
        assert points
        for point in points:
            assert point.payload["owner"] == "florian"
            assert point.payload["tag"] == "certifications"
            assert point.payload["project_id"] == "proj-1"
            assert point.payload["source_id"] == "nas"

    def test_missing_row_fields_fall_back_to_the_source_owner_no_tag_and_global(self, registry, qdrant):
        # A fixed-owner source (MBP) sends no owner, tag or project: the points get the source's owner,
        # an empty tag and the global project, as B1 wrote them.
        request_id = "req-fallback"
        registry.insert_intake(
            "claude-MBP", "upsert", "claude-session://fallback",
            text="a session that names no owner, tag or project. " * 20, request_id=request_id,
        )

        _drain(registry, qdrant)

        assert registry.intake_by_id(request_id)["status"] == "completed"
        points = [p for call in qdrant.upsert.call_args_list for p in call.kwargs["points"]]
        assert points[0].payload["owner"] == "florian"
        assert points[0].payload["tag"] == ""
        assert points[0].payload["project_id"] == "global"

    def test_opencode_text_upsert_is_stored_as_an_opencode_session_without_a_hash(self, registry, qdrant):
        # The connector of the source (opencode) reaches ingest_content: the opencode type, no hash.
        request_id = "req-oc-1"
        registry.insert_intake(
            "opencode-k11", "upsert", "opencode://ses_abc",
            text="an opencode session rendered by the sender. " * 20,
            metadata={"session_id": "ses_abc", "session_title": "Planning"},
            owner="florian", request_id=request_id,
        )

        _drain(registry, qdrant)

        row = registry.intake_by_id(request_id)
        assert row["status"] == "completed"
        assert row["content_hash"] is None
        points = [p for call in qdrant.upsert.call_args_list for p in call.kwargs["points"]]
        assert points[0].payload["source_type"] == "opencode_session"
        assert points[0].payload["file_type"] == "opencode"
        assert points[0].payload["file_name"] == "Planning"
        assert points[0].payload["content_hash"] is None

    def test_change_metadata_project_id_is_set_on_the_items_points(self, registry, qdrant):
        registry.upsert_project("proj-1", "/Users/fotel/MBP/proj", "My Project", "florian", "MBP")
        request_id = "req-proj-1"
        registry.insert_intake(
            "MBP", "change_metadata", "/Users/fotel/MBP/a.md",
            metadata={"project_id": "proj-1"}, request_id=request_id,
        )

        _drain(registry, qdrant)

        assert registry.intake_by_id(request_id)["status"] == "completed"
        assert qdrant.set_payload.call_args.kwargs["payload"] == {"project_id": "proj-1"}
        assert _set_payload_filter_values(qdrant) == {"source_id": "MBP", FIELD_SOURCE_PATH: "/Users/fotel/MBP/a.md"}


# ---------------------------------------------------------------------------
# The worker pool (session B2, A.4b)
# ---------------------------------------------------------------------------

def _fake_result() -> IngestResult:
    """An ingest_content result for a request the fake applied."""
    return IngestResult("completed", None, 1, None, "p1")


class TestWorkerPool:
    def test_requests_for_the_same_key_never_overlap_and_keep_their_order(self, registry, qdrant, monkeypatch):
        # Three upserts of one item, with the work held open for a moment so that a second worker would
        # overlap the first if it were allowed to. The recorded events must be strictly start/end pairs.
        events: list[tuple[str, str]] = []

        async def slow_ingest(**kwargs):
            events.append(("start", kwargs["text"]))
            await asyncio.sleep(0.02)
            events.append(("end", kwargs["text"]))
            return _fake_result()

        monkeypatch.setattr(intake_worker, "ingest_content", slow_ingest)
        for text in ("first", "second", "third"):
            registry.insert_intake(
                "claude-MBP", "upsert", "claude-session://same-item", text=text,
            )

        # The timeout keeps a regression from hanging the suite.
        asyncio.run(asyncio.wait_for(intake_worker.drain_intake(registry, RAG_CFG, qdrant, workers=3), timeout=20))

        assert events == [
            ("start", "first"), ("end", "first"),
            ("start", "second"), ("end", "second"),
            ("start", "third"), ("end", "third"),
        ]

    def test_requests_for_different_keys_run_in_parallel(self, registry, qdrant, monkeypatch):
        # Two different items: each ingest waits at a barrier that only opens when both are in flight.
        # Run one after the other, the first would wait out its timeout and fail the request.
        async def run() -> None:
            barrier = asyncio.Barrier(2)

            async def meet_at_barrier(**_kwargs):
                await asyncio.wait_for(barrier.wait(), timeout=5)
                return _fake_result()

            monkeypatch.setattr(intake_worker, "ingest_content", meet_at_barrier)
            first = registry.insert_intake("claude-MBP", "upsert", "claude-session://item-a", text="a " * 30)
            second = registry.insert_intake("claude-MBP", "upsert", "claude-session://item-b", text="b " * 30)
            processed = await asyncio.wait_for(
                intake_worker.drain_intake(registry, RAG_CFG, qdrant, workers=2), timeout=20,
            )
            assert processed == 2
            assert registry.intake_by_id(first)["status"] == "completed"
            assert registry.intake_by_id(second)["status"] == "completed"

        asyncio.run(run())

    def test_every_request_is_applied_exactly_once_across_the_pool(self, registry, qdrant, monkeypatch):
        # Twenty requests, one per item, through four workers: each item is ingested once, and none is skipped.
        # This is the pool property the daemon's pool test checked for files.
        applied: list[str] = []

        async def record(**kwargs):
            applied.append(kwargs["key"])
            await asyncio.sleep(0.001)
            return _fake_result()

        monkeypatch.setattr(intake_worker, "ingest_content", record)
        keys = [f"claude-session://item-{index:02d}" for index in range(20)]
        for key in keys:
            registry.insert_intake("claude-MBP", "upsert", key, text="a " * 30)

        processed = asyncio.run(
            asyncio.wait_for(intake_worker.drain_intake(registry, RAG_CFG, qdrant, workers=4), timeout=20),
        )

        assert processed == 20
        assert sorted(applied) == sorted(keys)
        assert len(applied) == len(set(applied))

    def test_a_change_key_to_b_is_applied_before_a_later_upsert_of_b(self, registry, qdrant, tmp_path):
        # The change_key moves A to B. The later upsert of B is held back until the move is done, so the
        # move's set_payload is recorded before the upsert's points are written.
        registry.insert_intake(
            "MBP", "change_key", "/Users/fotel/MBP/a.md", new_key="/Users/fotel/MBP/b.md", request_id="req-cb-move",
        )
        spool = _spool(tmp_path, "req-cb-upsert", b"# B\n\nThe note that lands on key B after the move.\n")
        registry.insert_intake(
            "MBP", "upsert", "/Users/fotel/MBP/b.md", file_type="md",
            spool_path=spool, request_id="req-cb-upsert",
        )

        assert _drain(registry, qdrant) == 2

        assert registry.intake_by_id("req-cb-move")["status"] == "completed"
        assert registry.intake_by_id("req-cb-upsert")["status"] == "completed"
        names = [name for name, _args, _kwargs in qdrant.mock_calls]
        assert names.index("set_payload") < names.index("upsert")

    def test_a_worker_pool_must_have_at_least_one_worker(self, registry, qdrant):
        with pytest.raises(ValueError, match="at least one worker"):
            asyncio.run(intake_worker.drain_intake(registry, RAG_CFG, qdrant, workers=0))

    def test_workers_flag_defaults_to_three_and_can_be_set(self):
        assert intake_worker._parse_args([]).workers == 3
        assert intake_worker._parse_args(["--workers", "5"]).workers == 5


# ---------------------------------------------------------------------------
# The claim rule on its own: next_intake() holds back a request behind an earlier one (session B2, A.4b)
# ---------------------------------------------------------------------------

class TestClaimHoldsBackLaterRequests:
    def test_a_later_request_for_the_same_key_is_not_claimable_while_an_earlier_one_is_processing(self, registry):
        registry.insert_intake("MBP", "delete", "/a.md", request_id="req-h1")
        registry.insert_intake("MBP", "delete", "/a.md", request_id="req-h2")

        assert registry.next_intake()["id"] == "req-h1"
        # req-h2 has the same key and an earlier request is still processing: nothing is claimable.
        assert registry.next_intake() is None
        assert registry.intake_by_id("req-h2")["status"] == "received"

        registry.update_intake("req-h1", status="completed")
        assert registry.next_intake()["id"] == "req-h2"

    def test_the_earlier_of_two_same_key_requests_is_claimed_first_even_when_both_are_received(self, registry):
        registry.insert_intake("MBP", "delete", "/a.md", request_id="req-e1")
        registry.insert_intake("MBP", "upsert", "/a.md", request_id="req-e2", text="later")

        assert registry.next_intake()["id"] == "req-e1"

    def test_a_change_key_holds_back_a_later_request_for_its_new_key(self, registry):
        # r1 moves A to B and is processing. r2 upserts B: held. r3 deletes A: also held (A is r1's key).
        registry.insert_intake("MBP", "change_key", "/a.md", new_key="/b.md", request_id="req-n1")
        registry.insert_intake("MBP", "delete", "/b.md", request_id="req-n2")
        registry.insert_intake("MBP", "delete", "/a.md", request_id="req-n3")

        assert registry.next_intake()["id"] == "req-n1"
        assert registry.next_intake() is None

        registry.update_intake("req-n1", status="completed")
        # Once the move is done, r2 (key B) is claimable. r3 (key A) is free too, since r1 no longer holds A.
        assert registry.next_intake()["id"] == "req-n2"
        assert registry.next_intake()["id"] == "req-n3"

    def test_a_request_for_a_key_held_by_a_change_key_source_is_held_by_its_new_key_too(self, registry):
        # The other direction: an earlier upsert of B holds back a later change_key from A to B.
        registry.insert_intake("MBP", "delete", "/b.md", request_id="req-x1")
        registry.insert_intake("MBP", "change_key", "/a.md", new_key="/b.md", request_id="req-x2")

        assert registry.next_intake()["id"] == "req-x1"
        assert registry.next_intake() is None

    def test_keys_are_compared_case_sensitively(self, registry):
        registry.insert_intake("MBP", "delete", "/a/B.md", request_id="req-c1")
        registry.insert_intake("MBP", "delete", "/a/b.md", request_id="req-c2")

        assert registry.next_intake()["id"] == "req-c1"
        assert registry.next_intake()["id"] == "req-c2"

    def test_source_ids_are_compared_case_insensitively(self, registry):
        registry.insert_intake("MBP", "delete", "/a.md", request_id="req-s1")
        registry.insert_intake("mbp", "delete", "/a.md", request_id="req-s2")

        assert registry.next_intake()["id"] == "req-s1"
        assert registry.next_intake() is None

    def test_other_sources_do_not_hold_back_a_request(self, registry):
        registry.insert_intake("MBP", "delete", "/a.md", request_id="req-o1")
        registry.insert_intake("nas", "delete", "/a.md", request_id="req-o2")

        assert registry.next_intake()["id"] == "req-o1"
        assert registry.next_intake()["id"] == "req-o2"

    def test_a_later_request_for_a_key_with_no_earlier_request_is_claimed(self, registry):
        # The rule holds requests back only behind an earlier request of the same item.
        registry.insert_intake("MBP", "delete", "/a.md", request_id="req-f1")
        registry.insert_intake("MBP", "delete", "/b.md", request_id="req-f2")

        assert registry.next_intake()["id"] == "req-f1"
        assert registry.next_intake()["id"] == "req-f2"
