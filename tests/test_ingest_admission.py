"""
Tests for the ingestion admission gate and the docling CPU accelerator (plan steps 7-8).

No live services are used. StateDB and the Qdrant client are mocks, the Ollama embedding
call is replaced by an AsyncMock, and docling conversion is replaced where it would run.
"""

import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from docling.datamodel.accelerator_options import AcceleratorDevice
from docling.datamodel.base_models import InputFormat
from docling.document_converter import DocumentConverter

from ragnart import ingest
from ragnart.schema import (
    FIELD_CONTENT_HASH,
    FIELD_INGESTED_AT,
    FIELD_MODIFIED_AT,
    FIELD_PROJECT_ID,
    FIELD_SOURCE_TYPE,
    GLOBAL_PROJECT_ID,
)
from ragnart.state import StateDB


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _no_live_services(monkeypatch):
    """
    Replace every call that would reach Qdrant or Ollama.

    Returns the embedding mock so a test can assert that embedding was never reached.
    """
    monkeypatch.setattr(ingest, "ensure_collection", MagicMock())
    embed = AsyncMock(return_value=[])
    monkeypatch.setattr(ingest, "embed_batch", embed)
    return embed


def _run_ingest(file_path: Path, rag_cfg: dict | None = None) -> MagicMock:
    """
    Run ingest_file() on one path with a mocked StateDB and return that mock.

    The Qdrant client is a plain MagicMock, so its delete/upsert calls are recorded
    but never sent anywhere.
    """
    state_db = MagicMock(spec=StateDB)
    qdrant_client = MagicMock()
    asyncio.run(
        ingest.ingest_file(
            str(file_path),
            owner="tester",
            rag_cfg=rag_cfg or {},
            state_db=state_db,
            qdrant_client=qdrant_client,
        )
    )
    return state_db


# ---------------------------------------------------------------------------
# Step 7a: no raw-read fallback for docling types
# ---------------------------------------------------------------------------

class TestDoclingFailureHandling:
    """A docling failure on PDF or DOCX must raise. It must never fall back to a raw read."""

    @pytest.mark.parametrize("suffix", ["pdf", "docx"])
    def test_parse_raises_runtime_error_with_cause(self, monkeypatch, tmp_path, suffix):
        # Arrange: docling conversion fails for this file.
        original = ValueError("corrupt container")
        converter = MagicMock()
        converter.convert.side_effect = original
        monkeypatch.setattr(ingest, "_get_converter", lambda: converter)

        # Act / Assert: the error is re-raised as RuntimeError, chained to the original.
        with pytest.raises(RuntimeError, match="docling failed:") as excinfo:
            ingest._parse_to_text(str(tmp_path / f"doc.{suffix}"), suffix)
        assert excinfo.value.__cause__ is original

    @pytest.mark.parametrize("suffix", ["pdf", "docx"])
    def test_ingest_marks_failed_not_ignored(self, monkeypatch, tmp_path, suffix):
        # Arrange: a real file on disk, docling conversion fails.
        file_path = tmp_path / f"doc.{suffix}"
        file_path.write_bytes(b"\x00\x01binary-looking payload")
        converter = MagicMock()
        converter.convert.side_effect = ValueError("corrupt container")
        monkeypatch.setattr(ingest, "_get_converter", lambda: converter)

        # Act: ingest_file re-raises after recording the failure.
        with pytest.raises(RuntimeError, match="docling failed:"):
            _run_ingest(file_path)

    def test_ingest_failure_recorded_via_mark_failed(self, monkeypatch, tmp_path):
        # Arrange
        file_path = tmp_path / "doc.pdf"
        file_path.write_bytes(b"%PDF-1.4 not really")
        converter = MagicMock()
        converter.convert.side_effect = ValueError("corrupt container")
        monkeypatch.setattr(ingest, "_get_converter", lambda: converter)

        # Act
        state_db = MagicMock(spec=StateDB)
        with pytest.raises(RuntimeError):
            asyncio.run(
                ingest.ingest_file(
                    str(file_path), owner="tester", rag_cfg={},
                    state_db=state_db, qdrant_client=MagicMock(),
                )
            )

        # Assert: the failure goes through mark_failed, not mark_ignored.
        state_db.mark_failed.assert_called_once()
        assert "docling failed:" in state_db.mark_failed.call_args.args[1]
        state_db.mark_ignored.assert_not_called()

    def test_pptx_docling_failure_still_uses_python_pptx(self, monkeypatch, tmp_path):
        # PPTX keeps its python-pptx fallback. This guards against the change above
        # removing that path by mistake.
        converter = MagicMock()
        converter.convert.side_effect = ValueError("format not detected")
        monkeypatch.setattr(ingest, "_get_converter", lambda: converter)
        monkeypatch.setattr(ingest, "_parse_pptx", lambda path: "Slide 1\nfallback text")

        result = ingest._parse_to_text(str(tmp_path / "deck.pptx"), "pptx")

        assert result == "Slide 1\nfallback text"


# ---------------------------------------------------------------------------
# Step 7b: admission for text-like files and the chunk cap
# ---------------------------------------------------------------------------

class TestAdmission:

    def test_size_cap_rejects_before_reading(self, monkeypatch, tmp_path):
        # Arrange: a 100-byte file against a 10-byte cap.
        file_path = tmp_path / "big.txt"
        file_path.write_bytes(b"a" * 100)
        parse = MagicMock(return_value="unused")
        monkeypatch.setattr(ingest, "_parse_to_text", parse)

        # Act
        state_db = _run_ingest(file_path, {"admission": {"max_text_file_size_bytes": 10}})

        # Assert: rejected with the byte numbers, and the file is never parsed (read).
        state_db.mark_ignored.assert_called_once_with(
            str(file_path), "too large: 100 bytes > 10 byte cap",
        )
        parse.assert_not_called()

    def test_nul_byte_in_first_window_is_rejected_before_parse(self, monkeypatch, tmp_path):
        # Arrange: a text-like file with a NUL byte near the start.
        file_path = tmp_path / "blob.txt"
        file_path.write_bytes(b"hello\x00world" + b"x" * 100)
        parse = MagicMock(return_value="unused")
        monkeypatch.setattr(ingest, "_parse_to_text", parse)

        # Act
        state_db = _run_ingest(file_path)

        # Assert: marked ignored with a NUL reason, and parsing never ran.
        state_db.mark_ignored.assert_called_once()
        reason = state_db.mark_ignored.call_args.args[1]
        assert "NUL" in reason
        parse.assert_not_called()

    def test_replacement_char_ratio_over_threshold_is_rejected(self, monkeypatch, tmp_path):
        # Arrange: 10 U+FFFD in 1010 characters is about 0.99%, above the 0.5% default.
        # The printable gate would still pass this (FFFD is 1% of the text), so the
        # ratio check is what must reject it.
        file_path = tmp_path / "mojibake.txt"
        file_path.write_text("�" * 10 + "a" * 1000, encoding="utf-8")
        select = MagicMock(return_value=([], "flat"))
        monkeypatch.setattr(ingest, "_select_chunks", select)

        # Act
        state_db = _run_ingest(file_path)

        # Assert
        state_db.mark_ignored.assert_called_once()
        assert "replacement chars" in state_db.mark_ignored.call_args.args[1]
        select.assert_not_called()

    def test_replacement_char_ratio_under_threshold_proceeds_to_chunking(self, monkeypatch, tmp_path):
        # Arrange: 1 U+FFFD in 1000 characters is 0.1%, below the 0.5% default.
        file_path = tmp_path / "almost-clean.txt"
        file_path.write_text("�" + "a" * 999, encoding="utf-8")
        # Empty chunk list ends the run right after chunking, before any embedding.
        select = MagicMock(return_value=([], "flat"))
        monkeypatch.setattr(ingest, "_select_chunks", select)

        # Act
        state_db = _run_ingest(file_path)

        # Assert: the file reached chunking and was not rejected by the ratio check.
        select.assert_called_once()
        state_db.mark_ignored.assert_not_called()
        state_db.mark_completed.assert_called_once_with(str(file_path), chunk_count=0, pipeline_version="p1")

    def test_chunk_cap_rejects_before_embedding(self, monkeypatch, tmp_path, _no_live_services):
        # Arrange: three chunks against a cap of one.
        file_path = tmp_path / "many.txt"
        file_path.write_text("some text", encoding="utf-8")
        three_chunks = [("a", "a"), ("b", "b"), ("c", "c")]
        monkeypatch.setattr(ingest, "_select_chunks", MagicMock(return_value=(three_chunks, "flat")))

        # Act
        state_db = _run_ingest(file_path, {"admission": {"max_chunks_per_item": 1}})

        # Assert: rejected with the actual numbers, and no embedding call was made.
        state_db.mark_ignored.assert_called_once_with(
            str(file_path), "too large: 3 chunks > 1 chunk cap",
        )
        _no_live_services.assert_not_awaited()

    def test_chunk_cap_applies_to_docling_types(self, monkeypatch, tmp_path, _no_live_services):
        # The chunk cap covers all file types, not only text-like ones.
        file_path = tmp_path / "report.docx"
        file_path.write_bytes(b"placeholder")
        converter = MagicMock()
        converter.convert.return_value.document.export_to_markdown.return_value = "markdown body"
        monkeypatch.setattr(ingest, "_get_converter", lambda: converter)
        three_chunks = [("a", "a"), ("b", "b"), ("c", "c")]
        monkeypatch.setattr(ingest, "_select_chunks", MagicMock(return_value=(three_chunks, "flat")))

        state_db = _run_ingest(file_path, {"admission": {"max_chunks_per_item": 2}})

        state_db.mark_ignored.assert_called_once_with(
            str(file_path), "too large: 3 chunks > 2 chunk cap",
        )
        _no_live_services.assert_not_awaited()


# ---------------------------------------------------------------------------
# Step 8: explicit CPU accelerator for docling
# ---------------------------------------------------------------------------

class TestDoclingCpuAccelerator:

    def test_converter_constructs_without_raising(self):
        converter = ingest._build_converter()
        assert isinstance(converter, DocumentConverter)

    @pytest.mark.parametrize(
        "input_format", [InputFormat.PDF, InputFormat.DOCX, InputFormat.PPTX],
    )
    def test_every_docling_format_is_pinned_to_cpu(self, input_format):
        # Introspect the converter's own config rather than trusting the builder code.
        converter = ingest._build_converter()
        pipeline_options = converter.format_to_options[input_format].pipeline_options

        assert pipeline_options.accelerator_options.device == AcceleratorDevice.CPU


# ---------------------------------------------------------------------------
# Step 10: claude-session dispatch, origin_path reads, launch_dir and new payload fields
# ---------------------------------------------------------------------------

def _write_claude_transcript(path: Path, cwd: str) -> Path:
    """
    Write a minimal Claude Code session JSONL with one user and one assistant text turn.

    The file sits under a `projects/` directory, as real transcripts do. The parser
    finds the .claude directory from that component, so a bare tmp_path would fail.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = [
        {
            "type": "user", "sessionId": "abc", "cwd": cwd,
            "message": {"role": "user", "content": "How do I rotate the backup key?"},
        },
        {
            "type": "assistant", "sessionId": "abc", "cwd": cwd,
            "message": {
                "role": "assistant", "model": "test-model",
                "content": [{"type": "text", "text": "Run the rotate command."}],
            },
        },
    ]
    path.write_text("\n".join(json.dumps(entry) for entry in entries) + "\n", encoding="utf-8")
    return path


def _one_vector_per_text(child_texts, **_kwargs):
    """Stand-in for embed_batch: one dummy vector per child text, so points are built."""
    return [[0.0, 0.0] for _ in child_texts]


class TestClaudeSessionDispatch:
    """claude-session:// keys are read from origin_path, and every new point carries the v0.2 fields."""

    def test_is_text_like_path_excludes_claude_session_prefix(self):
        assert ingest.is_text_like_path("claude-session://abc") is False

    def test_claude_session_without_origin_path_raises(self):
        state_db = MagicMock(spec=StateDB)

        with pytest.raises(RuntimeError, match="no origin_path"):
            asyncio.run(
                ingest.ingest_file(
                    file_path="claude-session://abc",
                    owner="tester",
                    rag_cfg={},
                    state_db=state_db,
                    qdrant_client=MagicMock(),
                )
            )

        state_db.mark_failed.assert_called_once()

    def test_claude_session_reads_from_origin_path(self, tmp_path):
        # The key has no file behind it. The transcript at origin_path must be parsed.
        transcript = _write_claude_transcript(
            tmp_path / ".claude" / "projects" / "-x" / "abc.jsonl", cwd="/x",
        )
        state_db = MagicMock(spec=StateDB)

        asyncio.run(
            ingest.ingest_file(
                file_path="claude-session://abc",
                origin_path=str(transcript),
                owner="tester",
                rag_cfg={},
                state_db=state_db,
                qdrant_client=MagicMock(),
            )
        )

        state_db.mark_failed.assert_not_called()
        state_db.mark_completed.assert_called_once()
        assert state_db.mark_completed.call_args.args[0] == "claude-session://abc"

    def test_zero_turn_claude_session_marks_p1_with_zero_chunks(self, tmp_path):
        # A transcript with no user or assistant turn parses to empty text, so it takes the
        # "Empty document" early exit. That row must carry pipeline_version="p1" so the
        # metadata backfill never reclassifies it as a nas row.
        transcript = tmp_path / ".claude" / "projects" / "-x" / "abc.jsonl"
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text(
            json.dumps({"type": "summary", "summary": "nothing said", "leafUuid": "abc"}) + "\n",
            encoding="utf-8",
        )
        state_db = MagicMock(spec=StateDB)

        asyncio.run(
            ingest.ingest_file(
                file_path="claude-session://abc",
                origin_path=str(transcript),
                owner="tester",
                rag_cfg={},
                state_db=state_db,
                qdrant_client=MagicMock(),
            )
        )

        state_db.mark_failed.assert_not_called()
        state_db.mark_completed.assert_called_once()
        kwargs = state_db.mark_completed.call_args.kwargs
        assert kwargs["pipeline_version"] == "p1"
        assert kwargs["chunk_count"] == 0

    def test_launch_dir_reaches_mark_completed_verbatim(self, tmp_path, monkeypatch, _no_live_services):
        # The trailing slash is kept on purpose: launch_dir is stored verbatim, never normalised.
        cwd = "/mnt/nfs/Florian/Gin-AI/projects/RAGNART/"
        transcript = _write_claude_transcript(
            tmp_path / ".claude" / "projects" / "-x" / "abc.jsonl", cwd=cwd,
        )
        _no_live_services.side_effect = _one_vector_per_text
        monkeypatch.setattr(ingest, "_is_sparse_ready", lambda _client: False)
        state_db = MagicMock(spec=StateDB)
        qdrant_client = MagicMock()

        asyncio.run(
            ingest.ingest_file(
                file_path="claude-session://abc",
                origin_path=str(transcript),
                owner="tester",
                rag_cfg={},
                state_db=state_db,
                qdrant_client=qdrant_client,
            )
        )

        # The SQLite side gets launch_dir and the hash of the transcript file.
        kwargs = state_db.mark_completed.call_args.kwargs
        assert kwargs["launch_dir"] == cwd
        assert kwargs["source_type"] == "claude_session"
        assert kwargs["content_hash"] == hashlib.md5(transcript.read_bytes()).hexdigest()

        # The Qdrant payload must not carry the launch directory.
        points = [p for call in qdrant_client.upsert.call_args_list for p in call.kwargs["points"]]
        assert points
        assert "launch_dir" not in points[0].payload
        assert cwd not in points[0].payload.values()

    def test_points_carry_identity_and_hash_fields(self, tmp_path, monkeypatch, _no_live_services):
        file_path = tmp_path / "notes.md"
        file_path.write_text("Notes about the backup rotation schedule.", encoding="utf-8")
        _no_live_services.side_effect = _one_vector_per_text
        monkeypatch.setattr(ingest, "_is_sparse_ready", lambda _client: False)
        state_db = MagicMock(spec=StateDB)
        qdrant_client = MagicMock()

        # 1_700_000_000 is 2023-11-14T22:13:20 UTC.
        asyncio.run(
            ingest.ingest_file(
                file_path=str(file_path),
                owner="tester",
                rag_cfg={},
                state_db=state_db,
                qdrant_client=qdrant_client,
                last_modified=1_700_000_000.0,
            )
        )

        points = [p for call in qdrant_client.upsert.call_args_list for p in call.kwargs["points"]]
        assert points
        for point in points:
            assert point.payload[FIELD_PROJECT_ID] == GLOBAL_PROJECT_ID
            assert point.payload[FIELD_SOURCE_TYPE] == "file"
            assert point.payload[FIELD_CONTENT_HASH] == hashlib.md5(file_path.read_bytes()).hexdigest()
            assert point.payload[FIELD_MODIFIED_AT] == "2023-11-14T22:13:20+00:00"
            # ingested_at is the same string SQLite stores as completed_at.
            assert point.payload[FIELD_INGESTED_AT] == state_db.mark_completed.call_args.kwargs["completed_at"]


# ---------------------------------------------------------------------------
# Plain .jsonl files outside the Claude Code layout (NAS notes)
# ---------------------------------------------------------------------------

class TestPlainJsonlOutsideClaudeLayout:
    """A .jsonl with no `projects` component must parse, not raise on the .claude lookup."""

    def test_bare_tmp_path_jsonl_parses_without_raising(self, tmp_path):
        # A bare tmp_path has no `projects` component, so the .claude lookup must be skipped.
        notes = _write_claude_transcript(tmp_path / "notes.jsonl", cwd="/x")

        text, metadata, launch_dir = ingest._parse_claude_chat(notes)

        assert "How do I rotate the backup key?" in text
        assert "Run the rotate command." in text
        assert metadata["session_title"] == "How do I rotate the backup key?"
        assert launch_dir == "/x"

    def test_nas_jsonl_through_generic_branch_does_not_hit_claude_lookup(self, tmp_path, monkeypatch):
        # A NAS .jsonl takes the generic branch (file_type "jsonl" -> _parse_to_text).
        # The .claude lookup must not run for it, so claude_dot_claude_dir must never be called.
        def _fail_if_called(path):
            raise AssertionError(f"claude_dot_claude_dir called for non-Claude path: {path}")

        monkeypatch.setattr(ingest, "claude_dot_claude_dir", _fail_if_called)
        notes = _write_claude_transcript(tmp_path / "nas" / "Dropbox" / "notes.jsonl", cwd="/x")
        state_db = MagicMock(spec=StateDB)

        asyncio.run(
            ingest.ingest_file(
                file_path=str(notes),
                owner="tester",
                rag_cfg={},
                state_db=state_db,
                qdrant_client=MagicMock(),
            )
        )

        state_db.mark_failed.assert_not_called()
        state_db.mark_completed.assert_called_once()
