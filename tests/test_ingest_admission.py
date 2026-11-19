"""
Tests for the admission gate, its order, and the docling CPU accelerator, on ingest_content() (session B2, A.13).

The order is what these tests pin: a refusal by size or NUL sniff happens before any parse, the
replacement-character check runs on the parsed text, and the printable gate and the chunk cap run before any
embedding. The reasons' wording is covered in tests/test_ingest_content_core.py.

No live services are used. The Qdrant client is a MagicMock, the embedder is an AsyncMock, and docling conversion
is replaced where it would run.
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
from ragnart.ingest import IngestResult, ingest_content
from ragnart.schema import (
    FIELD_CONTENT_HASH,
    FIELD_INGESTED_AT,
    FIELD_MODIFIED_AT,
    FIELD_PIPELINE_VERSION,
    FIELD_PROJECT_ID,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    FIELD_SOURCE_TYPE,
    GLOBAL_PROJECT_ID,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
)

RAG_CFG = {"embedding_model": "bge-m3", "admission": {}}
MODIFIED_AT = "2023-11-14T22:13:20+00:00"  # 1_700_000_000 seconds, as the API stores it.


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _no_live_services(monkeypatch):
    """
    Replace every call that would reach Qdrant or Ollama.

    The embedder returns one dummy vector per child text, so points are built when a test gets that far.
    The returned mock lets a test assert that embedding was never reached.
    """
    monkeypatch.setattr(ingest, "ensure_collection", MagicMock())
    monkeypatch.setattr(ingest, "_is_sparse_ready", lambda _client: False)
    embed = AsyncMock(side_effect=lambda texts, **_kwargs: [[0.0, 0.0] for _ in texts])
    monkeypatch.setattr(ingest, "embed_batch", embed)
    return embed


def _spool(tmp_path: Path, data: bytes, name: str = "req-1") -> str:
    """Write uploaded bytes to a spool file with no suffix, as the ingestion API does."""
    spool_dir = tmp_path / "spool"
    spool_dir.mkdir(exist_ok=True)
    path = spool_dir / name
    path.write_bytes(data)
    return str(path)


def _ingest(rag_cfg: dict | None = None, **kwargs) -> tuple[IngestResult, MagicMock]:
    """Run ingest_content() with a mock Qdrant client. Returns (result, client)."""
    client = MagicMock()
    result = asyncio.run(
        ingest_content(
            qdrant_client=client,
            rag_cfg=rag_cfg if rag_cfg is not None else RAG_CFG,
            **kwargs,
        )
    )
    return result, client


def _upload(rag_cfg: dict | None, spool: str, key: str, file_type: str) -> tuple[IngestResult, MagicMock]:
    """Run ingest_content() for one uploaded file, as the NAS or Mac side sends it."""
    return _ingest(rag_cfg=rag_cfg, source_id=SOURCE_ID_NAS, key=key, owner="tester",
                   spool_path=spool, file_type=file_type)


def _points(client: MagicMock) -> list:
    """Every point the client was asked to upsert, across all batches."""
    return [point for call in client.upsert.call_args_list for point in call.kwargs["points"]]


def _cfg(**admission) -> dict:
    """A rag config whose admission block holds the given overrides on top of the code defaults."""
    return {"embedding_model": "bge-m3", "admission": dict(admission)}


# ---------------------------------------------------------------------------
# Docling failures: a parse failure is an error, not an ignored item, and no raw read is attempted
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
    def test_docling_failure_propagates_and_writes_no_points(self, monkeypatch, tmp_path, suffix):
        # The exception reaches the caller. The intake worker records the request as failed (see
        # tests/test_intake_worker.py TestFailures), so the item is not ignored and nothing is upserted.
        converter = MagicMock()
        converter.convert.side_effect = ValueError("corrupt container")
        monkeypatch.setattr(ingest, "_get_converter", lambda: converter)
        spool = _spool(tmp_path, b"\x00\x01binary-looking payload")

        with pytest.raises(RuntimeError, match="docling failed:"):
            _upload(None, spool, f"/mnt/nfs/doc.{suffix}", suffix)

    def test_pptx_docling_failure_still_uses_python_pptx(self, monkeypatch, tmp_path):
        # PPTX keeps its python-pptx fallback. This guards against a change above removing that path by mistake.
        converter = MagicMock()
        converter.convert.side_effect = ValueError("format not detected")
        monkeypatch.setattr(ingest, "_get_converter", lambda: converter)
        monkeypatch.setattr(ingest, "_parse_pptx", lambda path: "Slide 1\nfallback text")

        result = ingest._parse_to_text(str(tmp_path / "deck.pptx"), "pptx")

        assert result == "Slide 1\nfallback text"


# ---------------------------------------------------------------------------
# Admission order: each refusal happens at the first point its rule can decide
# ---------------------------------------------------------------------------

class TestAdmissionOrder:

    def test_size_cap_refuses_before_the_parse(self, monkeypatch, tmp_path, _no_live_services):
        # A 100-byte file against a 10-byte cap: refused by its size, so it is never parsed or embedded.
        parse = MagicMock(return_value="unused")
        monkeypatch.setattr(ingest, "_parse_to_text", parse)
        spool = _spool(tmp_path, b"a" * 100)

        result, client = _upload(_cfg(max_text_file_size_bytes=10), spool, "/mnt/nfs/big.txt", "txt")

        assert result == IngestResult("ignored", "too large: 100 bytes > 10 byte cap", 0, None, "p1")
        parse.assert_not_called()
        _no_live_services.assert_not_awaited()
        client.upsert.assert_not_called()

    def test_nul_byte_in_first_window_refuses_before_the_parse(self, monkeypatch, tmp_path, _no_live_services):
        # A text-like upload with a NUL byte near the start: refused by the NUL sniff, before any parse.
        parse = MagicMock(return_value="unused")
        monkeypatch.setattr(ingest, "_parse_to_text", parse)
        spool = _spool(tmp_path, b"hello\x00world" + b"x" * 100)

        result, _client = _upload(None, spool, "/mnt/nfs/blob.txt", "txt")

        assert result.status == "ignored"
        assert "NUL" in result.reason
        parse.assert_not_called()
        _no_live_services.assert_not_awaited()

    def test_replacement_ratio_is_checked_on_the_parsed_text(self, monkeypatch, tmp_path, _no_live_services):
        # 10 U+FFFD in 1010 characters is about 0.99%, above the 0.5% default. The file is parsed first (the
        # check needs the decoded text), then refused before chunking.
        parse = MagicMock(wraps=ingest._parse_to_text)
        monkeypatch.setattr(ingest, "_parse_to_text", parse)
        select = MagicMock(return_value=([], "flat"))
        monkeypatch.setattr(ingest, "_select_chunks", select)
        spool = _spool(tmp_path, ("�" * 10 + "a" * 1000).encode("utf-8"))

        result, client = _upload(None, spool, "/mnt/nfs/mojibake.txt", "txt")

        assert result.status == "ignored"
        assert "replacement chars" in result.reason
        parse.assert_called_once()
        select.assert_not_called()
        _no_live_services.assert_not_awaited()
        client.upsert.assert_not_called()

    def test_replacement_ratio_under_threshold_proceeds_to_chunking(self, monkeypatch, tmp_path):
        # 1 U+FFFD in 1000 characters is 0.1%, below the 0.5% default. An empty chunk list ends the run
        # right after chunking, before any embedding, so the item completes with no points.
        select = MagicMock(return_value=([], "flat"))
        monkeypatch.setattr(ingest, "_select_chunks", select)
        spool = _spool(tmp_path, ("�" + "a" * 999).encode("utf-8"))

        result, _client = _upload(None, spool, "/mnt/nfs/almost-clean.txt", "txt")

        select.assert_called_once()
        assert result == IngestResult("completed", None, 0, None, "p1")

    def test_printable_gate_refuses_before_chunking(self, monkeypatch):
        # Rendered session text is not parsed, but it passes through the printable gate before it is chunked.
        select = MagicMock(return_value=([], "flat"))
        monkeypatch.setattr(ingest, "_select_chunks", select)

        result, client = _ingest(connector="claude_code", source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
                                 key="claude-session://junk", owner="tester", text="\x01\x02\x03" * 50)

        assert result.status == "ignored"
        assert result.reason == "binary content: 0% printable chars"
        select.assert_not_called()
        client.upsert.assert_not_called()

    def test_chunk_cap_refuses_before_embedding(self, monkeypatch, tmp_path, _no_live_services):
        # Three chunks against a cap of one: refused with the actual numbers, and no embedding call is made.
        three_chunks = [("a", "a"), ("b", "b"), ("c", "c")]
        monkeypatch.setattr(ingest, "_select_chunks", MagicMock(return_value=(three_chunks, "flat")))
        spool = _spool(tmp_path, b"some text")

        result, client = _upload(_cfg(max_chunks_per_item=1), spool, "/mnt/nfs/many.txt", "txt")

        assert result == IngestResult("ignored", "too large: 3 chunks > 1 chunk cap", 0, None, "p1")
        _no_live_services.assert_not_awaited()
        client.upsert.assert_not_called()

    def test_chunk_cap_applies_to_docling_types(self, monkeypatch, tmp_path, _no_live_services):
        # The chunk cap covers every file type, not only text-like ones: a parsed DOCX is refused the same way.
        converter = MagicMock()
        converter.convert.return_value.document.export_to_markdown.return_value = "markdown body"
        monkeypatch.setattr(ingest, "_get_converter", lambda: converter)
        three_chunks = [("a", "a"), ("b", "b"), ("c", "c")]
        monkeypatch.setattr(ingest, "_select_chunks", MagicMock(return_value=(three_chunks, "flat")))
        spool = _spool(tmp_path, b"placeholder")

        result, _client = _upload(_cfg(max_chunks_per_item=2), spool, "/mnt/nfs/report.docx", "docx")

        assert result == IngestResult("ignored", "too large: 3 chunks > 2 chunk cap", 0, None, "p1")
        _no_live_services.assert_not_awaited()


# ---------------------------------------------------------------------------
# Explicit CPU accelerator for docling
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
# Session text: an empty session completes with no points, and its points carry the Claude source id
# ---------------------------------------------------------------------------

class TestSessionText:

    def test_empty_session_text_completes_with_no_chunks_and_p1(self, _no_live_services):
        # A transcript with no user or assistant turn renders to empty text. The item completes with no points,
        # and its row must carry pipeline_version p1, so the metadata backfill never reclassifies it.
        result, client = _ingest(connector="claude_code", source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
                                 key="claude-session://abc", owner="tester", text="")

        assert result == IngestResult("completed", None, 0, None, "p1")
        _no_live_services.assert_not_awaited()
        client.upsert.assert_not_called()

    def test_claude_session_points_carry_the_claude_source_id_p1_and_text_hash(self):
        text = "How do I rotate the backup key?\n\nRun the rotate command."
        result, client = _ingest(connector="claude_code", source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
                                 key="claude-session://abc", owner="tester", text=text)

        points = _points(client)
        assert points
        for point in points:
            assert point.payload[FIELD_SOURCE_ID] == SOURCE_ID_CLAUDE_LINUX_SHARED_NFS
            assert point.payload[FIELD_PIPELINE_VERSION] == "p1"
            assert point.payload[FIELD_SOURCE_TYPE] == "claude_session"
            assert point.payload[FIELD_CONTENT_HASH] == hashlib.md5(text.encode("utf-8")).hexdigest()
        assert result.content_hash == hashlib.md5(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Upload points: identity, hash and freshness fields
# ---------------------------------------------------------------------------

class TestUploadPoints:

    def test_upload_points_carry_identity_hash_and_modified_at(self, tmp_path):
        body = b"# Notes\n\nAbout the backup rotation schedule.\n"
        spool = _spool(tmp_path, body)

        result, client = _ingest(rag_cfg=RAG_CFG, source_id=SOURCE_ID_NAS, key="/mnt/nfs/notes.md", owner="tester",
                                 spool_path=spool, file_type="md", modified_at=MODIFIED_AT)

        points = _points(client)
        assert points
        for point in points:
            assert point.payload[FIELD_PROJECT_ID] == GLOBAL_PROJECT_ID
            assert point.payload[FIELD_SOURCE_TYPE] == "file"
            assert point.payload[FIELD_SOURCE_ID] == SOURCE_ID_NAS
            assert point.payload[FIELD_SOURCE_PATH] == "/mnt/nfs/notes.md"
            assert point.payload[FIELD_PIPELINE_VERSION] == "p1"
            assert point.payload[FIELD_CONTENT_HASH] == hashlib.md5(body).hexdigest()
            assert point.payload[FIELD_MODIFIED_AT] == MODIFIED_AT
            # ingested_at is written at upsert time: present, and a non-empty ISO-8601 string.
            assert point.payload[FIELD_INGESTED_AT]
        assert result.content_hash == hashlib.md5(body).hexdigest()


# ---------------------------------------------------------------------------
# A .jsonl upload outside the Claude Code layout is parsed as a plain transcript, never through the .claude lookup
# ---------------------------------------------------------------------------

def _write_claude_transcript(path: Path, cwd: str) -> Path:
    """
    Write a minimal Claude Code session JSONL with one user and one assistant text turn.

    The file sits under a `projects/` directory, as real transcripts do. The parser finds the .claude
    directory from that component, so a bare tmp_path would fail.
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


class TestPlainJsonlOutsideClaudeLayout:

    def test_bare_tmp_path_jsonl_parses_without_raising(self, tmp_path):
        # A bare tmp_path has no `projects` component, so the .claude lookup must be skipped.
        notes = _write_claude_transcript(tmp_path / "notes.jsonl", cwd="/x")

        text, metadata, launch_dir = ingest._parse_claude_chat(notes)

        assert "How do I rotate the backup key?" in text
        assert "Run the rotate command." in text
        assert metadata["session_title"] == "How do I rotate the backup key?"
        assert launch_dir == "/x"

    def test_nas_jsonl_upload_does_not_hit_the_claude_lookup(self, tmp_path, monkeypatch, _no_live_services):
        # A NAS .jsonl upload takes the generic branch (file_type "jsonl" -> _parse_to_text). The .claude lookup
        # in chat_render must not run for it.
        def _fail_if_called(path):
            raise AssertionError(f"claude_dot_claude_dir called for non-Claude path: {path}")

        monkeypatch.setattr("ragnart.chat_render.claude_dot_claude_dir", _fail_if_called)
        notes = _write_claude_transcript(tmp_path / "nas" / "Dropbox" / "notes.jsonl", cwd="/x")
        spool = _spool(tmp_path, notes.read_bytes())

        result, _client = _upload(None, spool, "/mnt/nfs/Dropbox/notes.jsonl", "jsonl")

        assert result.status == "completed"
        assert result.chunk_count > 0


# ---------------------------------------------------------------------------
# Source fields on the delete path and on the points: source_id and pipeline_version p1
# ---------------------------------------------------------------------------

class TestSourceFields:

    def test_upload_points_carry_the_source_id_passed_in_not_one_from_the_key(self, tmp_path):
        # The source comes from the caller. A key that looks like another source's key does not change it.
        spool = _spool(tmp_path, b"Some notes about the backup rotation.")

        _result, client = _ingest(rag_cfg=RAG_CFG, source_id=SOURCE_ID_NAS, key="claude-session://looks-like-a-session",
                                  owner="tester", spool_path=spool, file_type="md")

        points = _points(client)
        assert points
        assert all(point.payload[FIELD_SOURCE_ID] == SOURCE_ID_NAS for point in points)
        assert all(point.payload[FIELD_PIPELINE_VERSION] == "p1" for point in points)
