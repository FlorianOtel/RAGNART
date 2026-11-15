"""
Tests for the ingestion admission gate and the docling CPU accelerator (plan steps 7-8).

No live services are used. StateDB and the Qdrant client are mocks, the Ollama embedding
call is replaced by an AsyncMock, and docling conversion is replaced where it would run.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from docling.datamodel.accelerator_options import AcceleratorDevice
from docling.datamodel.base_models import InputFormat
from docling.document_converter import DocumentConverter

from ragnart import ingest
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
        state_db.mark_completed.assert_called_once_with(str(file_path))

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
