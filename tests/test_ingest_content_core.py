"""
Tests for ingest.ingest_content(), the content-based ingestion core for endpoint items (v0.3 B1).

No live services are used. The Qdrant client is a MagicMock, so its delete and upsert calls are
recorded and never sent. The embedder is an AsyncMock that returns one small vector per child
text. Sparse vectors are switched off, so no fastembed model is loaded.

Each test asserts one property of the points or of the result: the payload shape of a chat text
upsert, the payload shape of a markdown upload, the admission reasons, the delete filter, and the
way a spool file is parsed by its declared type.
"""

import asyncio
import hashlib
import os
import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from ragnart import ingest
from ragnart.ingest import IngestResult, ingest_content
from ragnart.schema import (
    FIELD_CONTENT_HASH,
    FIELD_FILE_NAME,
    FIELD_FILE_TYPE,
    FIELD_PIPELINE_VERSION,
    FIELD_PROJECT_ID,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    FIELD_SOURCE_TYPE,
    FIELD_TAG,
    GLOBAL_PROJECT_ID,
    PIPELINE_VERSION_P1,
)

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


def _points(client: MagicMock) -> list:
    """Every point the client was asked to upsert, across all batches."""
    return [point for call in client.upsert.call_args_list for point in call.kwargs["points"]]


def _delete_filter_values(client: MagicMock) -> dict:
    """The {key: value} of the conditions in the delete filter. Asserts there is one filter of must-conditions."""
    selector = client.delete.call_args.kwargs["points_selector"]
    return {condition.key: condition.match.value for condition in selector.filter.must}


def _spool(tmp_path: Path, data: bytes, name: str = "req-1") -> str:
    """Write uploaded bytes to a spool file with no suffix, as the ingestion API does."""
    spool_dir = tmp_path / "spool"
    spool_dir.mkdir(exist_ok=True)
    path = spool_dir / name
    path.write_bytes(data)
    return str(path)


# ---------------------------------------------------------------------------
# Text upserts (claude_code sessions rendered by the endpoint)
# ---------------------------------------------------------------------------

class TestTextUpsert:
    def test_writes_chat_payload_with_given_source_and_key(self):
        text = "hello world, a short planning session. " * 40
        key = "claude-session://abc-123"
        meta = {"session_id": "abc-123", "project": "ragnart", "session_title": "Planning chat"}

        result, client = _ingest(connector="claude_code", source_id="claude-MBP", key=key, owner="florian", text=text, chat_meta=meta)

        assert result.status == "completed"
        assert result.reason is None
        assert result.pipeline_version == PIPELINE_VERSION_P1
        assert result.content_hash == hashlib.md5(text.encode("utf-8")).hexdigest()

        points = _points(client)
        assert result.chunk_count == len(points) > 0
        for point in points:
            payload = point.payload
            # The source is the one passed in. The key's prefix does not decide it (the NAS rule would not).
            assert payload[FIELD_SOURCE_ID] == "claude-MBP"
            assert payload[FIELD_SOURCE_PATH] == key
            assert payload[FIELD_SOURCE_TYPE] == "claude_session"
            assert payload["file_type"] == "claude_chat"
            assert payload[FIELD_FILE_TYPE] == "claude_chat"
            assert payload[FIELD_FILE_NAME] == "Planning chat"
            assert payload["owner"] == "florian"
            assert payload[FIELD_PROJECT_ID] == GLOBAL_PROJECT_ID
            assert payload[FIELD_PIPELINE_VERSION] == PIPELINE_VERSION_P1
            assert payload[FIELD_CONTENT_HASH] == result.content_hash
            # The chat metadata is spread into every point.
            assert payload["session_id"] == "abc-123"
            assert payload["project"] == "ragnart"
            assert payload["session_title"] == "Planning chat"

    def test_delete_filter_names_source_and_key(self):
        _, client = _ingest(connector="claude_code", source_id="claude-MBP", key="claude-session://abc", owner="florian",
                            text="some text " * 20)

        assert _delete_filter_values(client) == {FIELD_SOURCE_ID: "claude-MBP", FIELD_SOURCE_PATH: "claude-session://abc"}
        client.delete.assert_called_once()
        # The delete comes before the first upsert.
        call_names = [name for name, _args, _kwargs in client.mock_calls]
        assert call_names.index("delete") < call_names.index("upsert")

    def test_chat_metadata_cannot_override_identity_fields(self):
        # Defence in depth: the API already refuses these names, but the core must not let them win.
        meta = {"source_id": "nas", "source_path": "/elsewhere", "owner": "mallory", "session_title": "T"}

        _, client = _ingest(connector="claude_code", source_id="claude-MBP", key="claude-session://abc", owner="florian",
                            text="some text " * 20, chat_meta=meta)

        for point in _points(client):
            assert point.payload[FIELD_SOURCE_ID] == "claude-MBP"
            assert point.payload[FIELD_SOURCE_PATH] == "claude-session://abc"
            assert point.payload["owner"] == "florian"

    def test_modified_at_is_written_unchanged_to_every_point(self):
        # The string is written as given: isoformat() of a UTC datetime for the same instant.
        stamp = "2023-11-14T22:13:20+00:00"
        _, client = _ingest(connector="claude_code", source_id="claude-MBP", key="claude-session://abc", owner="florian",
                            text="some text " * 20, modified_at=stamp)

        for point in _points(client):
            assert point.payload["modified_at"] == stamp

    def test_modified_at_matches_the_nas_format(self):
        # The NAS side writes datetime.fromtimestamp(ts, utc).isoformat(); the API's stored form must equal it.
        from datetime import datetime, timezone
        stamp = datetime.fromtimestamp(1_700_000_000.0, tz=timezone.utc).isoformat()
        _, client = _ingest(connector="claude_code", source_id="claude-MBP", key="claude-session://abc", owner="florian",
                            text="some text " * 20, modified_at=stamp)

        assert _points(client)[0].payload["modified_at"] == "2023-11-14T22:13:20+00:00"


# ---------------------------------------------------------------------------
# Byte upserts (files from the endpoint's filesystem source)
# ---------------------------------------------------------------------------

class TestBytesUpsert:
    def test_markdown_upload_writes_file_payload(self, tmp_path):
        body = "# Notes\n\nSome plain markdown body text for the endpoint file.\n".encode("utf-8")
        spool = _spool(tmp_path, body)
        key = "/Users/fotel/MBP/notes/todo.md"

        result, client = _ingest(source_id="MBP", key=key, owner="florian",
                                 spool_path=spool, file_type="md")

        assert result.status == "completed"
        assert result.content_hash == hashlib.md5(body).hexdigest()
        points = _points(client)
        assert result.chunk_count == len(points) > 0
        for point in points:
            payload = point.payload
            assert payload[FIELD_SOURCE_ID] == "MBP"
            assert payload[FIELD_SOURCE_PATH] == key
            assert payload[FIELD_SOURCE_TYPE] == "file"
            assert payload[FIELD_FILE_TYPE] == "md"
            assert payload[FIELD_FILE_NAME] == "todo.md"
            assert payload["owner"] == "florian"
            assert payload[FIELD_PROJECT_ID] == GLOBAL_PROJECT_ID
            assert payload[FIELD_PIPELINE_VERSION] == PIPELINE_VERSION_P1
            # No session fields on a file.
            for session_field in ("session_id", "session_title", "project"):
                assert session_field not in payload

    def test_spool_file_is_read_not_removed(self, tmp_path):
        # The caller (the intake worker) removes the spool file after a terminal status, not ingest_content.
        body = b"plain text body " * 10
        spool = _spool(tmp_path, body)

        _ingest(source_id="MBP", key="/Users/fotel/MBP/a.txt", owner="florian", spool_path=spool, file_type="txt")

        assert Path(spool).read_bytes() == body

    def test_declared_type_is_normalised(self, tmp_path):
        spool = _spool(tmp_path, b"some text body " * 10)

        result, client = _ingest(source_id="MBP", key="/Users/fotel/MBP/a.MD", owner="florian",
                                 spool_path=spool, file_type=".MD")

        assert result.status == "completed"
        assert _points(client)[0].payload[FIELD_FILE_TYPE] == "md"


class TestParsingByDeclaredType:
    def test_parser_gets_a_link_with_the_declared_suffix_and_the_link_is_removed(self, tmp_path, monkeypatch):
        # A spool file has no name, so the parser must be given a link whose suffix is the declared type.
        spool = _spool(tmp_path, b"%PDF-1.4 fake body")
        seen = {}

        def fake_parse(file_path: str, file_type: str) -> str:
            seen["path"] = file_path
            seen["file_type"] = file_type
            seen["link_exists_during_parse"] = os.path.exists(file_path)
            seen["link_target"] = os.path.realpath(file_path)
            return "parsed body text for the declared pdf " * 10

        monkeypatch.setattr(ingest, "_parse_to_text", fake_parse)

        result, client = _ingest(source_id="MBP", key="/Users/fotel/MBP/paper.pdf", owner="florian",
                                 spool_path=spool, file_type="pdf")

        assert result.status == "completed"
        assert seen["file_type"] == "pdf"
        assert seen["path"].endswith(".pdf")
        assert seen["link_exists_during_parse"] is True
        assert seen["link_target"] == os.path.realpath(spool)
        # The temporary link directory is gone once the parse ends.
        assert not os.path.exists(seen["path"])
        assert _points(client)[0].payload[FIELD_FILE_TYPE] == "pdf"


# ---------------------------------------------------------------------------
# Admission reasons
# ---------------------------------------------------------------------------

class TestAdmission:
    def test_oversize_text_like_upload_is_ignored_with_cap_reason(self, tmp_path, _no_live_services):
        cfg = {"admission": {**RAG_CFG["admission"], "max_text_file_size_bytes": 10}}
        spool = _spool(tmp_path, b"x" * 100)

        result, client = _ingest(rag_cfg=cfg, source_id="MBP", key="/Users/fotel/MBP/big.txt",
                                 owner="florian", spool_path=spool, file_type="txt")

        assert result == IngestResult("ignored", "too large: 100 bytes > 10 byte cap", 0, None, PIPELINE_VERSION_P1)
        _no_live_services.assert_not_called()
        client.upsert.assert_not_called()
        # The stale points are still deleted first.
        client.delete.assert_called_once()

    def test_nul_byte_in_text_like_upload_is_ignored(self, tmp_path, _no_live_services):
        spool = _spool(tmp_path, b"looks like text\x00but is not")

        result, client = _ingest(source_id="MBP", key="/Users/fotel/MBP/bin.txt", owner="florian",
                                 spool_path=spool, file_type="txt")

        assert result.status == "ignored"
        assert result.reason == "binary: NUL byte in first 8KB"
        _no_live_services.assert_not_called()
        client.upsert.assert_not_called()

    def test_replacement_heavy_text_like_upload_is_ignored(self, tmp_path):
        # 0x80 is not valid UTF-8 on its own, so every byte decodes to U+FFFD.
        spool = _spool(tmp_path, b"\x80" * 300)

        result, client = _ingest(source_id="MBP", key="/Users/fotel/MBP/junk.txt", owner="florian",
                                 spool_path=spool, file_type="txt")

        assert result.status == "ignored"
        assert result.reason == "binary: 100.0% replacement chars"
        client.upsert.assert_not_called()

    def test_text_upload_below_printable_gate_is_ignored(self):
        # Rendered text is not parsed, but it still passes the printable gate.
        result, client = _ingest(connector="claude_code", source_id="claude-MBP", key="claude-session://junk", owner="florian",
                                 text="\x01\x02\x03" * 50)

        assert result.status == "ignored"
        assert result.reason == "binary content: 0% printable chars"
        client.upsert.assert_not_called()

    def test_chunk_cap_ignores_the_item(self):
        cfg = {"admission": {**RAG_CFG["admission"], "max_chunks_per_item": 1}}

        result, client = _ingest(connector="claude_code", rag_cfg=cfg, source_id="claude-MBP", key="claude-session://long",
                                 owner="florian", text="word " * 3000)

        assert result.status == "ignored"
        assert re.fullmatch(r"too large: \d+ chunks > 1 chunk cap", result.reason)
        assert result.chunk_count == 0
        client.upsert.assert_not_called()

    def test_docling_type_skips_text_like_checks(self, tmp_path, monkeypatch):
        # A PDF is not text-like, so a NUL byte is not a reason to refuse it here; the parser decides.
        spool = _spool(tmp_path, b"\x00\x01binary body")
        monkeypatch.setattr(ingest, "_parse_to_text", lambda _p, _t: "parsed pdf text " * 10)

        result, _ = _ingest(source_id="MBP", key="/Users/fotel/MBP/paper.pdf", owner="florian",
                            spool_path=spool, file_type="pdf")

        assert result.status == "completed"


# ---------------------------------------------------------------------------
# Empty items
# ---------------------------------------------------------------------------

class TestEmptyItems:
    @pytest.mark.parametrize("text", ["", "   \n\t  "])
    def test_blank_text_completes_with_no_chunks_and_deletes_stale_points(self, text):
        result, client = _ingest(connector="claude_code", source_id="claude-MBP", key="claude-session://blank", owner="florian", text=text)

        assert result == IngestResult("completed", None, 0, None, PIPELINE_VERSION_P1)
        client.upsert.assert_not_called()
        assert _delete_filter_values(client) == {FIELD_SOURCE_ID: "claude-MBP", FIELD_SOURCE_PATH: "claude-session://blank"}

    def test_empty_upload_completes_with_no_chunks(self, tmp_path):
        spool = _spool(tmp_path, b"")

        result, client = _ingest(source_id="MBP", key="/Users/fotel/MBP/empty.md", owner="florian",
                                 spool_path=spool, file_type="md")

        assert result == IngestResult("completed", None, 0, None, PIPELINE_VERSION_P1)
        client.upsert.assert_not_called()
        client.delete.assert_called_once()


# ---------------------------------------------------------------------------
# Argument checks
# ---------------------------------------------------------------------------

class TestArguments:
    def test_neither_content_form_is_refused(self):
        with pytest.raises(ValueError, match="exactly one"):
            _ingest(source_id="MBP", key="/a.md", owner="florian")

    def test_both_content_forms_are_refused(self, tmp_path):
        spool = _spool(tmp_path, b"body")
        with pytest.raises(ValueError, match="exactly one"):
            _ingest(source_id="MBP", key="/a.md", owner="florian", spool_path=spool, file_type="md", text="x")

    def test_spool_without_file_type_is_refused(self, tmp_path):
        spool = _spool(tmp_path, b"body")
        with pytest.raises(ValueError, match="file_type"):
            _ingest(source_id="MBP", key="/a.md", owner="florian", spool_path=spool)

    @pytest.mark.parametrize("bad_type", ["../etc", "md/x", "m d", ""])
    def test_file_type_must_be_a_plain_word(self, tmp_path, bad_type):
        # The declared type becomes a file suffix during parsing, so anything but a plain word is refused.
        spool = _spool(tmp_path, b"body")
        with pytest.raises(ValueError, match="invalid file type"):
            _ingest(source_id="MBP", key="/a.md", owner="florian", spool_path=spool, file_type=bad_type)


# ---------------------------------------------------------------------------
# The connector decides the item's type; tag and project_id are written as given (session B2, A.5)
# ---------------------------------------------------------------------------

class TestConnectorAndPerItemFields:
    def test_opencode_text_is_stored_as_an_opencode_session_with_no_hash(self):
        # source_type opencode_session, file_type opencode, no hash.
        text = "an opencode session, rendered by the sender. " * 30
        key = "opencode://ses_abc"

        result, client = _ingest(connector="opencode", source_id="opencode-k11", key=key, owner="florian",
                                 text=text, chat_meta={"session_id": "ses_abc", "session_title": "Planning"})

        assert result.status == "completed"
        assert result.content_hash is None
        points = _points(client)
        assert result.chunk_count == len(points) > 0
        for point in points:
            assert point.payload[FIELD_SOURCE_TYPE] == "opencode_session"
            assert point.payload[FIELD_FILE_TYPE] == "opencode"
            assert point.payload[FIELD_CONTENT_HASH] is None
            assert point.payload[FIELD_SOURCE_ID] == "opencode-k11"
            assert point.payload[FIELD_FILE_NAME] == "Planning"

    def test_claude_code_text_keeps_the_b1_claude_session_type(self):
        _, client = _ingest(connector="claude_code", source_id="claude-MBP", key="claude-session://abc",
                            owner="florian", text="a claude session. " * 30)

        assert {p.payload[FIELD_SOURCE_TYPE] for p in _points(client)} == {"claude_session"}
        assert {p.payload[FIELD_FILE_TYPE] for p in _points(client)} == {"claude_chat"}

    def test_tag_and_project_id_are_written_to_every_point(self, tmp_path):
        body = "# Certificates\n\nA note about a certificate course.\n".encode("utf-8")
        spool = _spool(tmp_path, body)

        result, client = _ingest(source_id="nas", key="/mnt/nfs/Florian/proj/cert.md", owner="florian",
                                 spool_path=spool, file_type="md", tag="certifications", project_id="proj-1")

        assert result.status == "completed"
        points = _points(client)
        assert points
        for point in points:
            assert point.payload[FIELD_TAG] == "certifications"
            assert point.payload[FIELD_PROJECT_ID] == "proj-1"

    def test_tag_and_project_id_default_to_empty_and_global(self, tmp_path):
        spool = _spool(tmp_path, b"# Plain\n\nA plain note with no tag and no project.\n")

        _, client = _ingest(source_id="MBP", key="/Users/fotel/MBP/plain.md", owner="florian",
                            spool_path=spool, file_type="md")

        for point in _points(client):
            assert point.payload[FIELD_TAG] == ""
            assert point.payload[FIELD_PROJECT_ID] == GLOBAL_PROJECT_ID

    def test_bytes_for_a_session_connector_is_refused(self, tmp_path):
        spool = _spool(tmp_path, b"not a session, but bytes\n")
        for connector in ("claude_code", "opencode"):
            with pytest.raises(ValueError, match="sent as text"):
                _ingest(connector=connector, source_id="claude-MBP", key="claude-session://x", owner="florian",
                        spool_path=spool, file_type="md")

    def test_text_for_a_filesystem_connector_is_refused(self):
        with pytest.raises(ValueError, match="uploaded as bytes"):
            _ingest(connector="filesystem", source_id="MBP", key="/a.md", owner="florian", text="x " * 30)

    def test_an_unknown_connector_is_refused(self, tmp_path):
        spool = _spool(tmp_path, b"# Notes\n")
        with pytest.raises(ValueError, match="unknown connector"):
            _ingest(connector="ftp", source_id="MBP", key="/a.md", owner="florian",
                    spool_path=spool, file_type="md")
