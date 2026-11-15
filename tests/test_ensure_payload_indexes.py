"""
Tests for ensure_payload_indexes() in ragnart/collection.py.

Everything runs against a MagicMock Qdrant client. No live Qdrant server is
contacted. The module-level per-field cache is cleared before each test so
that tests do not leak state into each other.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from qdrant_client.models import PayloadSchemaType

from ragnart import collection
from ragnart.collection import ensure_payload_indexes
from ragnart.schema import FIELD_OWNER, FIELD_SOURCE_PATH


@pytest.fixture(autouse=True)
def _clear_index_cache():
    # The cache is process-global; start every test with it empty.
    collection._payload_indexes_ready.clear()
    yield
    collection._payload_indexes_ready.clear()


def _fake_qdrant(payload_schema: dict) -> MagicMock:
    """Mock client whose get_collection() reports the given live payload_schema."""
    client = MagicMock(name="qdrant")
    client.get_collection.return_value = SimpleNamespace(payload_schema=payload_schema)
    return client


def _calls_by_field(client: MagicMock) -> dict:
    """Map field_name -> field_schema for every create_payload_index call."""
    return {
        c.kwargs["field_name"]: c.kwargs["field_schema"]
        for c in client.create_payload_index.call_args_list
    }


def test_ensure_payload_indexes_creates_all_fields_on_empty_collection():
    client = _fake_qdrant(payload_schema={})

    ensure_payload_indexes(client)

    # 6 keyword fields + 2 datetime fields.
    assert client.create_payload_index.call_count == 8
    calls = _calls_by_field(client)
    assert calls["modified_at"] == PayloadSchemaType.DATETIME
    assert calls["ingested_at"] == PayloadSchemaType.DATETIME
    for keyword_field in (
        "source_path",
        "project_id",
        "owner",
        "source_type",
        "file_type",
        "content_hash",
    ):
        assert calls[keyword_field] == PayloadSchemaType.KEYWORD


def test_ensure_payload_indexes_skips_fields_already_indexed():
    client = _fake_qdrant(payload_schema={FIELD_SOURCE_PATH: object(), FIELD_OWNER: object()})

    ensure_payload_indexes(client)

    # Only the 6 still-missing fields are created; source_path and owner are not.
    assert client.create_payload_index.call_count == 6
    created = {c.kwargs["field_name"] for c in client.create_payload_index.call_args_list}
    assert FIELD_SOURCE_PATH not in created
    assert FIELD_OWNER not in created


def test_ensure_payload_indexes_is_noop_when_all_cached():
    client = _fake_qdrant(payload_schema={})
    ensure_payload_indexes(client)
    client.reset_mock()

    # Second call in the same process: every field is cached, so no Qdrant I/O at all.
    ensure_payload_indexes(client)

    client.get_collection.assert_not_called()
    client.create_payload_index.assert_not_called()


def test_ensure_payload_indexes_fills_gap_left_by_partial_index():
    # Only source_path is indexed on the server. The other 7 fields must still be
    # created, i.e. a partly indexed collection does not short-circuit the call.
    partial = _fake_qdrant(payload_schema={FIELD_SOURCE_PATH: object()})

    ensure_payload_indexes(partial)

    assert partial.create_payload_index.call_count == 7
    assert FIELD_SOURCE_PATH not in _calls_by_field(partial)


def test_ensure_payload_indexes_returns_quietly_when_collection_missing():
    client = MagicMock(name="qdrant")
    client.get_collection.side_effect = Exception("collection not found")

    ensure_payload_indexes(client)

    client.create_payload_index.assert_not_called()
