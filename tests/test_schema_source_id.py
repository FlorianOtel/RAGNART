"""
Tests for the source-id constants and source_id_for_key() in ragnart/schema.py.

Pure functions only: no database, no Qdrant, no filesystem.
"""

from __future__ import annotations

from ragnart.schema import (
    FIELD_PIPELINE_VERSION,
    FIELD_SOURCE_ID,
    PIPELINE_VERSION_P1,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
    source_id_for_key,
)


def test_field_names_and_constants_have_the_documented_values():
    assert FIELD_SOURCE_ID == "source_id"
    assert FIELD_PIPELINE_VERSION == "pipeline_version"
    assert SOURCE_ID_NAS == "nas"
    assert SOURCE_ID_CLAUDE_LINUX_SHARED_NFS == "claude-linux-shared-nfs"
    assert SOURCE_ID_OPENCODE_K11 == "opencode-k11"
    assert PIPELINE_VERSION_P1 == "p1"


def test_nas_path_maps_to_nas_source():
    key = "/mnt/nfs/Florian/Gin-AI/projects/RAGNART/docs/notes.md"

    assert source_id_for_key(key) == SOURCE_ID_NAS


def test_claude_session_key_maps_to_claude_source():
    key = "claude-session://0f3c2a1e-1111-2222-3333-444455556666"

    assert source_id_for_key(key) == SOURCE_ID_CLAUDE_LINUX_SHARED_NFS


def test_claude_subagent_key_maps_to_claude_source():
    # A subagent transcript is keyed under its parent session: the prefix alone decides.
    key = "claude-session://0f3c2a1e-1111-2222-3333-444455556666/agent-x"

    assert source_id_for_key(key) == SOURCE_ID_CLAUDE_LINUX_SHARED_NFS


def test_opencode_key_maps_to_opencode_source():
    key = "opencode://ses_abc123"

    assert source_id_for_key(key) == SOURCE_ID_OPENCODE_K11
