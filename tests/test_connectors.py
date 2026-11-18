"""
Tests for ragnart/connectors.py: ids, the capability table, function identity,
and the one-directional import rule.

No database, Qdrant or OpenCode is contacted. The import-rule test runs in a
subprocess so that modules already imported by other tests cannot hide a cycle.
"""

from __future__ import annotations

import dataclasses
import subprocess
import sys
from pathlib import Path

import pytest

import ragnart.ingest
import ragnart.scanner
from ragnart.connectors import (
    BY_SOURCE_ID,
    CAPABILITY_RAGNART_PROJECTS,
    CLAUDE_CODE,
    FILESYSTEM,
    OPENCODE,
)
from ragnart.schema import (
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
    SOURCES_HOLDING_PROJECTS,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_each_connector_carries_its_source_id_and_type():
    assert (FILESYSTEM.source_id, FILESYSTEM.connector_type) == (SOURCE_ID_NAS, "filesystem")
    assert (CLAUDE_CODE.source_id, CLAUDE_CODE.connector_type) == (
        SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
        "claude_code",
    )
    assert (OPENCODE.source_id, OPENCODE.connector_type) == (SOURCE_ID_OPENCODE_K11, "opencode")


def test_by_source_id_maps_each_source_id_to_its_connector():
    assert BY_SOURCE_ID == {
        SOURCE_ID_NAS: FILESYSTEM,
        SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: CLAUDE_CODE,
        SOURCE_ID_OPENCODE_K11: OPENCODE,
    }


def test_capability_table_only_the_filesystem_connector_holds_projects():
    assert FILESYSTEM.capabilities == frozenset({"exclusion", "ragnart_projects", "location"})
    assert CLAUDE_CODE.capabilities == frozenset()
    assert OPENCODE.capabilities == frozenset()


def test_ragnart_projects_capability_constant_matches_its_name():
    assert CAPABILITY_RAGNART_PROJECTS == "ragnart_projects"
    assert CAPABILITY_RAGNART_PROJECTS in FILESYSTEM.capabilities


def test_connector_enumerate_and_fetch_are_the_existing_functions_by_reference():
    # The connectors wrap existing code; they do not copy it.
    assert FILESYSTEM.enumerate is ragnart.scanner.scan_nfs_roots
    assert CLAUDE_CODE.enumerate is ragnart.scanner.scan_claude_chats
    assert OPENCODE.enumerate is ragnart.scanner.scan_opencode_sessions

    assert FILESYSTEM.fetch is ragnart.ingest._parse_to_text
    assert CLAUDE_CODE.fetch is ragnart.ingest._parse_claude_chat
    assert OPENCODE.fetch is ragnart.ingest._parse_opencode_session


def test_every_contract_element_is_present_and_non_empty():
    # The seven elements of design §4.7 are all set for every connector.
    for connector in (FILESYSTEM, CLAUDE_CODE, OPENCODE):
        assert connector.key_format
        assert connector.locator_format
        assert connector.display_format
        assert connector.completeness_signal
        assert connector.duplicate_rule
        assert callable(connector.enumerate)
        assert callable(connector.fetch)


def test_connector_is_frozen():
    # A connector is a fixed description; nothing may reassign its fields at run time.
    with pytest.raises(dataclasses.FrozenInstanceError):
        FILESYSTEM.source_id = "other"


def test_importing_scanner_or_ingest_does_not_import_connectors():
    # The one-directional rule: only connectors.py imports scanner and ingest.
    # A fresh interpreter, so that modules imported by other tests cannot hide a cycle.
    code = (
        "import sys\n"
        "import ragnart.scanner\n"
        "import ragnart.ingest\n"
        "assert 'ragnart.connectors' not in sys.modules, 'scanner/ingest imported connectors'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr


def test_sources_holding_projects_matches_the_connector_table():
    # schema.SOURCES_HOLDING_PROJECTS is a copy of the capability table, kept in schema.py so
    # that scope.py need not import connectors.py. This test fails if the two drift apart.
    from_connectors = {
        connector.source_id
        for connector in BY_SOURCE_ID.values()
        if CAPABILITY_RAGNART_PROJECTS in connector.capabilities
    }
    assert SOURCES_HOLDING_PROJECTS == from_connectors


def test_importing_the_search_service_loads_neither_ingest_nor_docling():
    # The search service must not load the ingestion code (docling, the parsers).
    # A fresh interpreter, so that modules imported by other tests cannot hide a leak.
    code = (
        "import sys\n"
        "import ragnart.service\n"
        "assert 'ragnart.ingest' not in sys.modules, 'service imported ragnart.ingest'\n"
        "assert 'ragnart.connectors' not in sys.modules, 'service imported ragnart.connectors'\n"
        "assert 'docling' not in sys.modules, 'service loaded docling'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
