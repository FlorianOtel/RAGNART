"""
Hub-scope tests for utils/rag_purge_corrupt.py (v0.3, session B1, A.7).

The scan covers the hub's own sources only, and the scroll filter always carries the hub
condition (MatchAny on source_id), ANDed with --user when that is given. The fake Qdrant applies
the scroll filter the way the server does, so a missing hub condition would show up as a
reported or deleted endpoint point.

Everything runs on a temporary database base with a seeded registry. No Qdrant server is
contacted, and the live queue and registry are never opened.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ragnart.registry import RegistryDB
from ragnart.schema import FIELD_OWNER, FIELD_PARENT_TEXT, FIELD_SOURCE_ID, FIELD_SOURCE_PATH
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent

# Text that the printable gate rejects, so a point carrying it is corrupt.
GARBAGE = "\x01\x02\x03\x04\x05" * 40
THRESHOLD = 0.8

HUB = {"nas", "claude-linux-shared-nfs", "opencode-k11"}

# A hub path that does not exist on disk, so the --confirm safety check does not skip it.
HUB_PATH = "/nas/Florian/gone.pdf"
# An endpoint path, with the endpoint's source id.
ENDPOINT_PATH = "/Users/fotel/MBP/gone.md"


def _point(source_id: str, path: str, text: str, owner: str = "florian") -> dict:
    """The payload of one point, as the scroll returns it."""
    return {
        FIELD_SOURCE_ID: source_id,
        FIELD_SOURCE_PATH: path,
        FIELD_PARENT_TEXT: text,
        FIELD_OWNER: owner,
    }


def _matches(payload: dict, flt) -> bool:
    """True when the payload satisfies every condition of the filter. None matches everything."""
    if flt is None:
        return True
    for condition in flt.must:
        value = payload.get(condition.key)
        if hasattr(condition.match, "any"):  # MatchAny
            if value not in condition.match.any:
                return False
        elif value != condition.match.value:  # MatchValue
            return False
    return True


def _value(flt, key: str):
    """The value of the one condition on `key` in a delete selector's filter."""
    return next(c.match.value for c in flt.must if c.key == key)


class ScopedQdrant:
    """
    Fake collection: a list of point payloads.

    scroll() returns the points matching its scroll_filter, in one page, and remembers the last
    filter it was given. delete() removes the points matching its selector, and records the
    (source_id, source_path) pair it deleted.
    """

    def __init__(self, points: list[dict]) -> None:
        self.points = list(points)
        self.deleted: list[tuple[str, str]] = []
        self.last_filter = None

    def scroll(self, collection_name, scroll_filter=None, limit=10, offset=None,
               with_payload=None, with_vectors=False):
        self.last_filter = scroll_filter
        matching = [p for p in self.points if _matches(p, scroll_filter)]
        return [SimpleNamespace(payload=p) for p in matching], None

    def delete(self, collection_name, points_selector, wait=True):
        flt = points_selector.filter
        source_id = _value(flt, FIELD_SOURCE_ID)
        path = _value(flt, FIELD_SOURCE_PATH)
        self.deleted.append((source_id, path))
        self.points = [
            p for p in self.points
            if not (p[FIELD_SOURCE_ID] == source_id and p[FIELD_SOURCE_PATH] == path)
        ]


@pytest.fixture
def purge_corrupt(monkeypatch):
    """Load utils/rag_purge_corrupt.py by file path. utils/ is not a package."""
    spec = importlib.util.spec_from_file_location(
        "rag_purge_corrupt", REPO_ROOT / "utils" / "rag_purge_corrupt.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# scan_qdrant_for_corrupt
# ---------------------------------------------------------------------------

def test_scan_reports_a_corrupt_hub_point_and_not_an_endpoint_one(purge_corrupt):
    client = ScopedQdrant([_point("nas", HUB_PATH, GARBAGE), _point("MBP", ENDPOINT_PATH, GARBAGE)])

    result = purge_corrupt.scan_qdrant_for_corrupt(client, THRESHOLD, HUB)

    assert set(result["corrupt_paths"]) == {HUB_PATH}
    assert result["total_points"] == 1


def test_scan_filter_always_names_the_hub_sources(purge_corrupt):
    client = ScopedQdrant([])

    purge_corrupt.scan_qdrant_for_corrupt(client, THRESHOLD, HUB)

    assert [c.key for c in client.last_filter.must] == [FIELD_SOURCE_ID]
    assert client.last_filter.must[0].match.any == sorted(HUB)


def test_user_filter_is_anded_with_the_hub_condition(purge_corrupt):
    client = ScopedQdrant([
        _point("nas", HUB_PATH, GARBAGE, owner="florian"),
        _point("nas", "/nas/Eva/other.pdf", GARBAGE, owner="eva"),
    ])

    result = purge_corrupt.scan_qdrant_for_corrupt(client, THRESHOLD, HUB, user_filter="florian")

    assert [c.key for c in client.last_filter.must] == [FIELD_SOURCE_ID, FIELD_OWNER]
    assert set(result["corrupt_paths"]) == {HUB_PATH}


def test_scan_refuses_an_empty_hub_scope(purge_corrupt):
    with pytest.raises(ValueError, match="hub_source_ids is empty"):
        purge_corrupt.scan_qdrant_for_corrupt(ScopedQdrant([]), THRESHOLD, set())


# ---------------------------------------------------------------------------
# main(): the hub ids come from the registry; --confirm deletes hub points only
# ---------------------------------------------------------------------------

@pytest.fixture
def main_env(purge_corrupt, tmp_path, monkeypatch):
    """
    Point main() at a temporary database base with a seeded registry and a fake Qdrant client.

    The client holds one corrupt hub point and one corrupt endpoint point. The lock is replaced,
    since only the lock tests in test_purge_corrupt_lock.py exercise it.
    """
    db_base = tmp_path / "db"
    StateDB(str(db_base / "sqlite" / "rag_state.db")).close()
    RegistryDB(str(db_base / "sqlite" / "hub_registry.db")).close()
    client = ScopedQdrant([_point("nas", HUB_PATH, GARBAGE), _point("MBP", ENDPOINT_PATH, GARBAGE)])
    config = {
        "db_base_path": str(db_base),
        "rag": {
            "qdrant_url": "http://unused:6333",
            "ingest_lock": str(db_base / "rag-ingest.lock"),
        },
    }
    monkeypatch.setattr(purge_corrupt.yaml, "safe_load", lambda handle: config)
    monkeypatch.setattr(purge_corrupt, "get_client", lambda url, timeout=60: client)
    monkeypatch.setattr(purge_corrupt, "acquire_or_exit", MagicMock(name="acquire_or_exit"))
    return client


def test_dry_run_reports_no_endpoint_path(purge_corrupt, main_env, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py"])

    purge_corrupt.main()

    out = capsys.readouterr().out
    assert HUB_PATH in out
    assert ENDPOINT_PATH not in out
    assert main_env.deleted == []


def test_confirm_deletes_the_corrupt_hub_point_and_leaves_the_endpoint_point(
    purge_corrupt, main_env, monkeypatch
):
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py", "--confirm"])

    purge_corrupt.main()

    assert main_env.deleted == [("nas", HUB_PATH)]
    assert [p[FIELD_SOURCE_PATH] for p in main_env.points] == [ENDPOINT_PATH]
