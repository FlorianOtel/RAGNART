"""
Hub-scope regression test for utils/rag_purge_rescan.py (v0.3, session B1, A.7).

The rescan is driven by the completed rows of rag_state.db, and those rows are hub-only by
construction. It never enumerates Qdrant: every read and delete is keyed on a queue row's path.
So an endpoint point, which has no queue row here, is never a candidate, even when its text
would fail the gate. This test pins that behaviour. No code change was needed for it.

The fake Qdrant applies the item filter (source_id and source_path), so a delete on the wrong
path would show in the recorded deletes. Everything runs on temporary files.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from ragnart.schema import FIELD_PARENT_TEXT, FIELD_SOURCE_ID, FIELD_SOURCE_PATH
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent
OWNER = "tester"

# Text that the printable gate rejects: control characters only.
GARBAGE = "\x01\x02\x03\x04\x05" * 40
# Text that passes every gate.
CLEAN = "A plain sentence with ordinary words and punctuation.\n" * 5

ENDPOINT_PATH = "/Users/fotel/MBP/notes.md"


@pytest.fixture
def rescan(monkeypatch):
    """Load utils/rag_purge_rescan.py as a module. It is a script, not a package."""
    name = "rag_purge_rescan"
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "utils" / "rag_purge_rescan.py")
    module = importlib.util.module_from_spec(spec)
    # Register before executing: the dataclasses in the module look themselves up in sys.modules.
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


class ItemQdrant:
    """
    Fake collection of points, each a payload dict. count, scroll and delete apply the item
    filter (MatchValue on source_id and source_path). delete records the path it removed.
    """

    def __init__(self, points: list[dict]) -> None:
        self.points = list(points)
        self.deleted: list[str] = []

    @staticmethod
    def _matches(payload: dict, flt) -> bool:
        return all(payload.get(c.key) == c.match.value for c in flt.must)

    def count(self, collection_name, count_filter=None, exact=True):
        matching = [p for p in self.points if self._matches(p, count_filter)]
        return SimpleNamespace(count=len(matching))

    def scroll(self, collection_name, scroll_filter=None, limit=10, offset=None,
               with_payload=None, with_vectors=False):
        matching = [p for p in self.points if self._matches(p, scroll_filter)]
        return [SimpleNamespace(payload=p) for p in matching], None

    def delete(self, collection_name, points_selector, wait=True):
        flt = points_selector.filter
        path = next(c.match.value for c in flt.must if c.key == FIELD_SOURCE_PATH)
        self.deleted.append(path)
        self.points = [p for p in self.points if not self._matches(p, flt)]


def _config(root: Path) -> dict:
    """A ragnart.yaml-shaped dict with one NFS root, markdown only, and generous caps."""
    return {
        "db_base_path": str(root.parent / "db"),
        "rag": {
            "qdrant_url": "http://qdrant.invalid:6333",
            "scanner": {"include_extensions": [".md"]},
            "admission": {
                "max_text_file_size_bytes": 100_000,
                "max_chunks_per_item": 5000,
                "fffd_ratio_threshold": 0.005,
            },
        },
        "users": {"tester@example.com": {"owner": OWNER, "nfs_roots": [str(root)]}},
    }


def _point(source_id: str, path: str, text: str) -> dict:
    return {FIELD_SOURCE_ID: source_id, FIELD_SOURCE_PATH: path, FIELD_PARENT_TEXT: text}


def test_rescan_never_touches_an_endpoint_point_even_when_its_text_is_corrupt(rescan, tmp_path):
    # A NAS file whose stored text is corrupt, so the rescan does act on a hub row.
    root = tmp_path / "nfs" / "Florian"
    root.mkdir(parents=True)
    nas_file = root / "bad.md"
    nas_file.write_text(GARBAGE)
    db = StateDB(str(tmp_path / "db" / "sqlite" / "rag_state.db"))
    try:
        db.discover_or_update(str(nas_file), OWNER, 1.0)
        db.mark_processing(str(nas_file))
        db.mark_completed(str(nas_file))

        # Each path's stored text is corrupt. The endpoint point has no queue row at all.
        qdrant = ItemQdrant([
            _point("nas", str(nas_file), GARBAGE),
            _point("MBP", ENDPOINT_PATH, GARBAGE),
        ])
        admission = rescan.admission_from_config(_config(root))

        _, candidates = rescan.scan_completed(db._db_path, qdrant, admission)
        rescan.apply_candidates(db, qdrant, candidates)

        assert [c.file_path for c in candidates] == [str(nas_file)]
        assert qdrant.deleted == [str(nas_file)]
        assert [p[FIELD_SOURCE_PATH] for p in qdrant.points] == [ENDPOINT_PATH]
    finally:
        db.close()
