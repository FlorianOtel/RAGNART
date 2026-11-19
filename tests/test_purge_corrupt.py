"""
Tests for utils/rag_purge_corrupt.py, the read-only corrupt-content report (session B2, A.15).

The tool scans every source by default, narrows with --source and --user, and never writes. The fake Qdrant
applies the scroll filter the way the server does, so a filter that leaves a point out shows up as a missing
report line. Nothing live is touched: Qdrant is a fake, and the queue database is a temporary file.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from ragnart.state import StateDB
from ragnart.schema import FIELD_OWNER, FIELD_PARENT_TEXT, FIELD_SOURCE_ID, FIELD_SOURCE_PATH

REPO_ROOT = Path(__file__).resolve().parent.parent

# Text that the printable gate rejects, so a point carrying it is corrupt.
GARBAGE = "\x01\x02\x03\x04\x05" * 40
CLEAN = "A perfectly ordinary sentence about the backup rotation schedule."
THRESHOLD = 0.8

NAS_PATH = "/nas/Florian/gone.pdf"
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
        if payload.get(condition.key) != condition.match.value:
            return False
    return True


class FilteringQdrant:
    """
    Fake collection: a list of point payloads, returned in one page by scroll().

    scroll() records the filter it was given, so a test can assert the conditions sent. It has no delete method:
    a call to one would raise AttributeError, which the read-only tests rely on.
    """

    def __init__(self, points: list[dict]) -> None:
        self.points = list(points)
        self.last_filter = None

    def scroll(self, collection_name, scroll_filter=None, limit=10, offset=None,
               with_payload=None, with_vectors=False):
        self.last_filter = scroll_filter
        matching = [p for p in self.points if _matches(p, scroll_filter)]
        return [SimpleNamespace(payload=p) for p in matching], None


@pytest.fixture
def purge_corrupt():
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

def test_scan_with_no_filter_reports_every_source(purge_corrupt):
    """Every source is scanned by default: a corrupt NAS point and a corrupt endpoint point are both reported."""
    client = FilteringQdrant([_point("nas", NAS_PATH, GARBAGE), _point("MBP", ENDPOINT_PATH, GARBAGE)])

    result = purge_corrupt.scan_qdrant_for_corrupt(client, THRESHOLD)

    assert set(result["corrupt_paths"]) == {NAS_PATH, ENDPOINT_PATH}
    assert result["total_points"] == 2
    assert result["total_corrupt_points"] == 2


def test_scan_with_no_filter_sends_no_scroll_filter(purge_corrupt):
    client = FilteringQdrant([])

    purge_corrupt.scan_qdrant_for_corrupt(client, THRESHOLD)

    assert client.last_filter is None


def test_source_filter_is_the_only_condition_and_reports_that_source_alone(purge_corrupt):
    client = FilteringQdrant([_point("nas", NAS_PATH, GARBAGE), _point("MBP", ENDPOINT_PATH, GARBAGE)])

    result = purge_corrupt.scan_qdrant_for_corrupt(client, THRESHOLD, source_filter="nas")

    assert [c.key for c in client.last_filter.must] == [FIELD_SOURCE_ID]
    assert client.last_filter.must[0].match.value == "nas"
    assert set(result["corrupt_paths"]) == {NAS_PATH}


def test_user_and_source_filters_are_anded(purge_corrupt):
    client = FilteringQdrant([
        _point("nas", NAS_PATH, GARBAGE, owner="florian"),
        _point("nas", "/nas/Eva/other.pdf", GARBAGE, owner="eva"),
        _point("MBP", ENDPOINT_PATH, GARBAGE, owner="florian"),
    ])

    result = purge_corrupt.scan_qdrant_for_corrupt(client, THRESHOLD, user_filter="florian", source_filter="nas")

    assert [c.key for c in client.last_filter.must] == [FIELD_SOURCE_ID, FIELD_OWNER]
    assert set(result["corrupt_paths"]) == {NAS_PATH}


def test_clean_points_are_not_reported(purge_corrupt):
    client = FilteringQdrant([_point("nas", NAS_PATH, CLEAN), _point("nas", "/nas/x.md", GARBAGE)])

    result = purge_corrupt.scan_qdrant_for_corrupt(client, THRESHOLD)

    assert set(result["corrupt_paths"]) == {"/nas/x.md"}
    assert result["total_corrupt_points"] == 1


# ---------------------------------------------------------------------------
# main(): a report only, never a write, and no lock
# ---------------------------------------------------------------------------

@pytest.fixture
def main_env(purge_corrupt, tmp_path, monkeypatch):
    """
    Point main() at a temporary database base with a fake Qdrant that holds one corrupt NAS point and one corrupt
    endpoint point. The config is replaced, so the live ragnart.yaml values are never used.
    """
    StateDB(str(tmp_path / "sqlite" / "rag_state.db")).close()
    client = FilteringQdrant([_point("nas", NAS_PATH, GARBAGE), _point("MBP", ENDPOINT_PATH, GARBAGE)])
    config = {"db_base_path": str(tmp_path), "rag": {"qdrant_url": "http://unused:6333"}}
    monkeypatch.setattr(purge_corrupt.yaml, "safe_load", lambda handle: config)
    monkeypatch.setattr(purge_corrupt, "get_client", lambda url, timeout=60: client)
    return client


def test_report_lists_every_corrupt_path_and_deletes_nothing(purge_corrupt, main_env, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py"])

    purge_corrupt.main()

    out = capsys.readouterr().out
    assert NAS_PATH in out
    assert ENDPOINT_PATH in out
    assert "Read-only report: nothing was deleted." in out
    # The fake has no delete method, so a delete would have raised before this line.
    assert len(main_env.points) == 2


def test_source_flag_limits_the_report_and_the_scope_line_says_so(purge_corrupt, main_env, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py", "--source", "nas"])

    purge_corrupt.main()

    out = capsys.readouterr().out
    assert "Scope: source nas" in out
    assert NAS_PATH in out
    assert ENDPOINT_PATH not in out


@pytest.mark.parametrize("retired_flag", ["--confirm", "--force"])
def test_the_retired_write_flags_are_refused_as_usage_errors(purge_corrupt, main_env, monkeypatch, retired_flag):
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py", retired_flag])

    with pytest.raises(SystemExit) as exc_info:
        purge_corrupt.main()

    assert exc_info.value.code == 2


def test_the_tool_has_no_lock_and_no_registry_read(purge_corrupt):
    """The read-only tool imports neither the single-writer lock nor the hub registry."""
    assert not hasattr(purge_corrupt, "acquire_or_exit")
    assert not hasattr(purge_corrupt, "open_registry_read_only")
    assert not hasattr(purge_corrupt, "file_exists")


def test_save_writes_the_corrupt_paths_one_per_line(purge_corrupt, main_env, monkeypatch, tmp_path):
    save_to = tmp_path / "corrupt.txt"
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py", "--save", str(save_to)])

    purge_corrupt.main()

    assert save_to.read_text(encoding="utf-8").splitlines() == sorted([NAS_PATH, ENDPOINT_PATH])
