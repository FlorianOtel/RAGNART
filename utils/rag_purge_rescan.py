#!/usr/bin/env python3
"""
Re-check existing rows against the current admission rules, and requeue transient failures.

This is a one-off repair tool. It is a sibling of utils/rag_purge_corrupt.py, not an
extension of it: that tool scans Qdrant payload text, while this one re-checks SQLite
`completed` rows against the admission rules in force today and against the file on disk.

Two actions, selected by flags:

  --rescan (default action)
      For every `completed` row that is still in scope:
        - pdf / docx: the stored parent_text of all its points is sampled. If the
          concatenated text is below GATE_THRESHOLD, the row is re-parsed (pending).
        - text-like files: size cap, NUL sniff, replacement-character ratio and
          printable gate are run on the file content, as ingest_file() does.
        - every type: the chunk cap is checked against the Qdrant point count.
      A row that fails a text-like or chunk check is marked ignored with the same
      skip_reason text ingest_file() would write.

      Rows are skipped (left for rag_sync_nfs.py to handle) when:
        - the extension is no longer in rag.scanner.include_extensions (e.g. .csv),
        - the row is not under a configured NFS root (claude chats, opencode sessions),
        - the file is missing on disk,
        - a .ragignore rule excludes the file.

  --requeue-transient
      Resets every `ignored` row whose skip_reason mentions a transient failure
      ("500 Internal Server Error" or "timed out") to pending, retry_count 0.

Safety:
  - Dry-run is the default. Without --apply nothing is written. Qdrant is only read,
    and SQLite is opened read-only (URI mode=ro), so StateDB is never constructed.
  - With --apply, each candidate's Qdrant points are deleted with wait=True before its
    SQLite row changes.

Usage (run from project root):
    python utils/rag_purge_rescan.py                          # dry-run rescan
    python utils/rag_purge_rescan.py --requeue-transient      # dry-run requeue only
    python utils/rag_purge_rescan.py --save /tmp/rescan.tsv   # also write candidates as TSV
    python utils/rag_purge_rescan.py --rescan --apply         # write the rescan changes
    python utils/rag_purge_rescan.py --requeue-transient --apply
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402

from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue  # noqa: E402

from ragnart.collection import DOCUMENTS_COLLECTION, get_client  # noqa: E402
# The admission definitions are imported, not copied, so this tool cannot drift from ingest.
from ragnart.ingest import fffd_ratio, is_text_like_path  # noqa: E402
# Single-writer lock: --apply is a writer, so it takes the same lock as sync and the daemon.
from ragnart.lock import acquire_or_exit, lock_path_from_config  # noqa: E402
from ragnart.ragignore import RagIgnoreMatcher  # noqa: E402
from ragnart.scanner import is_includable  # noqa: E402
from ragnart.schema import FIELD_PARENT_TEXT, FIELD_SOURCE_ID, FIELD_SOURCE_PATH, source_id_for_key  # noqa: E402
from ragnart.state import StateDB  # noqa: E402
from ragnart.textquality import GATE_THRESHOLD, printable_ratio, sniff_binary_text  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "ragnart.yaml"

# Types whose text comes from docling. Their text is only available as stored Qdrant
# parent_text, so the gate is judged by sampling those points.
_GATED_DOCLING_TYPES = frozenset({"pdf", "docx"})

# Qdrant reads can time out while the optimizer holds a segment lock, so reads are
# retried. The same policy as utils/rag_purge_corrupt.py.
_QDRANT_ATTEMPTS = 5
_SCROLL_PAGE_SIZE = 256

# skip_reason patterns that mark a failure as transient, for --requeue-transient.
TRANSIENT_PATTERNS = ("%500 Internal Server Error%", "%timed out%")

# How many sample paths are printed per category in the dry-run report.
SAMPLE_LIMIT = 20

# Action names stored on each Candidate.
ACTION_REPARSE = "reparse"
ACTION_IGNORE = "ignore"
ACTION_REQUEUE = "requeue"

# Category labels. They are the keys of the printed counts table, so keep them stable.
SKIP_NOT_INCLUDED = "skipped: not included (extension)"
SKIP_OUTSIDE_ROOTS = "skipped: outside NFS roots"
SKIP_MISSING = "skipped: file missing"
SKIP_RAGIGNORE = "skipped: .ragignore-excluded"
KEEP = "keep: passes admission"
REPARSE_GATE = "reparse: parsed text below gate"
IGNORE_TOO_LARGE_BYTES = "ignore: too large (bytes)"
IGNORE_NUL = "ignore: binary (NUL byte)"
IGNORE_FFFD = "ignore: binary (replacement chars)"
IGNORE_GATE = "ignore: binary content (printable)"
IGNORE_TOO_MANY_CHUNKS = "ignore: too many chunks"
REQUEUE_TRANSIENT = "requeue: transient failure"

T = TypeVar("T")


@dataclass(frozen=True)
class Admission:
    """
    The admission rules in force today, read from ragnart.yaml.

    Thresholds come from rag.admission, the same keys ingest_file() reads.
    """

    include_exts: frozenset[str]
    nfs_roots: tuple[str, ...]
    max_text_bytes: int
    fffd_threshold: float
    max_chunks: int


@dataclass(frozen=True)
class Candidate:
    """One row that --apply would change."""

    action: str       # ACTION_REPARSE, ACTION_IGNORE or ACTION_REQUEUE
    category: str     # one of the category labels above
    file_path: str
    points: int       # Qdrant points for this source_path when checked
    reason: str       # the skip_reason for "ignore"; an explanation for the other actions


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def admission_from_config(config: dict) -> Admission:
    """Build the Admission rules from a loaded ragnart.yaml. Fails loudly on missing keys."""
    rag = config["rag"]
    admission_cfg = rag.get("admission")
    if admission_cfg is None:
        raise ValueError("ragnart.yaml is missing the 'rag.admission' section.")

    required = ("max_text_file_size_bytes", "fffd_ratio_threshold", "max_chunks_per_item")
    missing = [key for key in required if key not in admission_cfg]
    if missing:
        raise ValueError(f"ragnart.yaml rag.admission is missing key(s): {', '.join(missing)}")

    # Every NFS root the scanner walks: per-user roots, then the shared root.
    roots: list[str] = []
    for user_cfg in config.get("users", {}).values():
        roots.extend(user_cfg.get("nfs_roots", []))
    roots.extend(config.get("shared", {}).get("nfs_roots", []))

    return Admission(
        include_exts=frozenset(rag["scanner"]["include_extensions"]),
        nfs_roots=tuple(roots),
        max_text_bytes=int(admission_cfg["max_text_file_size_bytes"]),
        fffd_threshold=float(admission_cfg["fffd_ratio_threshold"]),
        max_chunks=int(admission_cfg["max_chunks_per_item"]),
    )


# ---------------------------------------------------------------------------
# Qdrant access (reads retry, deletes do not)
# ---------------------------------------------------------------------------

def _source_filter(file_path: str) -> Filter:
    """
    Filter matching every point stored for one source file.

    Two conditions, both required: the source_id (derived from the key by source_id_for_key)
    AND the source_path. Matching on source_path alone could reach a point of another source
    that happens to carry the same path. Used by the reads (count, scroll) and the delete.
    """
    return Filter(
        must=[
            FieldCondition(key=FIELD_SOURCE_ID, match=MatchValue(value=source_id_for_key(file_path))),
            FieldCondition(key=FIELD_SOURCE_PATH, match=MatchValue(value=file_path)),
        ]
    )


def _call_with_retry(call: Callable[..., T], what: str, **kwargs: Any) -> T:
    """Call a Qdrant read, retrying on transient errors with a growing pause."""
    last_exc: Exception | None = None
    for attempt in range(1, _QDRANT_ATTEMPTS + 1):
        try:
            return call(**kwargs)
        except Exception as exc:  # qdrant_client raises several exception types
            last_exc = exc
            wait = 10 * attempt
            logger.warning(
                "%s attempt %d/%d failed: %s — retrying in %ds",
                what, attempt, _QDRANT_ATTEMPTS, exc, wait,
            )
            time.sleep(wait)
    # The loop ran at least once, so last_exc is set here.
    raise last_exc  # type: ignore[misc]


def count_points(client: Any, file_path: str) -> int:
    """
    Exact number of Qdrant points stored for source_path.

    One point is one child chunk, so this is the same number that the chunk cap in
    ingest_file() compares against.
    """
    result = _call_with_retry(
        client.count,
        "count",
        collection_name=DOCUMENTS_COLLECTION,
        count_filter=_source_filter(file_path),
        exact=True,
    )
    return int(result.count)


def parent_texts(client: Any, file_path: str) -> list[str]:
    """
    Every stored parent_text for source_path, read across all scroll pages.

    Vectors are not requested, so the read stays small.
    """
    texts: list[str] = []
    offset = None
    while True:
        points, offset = _call_with_retry(
            client.scroll,
            "scroll",
            collection_name=DOCUMENTS_COLLECTION,
            scroll_filter=_source_filter(file_path),
            limit=_SCROLL_PAGE_SIZE,
            offset=offset,
            with_payload=[FIELD_PARENT_TEXT],
            with_vectors=False,
        )
        for point in points:
            texts.append((point.payload or {}).get(FIELD_PARENT_TEXT, ""))
        if offset is None:
            return texts


def delete_points(client: Any, file_path: str) -> None:
    """Delete every point for source_path and wait until Qdrant confirms it."""
    client.delete(
        collection_name=DOCUMENTS_COLLECTION,
        points_selector=FilterSelector(filter=_source_filter(file_path)),
        wait=True,
    )


# ---------------------------------------------------------------------------
# SQLite access (read-only for the scan; StateDB only for --apply)
# ---------------------------------------------------------------------------

def read_completed_paths(db_path: str) -> list[str]:
    """Every file_path with status 'completed'. Opened read-only, so no WAL writes happen."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.execute(
            "SELECT file_path FROM ingestion_queue WHERE status = 'completed' ORDER BY file_path"
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def read_transient_rows(db_path: str) -> list[tuple[str, str]]:
    """(file_path, skip_reason) for every ignored row whose reason is a transient failure."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.execute(
            "SELECT file_path, skip_reason FROM ingestion_queue "
            "WHERE status = 'ignored' AND (skip_reason LIKE ? OR skip_reason LIKE ?) "
            "ORDER BY file_path",
            TRANSIENT_PATTERNS,
        )
        return [(row[0], row[1] or "") for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def _root_for(file_path: str, roots: tuple[str, ...]) -> str | None:
    """Return the configured NFS root that contains file_path. The longest root wins."""
    matches = [root for root in roots if file_path.startswith(root.rstrip("/") + "/")]
    return max(matches, key=len) if matches else None


def _check_text_like(file_path: str, admission: Admission) -> tuple[str, str] | None:
    """
    Run the text-like admission checks, in the same order as ingest_file().

    Returns (category, skip_reason) for the first failing check, or None if all pass.
    """
    # Size cap is checked from the filesystem, before the file is read.
    size_bytes = os.stat(file_path).st_size
    if size_bytes > admission.max_text_bytes:
        return (
            IGNORE_TOO_LARGE_BYTES,
            f"too large: {size_bytes} bytes > {admission.max_text_bytes} byte cap",
        )

    # NUL sniff on the raw bytes, before any decoding.
    nul_reason = sniff_binary_text(Path(file_path))
    if nul_reason is not None:
        return IGNORE_NUL, nul_reason

    # Decode exactly as ingest does, then apply the replacement-character and printable checks.
    text = Path(file_path).read_text(encoding="utf-8", errors="replace")
    replacement = fffd_ratio(text)
    if replacement > admission.fffd_threshold:
        return IGNORE_FFFD, f"binary: {replacement:.1%} replacement chars"

    printable = printable_ratio(text)
    if printable < GATE_THRESHOLD:
        return IGNORE_GATE, f"binary content: {printable:.0%} printable chars"

    return None


def classify_row(
    file_path: str,
    client: Any,
    admission: Admission,
    matchers: dict[str, RagIgnoreMatcher],
) -> tuple[str, Candidate | None]:
    """
    Decide one completed row's fate.

    Returns (category, candidate). The candidate is None when the row is kept or skipped.
    The skip checks run first and are cheap, so most rows never touch Qdrant.
    """
    # 1. Rows this tool leaves to the sync. Extension first, so .csv rows are counted
    #    as "not included" even while their files still exist on disk.
    if not is_includable(file_path, admission.include_exts):
        return SKIP_NOT_INCLUDED, None

    root = _root_for(file_path, admission.nfs_roots)
    if root is None:
        return SKIP_OUTSIDE_ROOTS, None

    if not os.path.isfile(file_path):
        return SKIP_MISSING, None

    # One matcher per root, cached, as the scanner builds one per root.
    matcher = matchers.get(root)
    if matcher is None:
        matcher = RagIgnoreMatcher(root)
        matchers[root] = matcher
    if matcher.is_path_excluded(file_path):
        return SKIP_RAGIGNORE, None

    # 2. Admission. Points are counted once and reused for the reason text.
    points = count_points(client, file_path)
    file_type = Path(file_path).suffix.lstrip(".").lower()

    if file_type in _GATED_DOCLING_TYPES:
        # Sample every point's parent_text. Repeated parents are counted once.
        distinct_texts = dict.fromkeys(parent_texts(client, file_path))
        ratio = printable_ratio("\n".join(distinct_texts))
        if ratio < GATE_THRESHOLD:
            # Re-parse rather than ignore: the parser may do better on a second attempt.
            # If it still fails, ingest_file() ignores the row with its own reason.
            return REPARSE_GATE, Candidate(
                action=ACTION_REPARSE,
                category=REPARSE_GATE,
                file_path=file_path,
                points=points,
                reason=f"parsed text {ratio:.0%} printable, below gate {GATE_THRESHOLD:.0%}",
            )

    elif is_text_like_path(file_path):
        failure = _check_text_like(file_path, admission)
        if failure is not None:
            category, reason = failure
            return category, Candidate(
                action=ACTION_IGNORE,
                category=category,
                file_path=file_path,
                points=points,
                reason=reason,
            )

    # 3. Chunk cap, applied to every type as ingest_file() applies it.
    if points > admission.max_chunks:
        reason = f"too large: {points} chunks > {admission.max_chunks} chunk cap"
        return IGNORE_TOO_MANY_CHUNKS, Candidate(
            action=ACTION_IGNORE,
            category=IGNORE_TOO_MANY_CHUNKS,
            file_path=file_path,
            points=points,
            reason=reason,
        )

    return KEEP, None


# ---------------------------------------------------------------------------
# Scans (read-only)
# ---------------------------------------------------------------------------

def scan_completed(
    db_path: str,
    client: Any,
    admission: Admission,
) -> tuple[Counter, list[Candidate]]:
    """Classify every completed row. Returns per-category counts and the candidates."""
    counts: Counter = Counter()
    candidates: list[Candidate] = []
    matchers: dict[str, RagIgnoreMatcher] = {}

    for file_path in read_completed_paths(db_path):
        category, candidate = classify_row(file_path, client, admission, matchers)
        counts[category] += 1
        if candidate is not None:
            candidates.append(candidate)

    return counts, candidates


def scan_transient(db_path: str, client: Any) -> tuple[Counter, list[Candidate]]:
    """List the ignored rows that --requeue-transient would reset."""
    counts: Counter = Counter()
    candidates: list[Candidate] = []

    for file_path, skip_reason in read_transient_rows(db_path):
        counts[REQUEUE_TRANSIENT] += 1
        candidates.append(
            Candidate(
                action=ACTION_REQUEUE,
                category=REQUEUE_TRANSIENT,
                file_path=file_path,
                points=count_points(client, file_path),
                reason=skip_reason,
            )
        )

    return counts, candidates


# ---------------------------------------------------------------------------
# Apply (writes)
# ---------------------------------------------------------------------------

def apply_candidates(state_db: StateDB, client: Any, candidates: list[Candidate]) -> Counter:
    """
    Carry out each candidate. Returns how many of each action was applied.

    For reparse and ignore, the Qdrant points are deleted with wait=True first, and the
    SQLite row changes only after that delete returns. Requeue rows hold no points, so
    they are only reset.
    """
    applied: Counter = Counter()
    for candidate in candidates:
        if candidate.action == ACTION_REQUEUE:
            state_db.reset_to_pending(candidate.file_path)
            applied[ACTION_REQUEUE] += 1
            continue

        delete_points(client, candidate.file_path)
        applied[f"{candidate.action}:points_deleted"] += candidate.points
        if candidate.action == ACTION_REPARSE:
            state_db.reset_to_pending(candidate.file_path)
        else:
            state_db.mark_ignored(candidate.file_path, candidate.reason)
        applied[candidate.action] += 1

    return applied


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def write_tsv(path: str, candidates: list[Candidate]) -> None:
    """Write the full candidate list, one row per candidate, tab-separated."""
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["action", "category", "points", "file_path", "reason"])
        for candidate in candidates:
            writer.writerow(
                [candidate.action, candidate.category, candidate.points,
                 candidate.file_path, candidate.reason]
            )


def print_report(title: str, counts: Counter, candidates: list[Candidate]) -> None:
    """Print the counts per category, then up to SAMPLE_LIMIT sample paths per category."""
    print(title)
    print("  Counts per category:")
    for category in sorted(counts):
        print(f"    {counts[category]:>8}  {category}")
    print()

    # Group candidates by category, keeping the scan order inside each group.
    groups: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        groups.setdefault(candidate.category, []).append(candidate)

    for category in sorted(groups):
        members = groups[category]
        print(f"  Samples: {category} (showing {min(len(members), SAMPLE_LIMIT)} of {len(members)})")
        for candidate in members[:SAMPLE_LIMIT]:
            print(f"    [{candidate.points:>5} pts]  {candidate.file_path}")
            print(f"                 reason: {candidate.reason}")
        print()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Re-check completed rows against current admission rules; requeue transient failures."
    )
    parser.add_argument(
        "--rescan",
        action="store_true",
        help="Re-check completed rows (default action when no action flag is given)",
    )
    parser.add_argument(
        "--requeue-transient",
        action="store_true",
        help="Reset ignored rows with a 500 or timed-out skip_reason to pending",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the changes. Without this flag the run is a dry-run.",
    )
    parser.add_argument(
        "--save",
        metavar="FILE",
        help="Write the full candidate list as TSV to FILE",
    )
    args = parser.parse_args(argv)

    # --rescan is the default action unless only --requeue-transient was asked for.
    run_rescan = args.rescan or not args.requeue_transient

    with open(CONFIG_PATH) as handle:
        config = yaml.safe_load(handle)
    admission = admission_from_config(config)
    db_path = f"{config['db_base_path']}/sqlite/rag_state.db"
    client = get_client(config["rag"]["qdrant_url"], timeout=120)

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== rag_purge_rescan ({mode}) ===")
    print(f"  SQLite: {db_path}")
    print(f"  Gate threshold: {GATE_THRESHOLD:.2f}  max text bytes: {admission.max_text_bytes}  "
          f"fffd threshold: {admission.fffd_threshold}  max chunks: {admission.max_chunks}")
    print()

    all_candidates: list[Candidate] = []

    if run_rescan:
        counts, candidates = scan_completed(db_path, client, admission)
        print_report("Rescan of completed rows", counts, candidates)
        all_candidates.extend(candidates)

    if args.requeue_transient:
        counts, candidates = scan_transient(db_path, client)
        print_report("Requeue of transient failures", counts, candidates)
        all_candidates.extend(candidates)

    print(f"Total candidates: {len(all_candidates)}")
    if args.save:
        write_tsv(args.save, all_candidates)
        print(f"Saved candidate list to {args.save}")
    print()

    if not args.apply:
        print("(dry-run: nothing written. Re-run with --apply to carry out the candidates.)")
        return

    # Writes go through StateDB only here, after the read-only scans are complete.
    #
    # This is the first point where the run writes, so the single-writer lock is taken
    # here and nowhere earlier. Dry-runs returned above and never reach this line, so
    # a dry-run never touches the lock file and never needs rag.ingest_lock.
    #
    # On contention, acquire_or_exit() prints the current holder to stderr and exits
    # with EXIT_LOCK_HELD (75) before any Qdrant delete or SQLite write happens. It
    # does not wait. The lock is held until this process exits, which covers the
    # whole of apply_candidates().
    acquire_or_exit(lock_path_from_config(config))

    state_db = StateDB(db_path)
    try:
        applied = apply_candidates(state_db, client, all_candidates)
    finally:
        state_db.close()

    print("Applied:")
    for key in sorted(applied):
        print(f"  {applied[key]:>8}  {key}")


if __name__ == "__main__":
    main()
