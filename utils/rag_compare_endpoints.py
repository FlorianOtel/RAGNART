"""
rag_compare_endpoints.py — old-vs-new retrieval regression check (tie-aware).

Compares SoHoAI's legacy endpoint (GET /v1/rag/search on :8000) with RAGNART's
endpoint (GET /v1/search on :8100) for every query in
utils/rag_bench_queries.txt.

Why ordered equality is NOT used
--------------------------------
An earlier version required the ORDERED list of source_path values to be
identical, plus identical scores. That criterion cannot be met on this stack,
for two reasons that both pre-date RAGNART (the code lives in SoHoAI's
search.py / rerank.py, which are byte-identical in RAGNART):

  1. Qdrant's RRF fusion breaks exact score ties inconsistently between calls.
     Two results with the same score can swap ranks from one request to the
     next, even when both requests go to the SAME endpoint.
  2. The cross-encoder reranker's relevance scores shift by roughly 1e-3..1e-2
     depending on which candidates share its batch, so near-ties flip order.

The CONTROL run (OLD vs OLD, identical requests) shows this noise floor
directly. Fixing the non-determinism would change production behaviour and is
out of scope for v0.1; it is recorded as an open item for v0.2+.

What is checked
---------------
HARD (gate, rerank=false). Per query:
  - The MULTISET of `score` values is identical (tolerance TOLERANCE).
  - Within every group of equal scores, the SET of `source_path` values is
    identical. The group cut by top_k (the last group, when the response holds
    exactly top_k results) is checked for count and score only, because which
    of its tied members make the cut is not deterministic.
  This catches real drift in scores or in WHICH documents are returned, while
  ignoring the order in which tied documents appear.

INFO (rerank=true, not a gate). Per query:
  - top-1 equality: same source_path at rank 1.
  - overlap: |intersection of source_path sets| / top_k.

CONTROL (OLD vs OLD, not a gate). The same HARD and INFO checks, with the
second request going to the OLD endpoint again. It shows the noise floor of the
legacy endpoint itself, so INFO and HARD numbers can be read against it.

Pass = every HARD check passes. Exit code 0 iff all HARD checks pass.

Query file format
-----------------
One query per line, formatted as `query | expected_path_substring`.
Lines that are blank, or whose first non-space character is `#`, are skipped.
Only the text LEFT of the LAST " | " is used as the query; the expected
substring is NOT asserted here (it belongs to the quality bench, not to this
old-vs-new comparison).

Usage
-----
    python utils/rag_compare_endpoints.py
    python utils/rag_compare_endpoints.py --user florian --top-k 5
    python utils/rag_compare_endpoints.py --old-url http://HOST:8000/v1/rag/search \
        --new-url http://HOST:8100/v1/search

Exit code: 0 if every HARD check passes, 1 otherwise.
"""

import argparse
import json
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

DEFAULT_OLD_URL = "http://192.168.1.93:8000/v1/rag/search"  # SoHoAI (legacy)
DEFAULT_NEW_URL = "http://192.168.1.95:8100/v1/search"      # RAGNART (new)
DEFAULT_USER = "florian"
DEFAULT_TOP_K = 5

# Scope sent with every RAGNART GET /v1/search request. `all` covers every ragnart
# project and the global scope; the endpoint rejects a request without a scope (HTTP 422).
SEARCH_SCOPE = "all"

# Reranked calls can be slow (cross-encoder over the candidate pool), so the
# timeout is generous compared with a plain Qdrant query.
TIMEOUT_SECONDS = 120

# Two scores closer than this are treated as equal (tie groups, multiset check).
TOLERANCE = 1e-6

# Query file lives next to this script, so the tool works from any cwd.
QUERY_FILE = Path(__file__).resolve().parent / "rag_bench_queries.txt"


def parse_queries(path: Path) -> list[str]:
    """Return the real queries from the bench file, in file order.

    Skips blank lines and comment lines (first non-space char is '#').
    For data lines, keeps the text left of the last ' | ' separator.
    """
    queries: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # rsplit on the LAST " | " so a query that itself contains "|" survives.
        query_part = line.rsplit(" | ", 1)[0].strip()
        if query_part:
            queries.append(query_part)
    return queries


def fetch_results(base_url: str, query: str, user: str, top_k: int, rerank: bool) -> list[dict]:
    """GET base_url?q=...&user=...&top_k=...&rerank=... and return the `results` list.

    Both endpoints accept the `rerank` boolean query parameter; it is always sent
    explicitly so the comparison never depends on the server-side config default.

    RAGNART's GET /v1/search requires `scope` (no default; without it the service
    answers 422), so every request to a `/v1/search` URL carries `scope=all`. The
    legacy SoHoAI `/v1/rag/search` takes no scope and its request is left unchanged.
    The rule follows the URL path, not the role (old or new), so the tool also
    works when both URLs point at RAGNART.
    """
    query_params = {
        "q": query,
        "user": user,
        "top_k": top_k,
        "rerank": "true" if rerank else "false",
    }
    if urllib.parse.urlsplit(base_url).path.endswith("/v1/search"):
        query_params["scope"] = SEARCH_SCOPE
    params = urllib.parse.urlencode(query_params)
    url = f"{base_url}?{params}"
    with urllib.request.urlopen(url, timeout=TIMEOUT_SECONDS) as resp:
        body = json.loads(resp.read())
    return body.get("results", [])


def group_by_score(results: list[dict]) -> list[list[dict]]:
    """Split a score-ordered result list into runs of (near-)equal scores.

    The endpoints return results sorted by score, so equal scores are adjacent.
    Each run is one 'tie group'.
    """
    groups: list[list[dict]] = []
    for result in results:
        score = float(result.get("score", 0.0))
        if groups and abs(float(groups[-1][0].get("score", 0.0)) - score) <= TOLERANCE:
            groups[-1].append(result)
        else:
            groups.append([result])
    return groups


def hard_check(old: list[dict], new: list[dict], top_k: int) -> tuple[bool, str]:
    """Tie-aware equality of two rerank=false result lists.

    Returns (passed, human-readable detail).
    """
    if len(old) != len(new):
        return False, f"result count differs: old={len(old)} new={len(new)}"

    # Step 1: the multiset of scores must match. Both sides are sorted
    # descending first, so the comparison does not depend on the order the
    # endpoint returned tied results in.
    old_scores = sorted((float(r.get("score", 0.0)) for r in old), reverse=True)
    new_scores = sorted((float(r.get("score", 0.0)) for r in new), reverse=True)
    for position, (a, b) in enumerate(zip(old_scores, new_scores), 1):
        if abs(a - b) > TOLERANCE:
            return False, f"score multiset differs (sorted position {position}): old={a} new={b}"

    if not old:
        return True, "both endpoints returned 0 results"

    # Step 2: within each tie group, the set of documents must match.
    old_groups = group_by_score(old)
    new_groups = group_by_score(new)
    if len(old_groups) != len(new_groups):
        return False, f"tie-group structure differs: old={len(old_groups)} new={len(new_groups)}"

    # Only the LAST group can be cut by top_k, and only when the response is
    # full. Which of its tied members survive the cut is not deterministic, so
    # it is checked for count and score only.
    response_is_full = len(old) == top_k
    last_group_index = len(old_groups) - 1
    set_checked = 0
    count_only = 0

    for g, (old_group, new_group) in enumerate(zip(old_groups, new_groups)):
        score = float(old_group[0].get("score", 0.0))
        if len(old_group) != len(new_group):
            return False, (
                f"tie group at score {score:.6g}: size differs "
                f"old={len(old_group)} new={len(new_group)}"
            )
        if response_is_full and g == last_group_index:
            count_only += 1
            continue

        old_set = {r.get("source_path", "") for r in old_group}
        new_set = {r.get("source_path", "") for r in new_group}
        if old_set != new_set:
            only_old = sorted(old_set - new_set)
            only_new = sorted(new_set - old_set)
            return False, (
                f"tie group at score {score:.6g}: membership differs "
                f"old-only={only_old} new-only={only_new}"
            )
        set_checked += 1

    return True, (
        f"scores identical; {set_checked} tie group(s) set-equal; "
        f"{count_only} cut group(s) count-only"
    )


def info_check(a: list[dict], b: list[dict], top_k: int) -> tuple[bool, float]:
    """Non-gating comparison of two rerank=true result lists.

    Returns (top-1 equal, |source_path set intersection| / top_k).
    """
    a_paths = [r.get("source_path", "") for r in a]
    b_paths = [r.get("source_path", "") for r in b]
    top1_equal = a_paths[:1] == b_paths[:1]
    overlap = len(set(a_paths) & set(b_paths)) / top_k
    return top1_equal, overlap


@dataclass
class QueryRow:
    """All check outcomes for one query."""
    index: int
    query: str
    hard_ok: bool = False          # OLD vs NEW, rerank=false (gate)
    hard_detail: str = ""
    ctl_hard_ok: bool = False      # OLD vs OLD, rerank=false (control)
    ctl_hard_detail: str = ""
    info_top1: bool = False        # OLD vs NEW, rerank=true
    info_overlap: float = 0.0
    ctl_top1: bool = False         # OLD vs OLD, rerank=true
    ctl_overlap: float = 0.0


def run_query(index: int, query: str, old_url: str, new_url: str, user: str, top_k: int) -> QueryRow:
    """Fetch every result list for one query and evaluate all checks.

    Any request error marks every check for this query as failed, so a network
    problem can never be read as a pass.
    """
    row = QueryRow(index=index, query=query)
    try:
        # rerank=false: inputs to the HARD gate and its control.
        old_plain = fetch_results(old_url, query, user, top_k, rerank=False)
        new_plain = fetch_results(new_url, query, user, top_k, rerank=False)
        ctl_plain = fetch_results(old_url, query, user, top_k, rerank=False)
        # rerank=true: inputs to the INFO check and its control.
        old_rerank = fetch_results(old_url, query, user, top_k, rerank=True)
        new_rerank = fetch_results(new_url, query, user, top_k, rerank=True)
        ctl_rerank = fetch_results(old_url, query, user, top_k, rerank=True)
    except Exception as exc:  # network / HTTP / JSON errors count as FAIL
        error = f"request error: {exc}"
        row.hard_detail = error
        row.ctl_hard_detail = error
        return row

    row.hard_ok, row.hard_detail = hard_check(old_plain, new_plain, top_k)
    row.ctl_hard_ok, row.ctl_hard_detail = hard_check(old_plain, ctl_plain, top_k)
    row.info_top1, row.info_overlap = info_check(old_rerank, new_rerank, top_k)
    row.ctl_top1, row.ctl_overlap = info_check(old_rerank, ctl_rerank, top_k)
    return row


def print_table(rows: list[QueryRow]) -> None:
    """Per-query table with HARD, CONTROL and INFO columns side by side."""
    width = min(max(len(r.query) for r in rows), 50)
    header = (
        f"{'#':>3}  {'HARD':<4}  {'CTL-HARD':<8}  {'INFO-top1':<9}  {'INFO-ovl':<8}  "
        f"{'CTL-top1':<8}  {'CTL-ovl':<7}  query"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        shown = r.query if len(r.query) <= width else r.query[: width - 1] + "…"
        print(
            f"{r.index:>3}  {'PASS' if r.hard_ok else 'FAIL':<4}  "
            f"{'PASS' if r.ctl_hard_ok else 'FAIL':<8}  "
            f"{'yes' if r.info_top1 else 'no':<9}  {r.info_overlap:<8.2f}  "
            f"{'yes' if r.ctl_top1 else 'no':<8}  {r.ctl_overlap:<7.2f}  {shown}"
        )

    # Details only for rows that need explaining (failures), to keep the table short.
    details = [r for r in rows if not r.hard_ok or not r.ctl_hard_ok]
    if details:
        print("\nDetails (HARD / CONTROL-HARD failures):")
        for r in details:
            if not r.hard_ok:
                print(f"  [{r.index}] HARD: {r.hard_detail}")
            if not r.ctl_hard_ok:
                print(f"  [{r.index}] CTL-HARD: {r.ctl_hard_detail}")


def print_summary(rows: list[QueryRow], top_k: int) -> None:
    """Totals for each check family, plus mean INFO overlap for both comparisons."""
    total = len(rows)
    hard_pass = sum(1 for r in rows if r.hard_ok)
    ctl_hard_pass = sum(1 for r in rows if r.ctl_hard_ok)
    info_top1 = sum(1 for r in rows if r.info_top1)
    ctl_top1 = sum(1 for r in rows if r.ctl_top1)
    mean_info = sum(r.info_overlap for r in rows) / total if total else 0.0
    mean_ctl = sum(r.ctl_overlap for r in rows) / total if total else 0.0

    print("\nSUMMARY")
    print(f"  HARD (gate, OLD vs NEW, rerank=false):   {hard_pass}/{total} PASS")
    print(f"  INFO (OLD vs NEW, rerank=true, no gate): top-1 equal {info_top1}/{total}, "
          f"mean overlap {mean_info:.2f} (top_k={top_k})")
    print(f"  CONTROL HARD (OLD vs OLD, rerank=false): {ctl_hard_pass}/{total} PASS")
    print(f"  CONTROL INFO (OLD vs OLD, rerank=true):  top-1 equal {ctl_top1}/{total}, "
          f"mean overlap {mean_ctl:.2f} (top_k={top_k})")


def main() -> int:
    parser = argparse.ArgumentParser(description="Old-vs-new RAG endpoint comparison (tie-aware)")
    parser.add_argument("--old-url", default=DEFAULT_OLD_URL,
                        help="legacy endpoint base URL (SoHoAI)")
    parser.add_argument("--new-url", default=DEFAULT_NEW_URL,
                        help="new endpoint base URL (RAGNART)")
    parser.add_argument("--user", default=DEFAULT_USER, help="user filter sent to both endpoints")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K,
                        help="top_k sent to both endpoints")
    args = parser.parse_args()

    queries = parse_queries(QUERY_FILE)
    print(f"Queries parsed: {len(queries)} (from {QUERY_FILE.name})")
    print(f"OLD: {args.old_url}")
    print(f"NEW: {args.new_url}")
    print(f"user={args.user} top_k={args.top_k} tolerance={TOLERANCE}\n")

    rows: list[QueryRow] = []
    for idx, query in enumerate(queries, 1):
        # Progress goes to stderr so the table on stdout stays clean.
        print(f"running query {idx}/{len(queries)}...", file=sys.stderr, flush=True)
        rows.append(run_query(idx, query, args.old_url, args.new_url, args.user, args.top_k))

    print_table(rows)
    print_summary(rows, args.top_k)

    # Exit 0 iff there is at least one query and every HARD check passes.
    all_hard_pass = bool(rows) and all(r.hard_ok for r in rows)
    return 0 if all_hard_pass else 1


if __name__ == "__main__":
    sys.exit(main())
