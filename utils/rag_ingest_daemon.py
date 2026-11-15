#!/usr/bin/env python3
"""
Process pending files from the RAG ingestion queue.

Runs the 7-step atomic ingestion loop (parse → chunk → embed → upsert) until
the queue is empty.

Work is distributed by a continuous pool, not in batches. --workers coroutines
each claim one pending file at a time from SQLite (StateDB.claim_next_pending),
ingest it, and claim the next one as soon as they finish. A slow file therefore
holds only its own worker; the others keep moving through the queue.

Both --workers and --batch are required. They are separate knobs for different
bottlenecks — the right values depend on where the embedding server lives:

  --workers  Number of files processed concurrently. Each worker runs docling
             parsing in its own OS thread (thread-local DocumentConverter), so
             workers overlap CPU parse time with GPU/CPU embed time from other
             files. Set higher when the embedding server is a remote GPU (parse
             is the bottleneck); set to 1 when embedding is CPU-local (parse and
             embed compete for the same cores).

  --batch    Max concurrent Ollama embedding requests per file. Controls how
             many chunks are in-flight to Ollama at once. Set higher for a
             remote GPU (RTX 5070 can saturate many requests); set lower for
             CPU-local Ollama (it serialises inference anyway, high values just
             queue up).

Recommended operating points:
  GPU embed (Server 2, RTX 5070):  --workers 4 --batch 20
  CPU embed (Server 1, local):     --workers 1 --batch 5
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402

from ragnart.collection import ensure_collection, get_client
from ragnart.ingest import ingest_file
from ragnart.lock import acquire_or_exit, lock_path_from_config
from ragnart.state import StateDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"


def _add_file_handler(log_path: str) -> None:
    """Add a FileHandler to the root logger so all output also goes to log_path."""
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    logging.getLogger().addHandler(handler)


# Emit a queue-wide summary (get_counts) once per this many completions. Replaces the
# old per-batch summary, since there are no batches any more.
_SUMMARY_EVERY = 10


async def run(
    state_db: StateDB,
    qdrant_client,
    rag_cfg: dict,
    embed_concurrency: int,
    file_workers: int,
) -> None:
    """Run file_workers coroutines that claim and ingest pending files until none remain."""
    recovered = state_db.crash_recovery()
    if recovered:
        logger.info("Crash recovery: reset %d stuck file(s) to pending", len(recovered))

    processed = failed = 0

    async def worker(worker_id: int) -> None:
        """Claim one pending file at a time and ingest it, until the queue has no pending rows."""
        nonlocal processed, failed
        while True:
            # claim_next_pending is synchronous with no await, so two workers cannot
            # claim the same row (see its docstring in ragnart/state.py).
            row = state_db.claim_next_pending()
            if row is None:
                return

            file_path = row["file_path"]
            logger.info(
                "[worker %d] processing [%d done / %d failed]: %s",
                worker_id, processed, failed, file_path,
            )
            try:
                await ingest_file(
                    file_path=file_path,
                    owner=row["owner"],
                    rag_cfg=rag_cfg,
                    state_db=state_db,
                    qdrant_client=qdrant_client,
                    embed_concurrency=embed_concurrency,
                    # Where the bytes are read (claude-session keys need it) and the
                    # source mtime written to the payload as modified_at.
                    origin_path=row["origin_path"],
                    last_modified=row["last_modified"],
                )
                processed += 1
            except Exception:
                # Per-item guard: a failing file must not end this worker, or the
                # gather below would cancel the other workers. ingest_file already
                # logged and marked the row failed.
                failed += 1

            if (processed + failed) % _SUMMARY_EVERY == 0:
                counts = state_db.get_counts()
                logger.info(
                    "Progress — pending: %d  completed: %d  failed: %d",
                    counts["pending"], counts["completed"], counts["failed"],
                )

    await asyncio.gather(*[worker(i) for i in range(file_workers)])

    logger.info("Worker finished: %d processed, %d failed", processed, failed)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process pending RAG ingestion files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Both flags are required — their correct values depend on the embedding server:\n"
            "  GPU embed (Server 2):  --workers 4 --batch 20\n"
            "  CPU embed (Server 1):  --workers 1 --batch 5"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        required=True,
        metavar="N",
        help=(
            "Number of files to parse+embed concurrently. "
            "Higher values overlap docling CPU parse with remote GPU embed. "
            "Use 1 when embedding is CPU-local to avoid core contention."
        ),
    )
    parser.add_argument(
        "--batch",
        type=int,
        required=True,
        metavar="M",
        help=(
            "Max concurrent Ollama embedding requests per file (chunk-level parallelism). "
            "Higher values suit a remote GPU; keep low (3–5) for CPU-local Ollama."
        ),
    )
    parser.add_argument(
        "--log-file",
        metavar="PATH",
        default=None,
        help=(
            "Write all log output to this file in addition to stderr. "
            "Required for rag_status.py --watch to work."
        ),
    )
    args = parser.parse_args()

    # Load ragnart.yaml once. The lock path and the rest of the config both come from it.
    _config_path = Path(__file__).resolve().parent.parent / "ragnart.yaml"
    with open(_config_path) as _f:
        config = yaml.safe_load(_f)

    # Take the single-writer ingestion lock before any state or Qdrant write. This daemon
    # writes rag_state.db rows and Qdrant points, so it must not run alongside sync, a
    # purge, a reset or the snapshot. Contention exits at once with EXIT_LOCK_HELD (75)
    # and names the holder; the daemon does not wait. When the run script already holds
    # the lock (see ragnart/lock.py `run`), acquire_or_exit returns None and this process
    # continues under that lock. The returned fd is kept in a named variable so the lock
    # stays for the whole process lifetime.
    _lock_fd = acquire_or_exit(lock_path_from_config(config))  # noqa: F841 — kept for lock lifetime

    if args.log_file:
        _add_file_handler(args.log_file)

    rag_cfg = config.get("rag", {})
    rag_cfg["opencode_api_url"] = config.get("opencode", {}).get("api_url", "http://localhost:4096")
    # File holding OPENCODE_SERVER_PASSWORD (OpenCode 2.x Basic auth); read only when
    # an opencode:// item is parsed. None -> opencode_client.DEFAULT_PASSWORD_ENV_FILE.
    rag_cfg["opencode_password_env_file"] = config.get("opencode", {}).get("password_env_file")
    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = f"{db_base}/sqlite/rag_state.db"
    state_db = StateDB(db_path)
    qdrant_client = get_client(rag_cfg.get("qdrant_url", "http://192.168.1.93:6333"))
    ensure_collection(qdrant_client)

    counts_before = state_db.get_counts()
    if counts_before["pending"] == 0:
        print("No pending files. Run rag_sync_nfs.py first.")
        state_db.close()
        return

    logger.info(
        "Starting ingest: %d files pending  workers=%d  batch=%d",
        counts_before["pending"], args.workers, args.batch,
    )

    try:
        asyncio.run(
            run(
                state_db,
                qdrant_client,
                rag_cfg,
                embed_concurrency=args.batch,
                file_workers=args.workers,
            )
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")

    counts_after = state_db.get_counts()
    print()
    print("Final queue state:")
    print(f"  pending    : {counts_after['pending']}")
    print(f"  completed  : {counts_after['completed']}")
    print(f"  failed     : {counts_after['failed']}")

    state_db.close()


if __name__ == "__main__":
    main()
