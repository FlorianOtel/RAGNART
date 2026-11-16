"""
Move detection for the NAS sync (design §4.2, RESEARCH C3).

A move is a file that disappeared from one path and appeared at another path in the
same sync, with identical content. Detection is by `content_hash` only. A move is
recognised only when the match is unambiguous, so that a wrong pairing never rewrites
the key of an unrelated item. Anything that does not match one-to-one is left to the
ordinary path: the vanished item is tombstoned and the new item is ingested as new.

This module is pure: no I/O, no database, no Qdrant.
"""

# MD5 of the empty file. Every empty file has this hash, so it says nothing about
# identity and must never be used to pair a vanished file with a new one.
EMPTY_FILE_MD5 = "d41d8cd98f00b204e9800998ecf8427e"


def detect_moves(vanished: dict[str, str], new_hashes: dict[str, str]) -> dict[str, str]:
    """
    Pair vanished paths with new paths that share the same content hash.

    `vanished` maps each vanished path to its stored content_hash.
    `new_hashes` maps each newly discovered path to its content_hash.

    Returns `{old_path: new_path}` for every hash that occurs exactly once on each side.
    A hash is ignored when it is falsy (None or empty string), when it equals
    EMPTY_FILE_MD5, or when it occurs more than once on either side (ambiguous).
    """
    # Group each side by hash. Falsy hashes and the empty-file hash are dropped here,
    # so they can never form a pair.
    vanished_by_hash: dict[str, list[str]] = {}
    for path, content_hash in vanished.items():
        if not content_hash or content_hash == EMPTY_FILE_MD5:
            continue
        vanished_by_hash.setdefault(content_hash, []).append(path)

    new_by_hash: dict[str, list[str]] = {}
    for path, content_hash in new_hashes.items():
        if not content_hash or content_hash == EMPTY_FILE_MD5:
            continue
        new_by_hash.setdefault(content_hash, []).append(path)

    moves: dict[str, str] = {}
    for content_hash, old_paths in vanished_by_hash.items():
        new_paths = new_by_hash.get(content_hash, [])
        # Exactly one on each side is a move. Zero or two-or-more is not, so it is
        # skipped and falls through to delete plus new.
        if len(old_paths) == 1 and len(new_paths) == 1:
            moves[old_paths[0]] = new_paths[0]
    return moves
