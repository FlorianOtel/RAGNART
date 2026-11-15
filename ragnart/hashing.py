"""
File hashing helper for change, move, and duplicate detection.

MD5 is used here for change/move/duplicate detection only. It is NOT a
security primitive: nothing in this module should be relied on for integrity
against an attacker (see `docs/RAGNART-design.md` §4.2).
"""

from __future__ import annotations

import hashlib

# Read size per block. 1 MiB keeps memory flat even for large PDFs and
# Claude transcripts while keeping the number of read syscalls low.
_BLOCK_SIZE = 1024 * 1024


def hash_file(path: str) -> tuple[int, str]:
    """
    Return (size_bytes, md5_hexdigest) for the file at `path`.

    The file is read in binary mode in 1 MiB blocks, so the whole file is
    never held in memory. Size is the number of bytes actually read.
    """
    digest = hashlib.md5()
    size_bytes = 0
    with open(path, "rb") as fh:
        # iter(callable, sentinel) keeps calling read() until it returns b"".
        for block in iter(lambda: fh.read(_BLOCK_SIZE), b""):
            digest.update(block)
            size_bytes += len(block)
    return size_bytes, digest.hexdigest()
