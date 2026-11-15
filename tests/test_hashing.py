"""
Tests for ragnart/hashing.py.

Uses only pytest's tmp_path fixture. No Qdrant, no NFS, no live state DB.
"""

from __future__ import annotations

import hashlib

from ragnart.hashing import hash_file


def test_hash_file_returns_size_and_md5(tmp_path):
    # Known byte string written to a temp file; expected values computed independently.
    data = b"ragnart hashing test\n" * 1000
    path = tmp_path / "sample.txt"
    path.write_bytes(data)

    assert hash_file(str(path)) == (len(data), hashlib.md5(data).hexdigest())
