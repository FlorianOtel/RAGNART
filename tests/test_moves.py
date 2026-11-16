"""
Tests for ragnart.moves.detect_moves (RESEARCH C3, design §4.2).

The function is pure, so these tests build plain dicts and check the returned mapping.
"""
from ragnart.moves import EMPTY_FILE_MD5, detect_moves


def test_one_to_one_non_empty_hash_is_a_move():
    # One vanished path and one new path share a real hash: this is a move.
    vanished = {"/nas/a/old.txt": "abc123"}
    new_hashes = {"/nas/b/new.txt": "abc123"}

    assert detect_moves(vanished, new_hashes) == {"/nas/a/old.txt": "/nas/b/new.txt"}


def test_two_vanished_sharing_a_hash_is_not_a_move():
    # Two vanished paths with the same hash are ambiguous: neither is paired.
    vanished = {"/nas/a/one.txt": "abc123", "/nas/a/two.txt": "abc123"}
    new_hashes = {"/nas/b/new.txt": "abc123"}

    assert detect_moves(vanished, new_hashes) == {}


def test_two_new_sharing_a_hash_is_not_a_move():
    # Two new paths with the same hash are ambiguous: neither is paired.
    vanished = {"/nas/a/old.txt": "abc123"}
    new_hashes = {"/nas/b/one.txt": "abc123", "/nas/b/two.txt": "abc123"}

    assert detect_moves(vanished, new_hashes) == {}


def test_empty_file_hash_never_matches():
    # Every empty file has the same hash, so it must never produce a move.
    vanished = {"/nas/a/empty_old.txt": EMPTY_FILE_MD5}
    new_hashes = {"/nas/b/empty_new.txt": EMPTY_FILE_MD5}

    assert detect_moves(vanished, new_hashes) == {}


def test_null_or_blank_hash_never_matches():
    # None and empty-string hashes carry no identity, so they never pair.
    vanished = {"/nas/a/none_old.txt": None, "/nas/a/blank_old.txt": ""}
    new_hashes = {"/nas/b/none_new.txt": None, "/nas/b/blank_new.txt": ""}

    assert detect_moves(vanished, new_hashes) == {}


def test_no_overlap_returns_empty_dict():
    # Vanished and new hashes are disjoint: nothing is a move.
    vanished = {"/nas/a/old.txt": "abc123"}
    new_hashes = {"/nas/b/new.txt": "def456"}

    assert detect_moves(vanished, new_hashes) == {}
