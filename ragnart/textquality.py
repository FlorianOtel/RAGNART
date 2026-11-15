"""
Shared text-quality helpers for the ingestion admission gate.

Both the ingest pipeline (`ragnart/ingest.py`) and the corrupt-content purge tool
(`utils/rag_purge_corrupt.py`) decide whether parsed text is "real text" or
binary/garbage. They must agree on that decision, so the rule lives here once.

Single source of truth:
    GATE_THRESHOLD  - minimum printable ratio a document must reach to be admitted.
"""

from __future__ import annotations

import pathlib

# Minimum fraction of "good" characters a parsed document must have to be admitted.
# Documents below this are marked ignored with a "binary content" reason.
GATE_THRESHOLD: float = 0.85

# Characters that count as good text even though str.isprintable() rejects them.
# Tabs, newlines and carriage returns are ordinary layout characters in real documents.
_LAYOUT_CHARS: frozenset[str] = frozenset("\t\n\r")

# U+FFFD (REPLACEMENT CHARACTER) is what a decoder emits for bytes it cannot map.
# It is technically "printable" to str.isprintable(), so it must be excluded
# explicitly, or corrupt text full of replacement characters would pass the gate.
_REPLACEMENT_CHAR: str = "\ufffd"


def printable_ratio(text: str) -> float:
    """
    Return the fraction of characters in `text` that look like real text (0.0 to 1.0).

    A character is "good" when it is either:
      - printable (per str.isprintable()) or a layout character (tab, LF, CR), AND
      - not the Unicode replacement character U+FFFD.

    Blank text (empty, or only whitespace) returns 1.0. Blank text is not corrupt;
    it is handled by the caller as an empty document, not as binary content.
    """
    # Blank text is not "corrupt". Return early so the division below never sees
    # an empty denominator, and so whitespace-only text is never penalised.
    if not text.strip():
        return 1.0

    good_count = 0
    for char in text:
        is_printable_or_layout = char.isprintable() or char in _LAYOUT_CHARS
        if is_printable_or_layout and char != _REPLACEMENT_CHAR:
            good_count += 1

    # len(text) is at least 1 here because the blank-text check above returned early.
    return good_count / len(text)


def sniff_binary_text(path: pathlib.Path, nul_window_bytes: int = 8192) -> str | None:
    """
    Cheap check on the raw bytes of a file, run BEFORE the file is parsed.

    Reads only the first `nul_window_bytes` bytes. Text files essentially never
    contain a NUL byte (0x00), but binary files almost always do, near the start.
    If a NUL is found, return a human-readable reason string for `mark_ignored`.
    Otherwise return None, meaning "no binary evidence in this window".

    The replacement-character check is NOT done here. It needs decoded text, so the
    caller applies it after parsing, to avoid reading and decoding the file twice.
    """
    # Read raw bytes, not text, so that undecodable content is still inspected.
    with path.open("rb") as handle:
        window = handle.read(nul_window_bytes)

    if b"\x00" in window:
        return "binary: NUL byte in first 8KB"
    return None
