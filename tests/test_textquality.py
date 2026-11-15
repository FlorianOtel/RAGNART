"""
Tests for ragnart/textquality.py: the shared printable-ratio rule and the NUL sniff.

Pure functions only. No Qdrant, no Ollama, no docling. The file-based sniff tests
use pytest's tmp_path so they never touch the NAS or the project tree.
"""

import pytest

from ragnart.textquality import GATE_THRESHOLD, printable_ratio, sniff_binary_text


# The gate threshold is a documented value. If someone changes it, this test
# should force a deliberate decision rather than a silent drift.
def test_gate_threshold_is_unchanged_value():
    assert GATE_THRESHOLD == 0.85


# ---------------------------------------------------------------------------
# printable_ratio: table-driven cases
# Each row: (case_name, input_text, expected_ratio, comparison_note)
# A float expectation is compared with pytest.approx, so 0.8 and 4/5 both pass.
# ---------------------------------------------------------------------------
PRINTABLE_RATIO_CASES = [
    # Plain ASCII prose is fully printable.
    ("pure_ascii", "The quick brown fox jumps over the lazy dog.", 1.0, "all good"),

    # Layout characters must not be penalised. A real document has tabs and newlines.
    ("tabs_and_newlines_not_penalised", "col1\tcol2\nrow1\tval1\r\nrow2\tval2", 1.0, "tab/LF/CR are good"),

    # Empty and whitespace-only text are not corrupt. They return 1.0 by convention.
    ("empty_string", "", 1.0, "blank is not corrupt"),
    ("whitespace_only", "  \t\n  ", 1.0, "blank is not corrupt"),

    # Regression case for the bug this module fixes.
    # The old gate used isprintable() alone, and U+FFFD counts as printable there.
    # So 80 real letters plus 20 replacement characters scored 1.0 and was admitted.
    # The fixed rule must score it 0.80, below GATE_THRESHOLD, so it is rejected.
    ("regression_fffd_counted_as_printable", "a" * 80 + "�" * 20, 0.80, "FFFD is bad"),

    # Corrupt-PDF-style mix: real words with embedded replacement characters.
    ("corrupt_pdf_style_mix", "Invoice ��� total ��", 0.75, "15 good of 20 chars"),

    # Control characters (other than tab/LF/CR) are not printable, so they are bad.
    ("control_chars_are_bad", "ok\x01\x02\x03\x04", 2 / 6, "NUL-ish controls are bad"),
]


@pytest.mark.parametrize(
    "text, expected",
    [(case[1], case[2]) for case in PRINTABLE_RATIO_CASES],
    ids=[case[0] for case in PRINTABLE_RATIO_CASES],
)
def test_printable_ratio_table(text, expected):
    assert printable_ratio(text) == pytest.approx(expected)


def test_regression_case_falls_below_gate():
    # Make the regression intent explicit: the fixed rule must reject this text.
    corrupt = "a" * 80 + "�" * 20
    assert printable_ratio(corrupt) < GATE_THRESHOLD


def test_clean_text_passes_gate():
    clean = "A normal paragraph of ordinary extracted text, with punctuation."
    assert printable_ratio(clean) >= GATE_THRESHOLD


# ---------------------------------------------------------------------------
# sniff_binary_text: file-based tests using tmp_path
# ---------------------------------------------------------------------------
def test_sniff_detects_nul_in_first_window(tmp_path):
    # A NUL byte placed inside the first 8 KB is binary evidence.
    path = tmp_path / "binary.bin"
    path.write_bytes(b"header text" + b"\x00" + b"more bytes")

    reason = sniff_binary_text(path)

    assert reason == "binary: NUL byte in first 8KB"


def test_sniff_ignores_nul_after_window(tmp_path):
    # A NUL beyond the window is not inspected, so the sniff returns None.
    # This keeps the check cheap: it reads a fixed prefix, not the whole file.
    path = tmp_path / "late_nul.txt"
    path.write_bytes(b"a" * 8192 + b"\x00")

    assert sniff_binary_text(path) is None


def test_sniff_clean_text_file_returns_none(tmp_path):
    path = tmp_path / "clean.txt"
    path.write_text("Plain UTF-8 text with ünïcode and no NUL bytes.\n", encoding="utf-8")

    assert sniff_binary_text(path) is None
