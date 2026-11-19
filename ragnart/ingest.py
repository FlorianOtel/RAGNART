"""
Document parsing, chunking, and Qdrant upsert for the RAG pipeline.

Entry point: ingest_content(), which the intake worker (ragnart/intake_worker.py) calls once for
each request the ingestion API accepted. For one item it deletes the item's old points, runs the
admission rules, parses, chunks, embeds and upserts the new points. Nothing here reads a queue or
a state database: the caller owns the item's state.

Chunking strategy (RAG-strategy.md §3.1):
  - Parent-child (PDF, DOCX, IPYNB, long MD): child ~250 tok embedded;
    parent ~1000 tok stored as context returned to the LLM.
  - Flat 512-tok (PPTX, YAML, CSV, short docs): parent == child.

Parsing:
  - Structured formats (PDF, DOCX, PPTX): docling → markdown export.
  - PPTX fallback: OLE2 detection → LibreOffice conversion → python-pptx when docling fails.
  - IPYNB: dedicated cell extractor (JSON parse → markdown + code cells).
  - Text formats (TXT, MD, YAML, CSV): direct UTF-8 read.
  - docling failures: PPTX falls back to python-pptx; PDF and DOCX raise.
    There is no raw-byte fallback for docling types.
  - Admission (text-like files and all chunked items): size cap, NUL sniff,
    replacement-character ratio and chunk cap. Caps come from rag_cfg["admission"].
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import tiktoken
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import ConvertPipelineOptions, ThreadedPdfPipelineOptions
from docling.document_converter import (
    DocumentConverter,
    PdfFormatOption,
    PowerpointFormatOption,
    WordFormatOption,
)
from qdrant_client import QdrantClient
from qdrant_client.models import (
    FieldCondition,
    Filter,
    FilterSelector,
    MatchValue,
    PointStruct,
    SparseVector,
)

from .collection import DOCUMENTS_COLLECTION, ensure_collection, collection_has_sparse
from .embeddings import embed_batch, embed_sparse
from .hashing import hash_file, hash_text
from .chat_render import _parse_claude_chat
from .schema import (
    FIELD_CHUNK_INDEX,
    FIELD_CONTENT_HASH,
    FIELD_FILE_NAME,
    FIELD_FILE_TYPE,
    FIELD_INGESTED_AT,
    FIELD_MODIFIED_AT,
    FIELD_OWNER,
    FIELD_PAGE,
    FIELD_PARENT_TEXT,
    FIELD_PIPELINE_VERSION,
    FIELD_PROJECT_ID,
    FIELD_SESSION_TITLE,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    FIELD_SOURCE_TYPE,
    FIELD_TAG,
    FIELD_TEXT,
    GLOBAL_PROJECT_ID,
    PIPELINE_VERSION_P1,
    SPARSE_VECTOR_NAME,
)
from .textquality import GATE_THRESHOLD, printable_ratio, sniff_binary_text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sparse vector readiness cache
# ---------------------------------------------------------------------------

# Module-level cache: stores whether the Qdrant collection has sparse_text configured.
# Lazily initialized on first call to _is_sparse_ready() and cached for the lifetime
# of the worker process (across multiple file ingestions).
_sparse_ready: bool | None = None


def _is_sparse_ready(qdrant_client: QdrantClient) -> bool:
    """
    Check if the Qdrant collection has sparse vector support enabled.

    Lazy-initializes the _sparse_ready cache on first call. Returns True if the
    collection has the SPARSE_VECTOR_NAME (sparse_text) vector space configured;
    False before migration via rag_sparse_migrate.py or if the collection is absent.

    Args:
        qdrant_client: Qdrant client instance.

    Returns:
        True if sparse vectors are supported, False otherwise.
    """
    global _sparse_ready
    if _sparse_ready is None:
        _sparse_ready = collection_has_sparse(qdrant_client)
        logger.debug("Sparse vector readiness: %s", _sparse_ready)
    return _sparse_ready


# ---------------------------------------------------------------------------
# Chunking parameters (RAG-strategy.md §3.1)
# ---------------------------------------------------------------------------

_PARENT_CHUNK_SIZE    = 1000   # tokens — midpoint of 800–1200 range
_PARENT_CHUNK_OVERLAP = 100
_CHILD_CHUNK_SIZE     = 250    # tokens — midpoint of 200–300 range; safe under bge-m3's 8192-token context
_CHILD_CHUNK_OVERLAP  = 20
_FLAT_CHUNK_SIZE      = 512
_FLAT_CHUNK_OVERLAP   = 50

# Qdrant rejects upsert requests whose JSON payload exceeds its HTTP body limit.
# Large files (textbooks, big CSVs) can produce thousands of points whose combined
# parent_text fields push well past that limit. Splitting into smaller batches keeps
# each request well under the threshold while preserving all-or-nothing semantics
# at the file level (step 0 already deleted stale points before we get here).
_UPSERT_BATCH_SIZE = 256   # points per qdrant_client.upsert() call

# Always use flat chunking for these types (compact; parent-child overhead not worth it)
_ALWAYS_FLAT_TYPES = {"pptx", "yaml", "yml", "csv"}

# If total token count is below this threshold, use flat even for PDF/IPYNB/MD
_SHORT_DOC_TOKENS = 600

# Structured formats that go through docling; everything else is direct text read
_DOCLING_TYPES = {"pdf", "docx", "pptx"}   # ipynb handled by _parse_ipynb()

# tiktoken cl100k_base — consistent encoding across ingest and search
_enc = tiktoken.get_encoding("cl100k_base")

# Thread-local DocumentConverter — one instance per OS thread so concurrent
# asyncio.to_thread() calls don't share docling's internal conversion state.
_thread_local = threading.local()


def _build_converter() -> DocumentConverter:
    """
    Build a DocumentConverter with an explicit CPU accelerator for every docling type we use.

    Docling picks its inference device automatically when none is given (DOCLING_DEVICE or
    "auto", which would use CUDA if it were visible). This host is CPU-only, and the choice
    should be written in code rather than inherited from the environment, so it is set here.

    Where the accelerator is actually used (docling 2.89.0):
      - PDF: StandardPdfPipeline runs layout, table and OCR models on the accelerator.
        This is the only pipeline that loads ML models, so it is the one that matters.
      - DOCX and PPTX: SimplePipeline builds the document from the declarative backend
        and loads no models by default. The CPU setting is passed anyway, so every docling
        type we handle reports the same explicit device, and it costs nothing.

    Thread count is left at docling's default. Only the device is pinned here.
    """
    cpu_accelerator = AcceleratorOptions(device=AcceleratorDevice.CPU)

    pdf_options = ThreadedPdfPipelineOptions(accelerator_options=cpu_accelerator)
    docx_options = ConvertPipelineOptions(accelerator_options=cpu_accelerator)
    pptx_options = ConvertPipelineOptions(accelerator_options=cpu_accelerator)

    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_options),
            InputFormat.DOCX: WordFormatOption(pipeline_options=docx_options),
            InputFormat.PPTX: PowerpointFormatOption(pipeline_options=pptx_options),
        }
    )


def _get_converter() -> DocumentConverter:
    if not hasattr(_thread_local, "converter"):
        _thread_local.converter = _build_converter()
    return _thread_local.converter


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_ipynb(path: Path) -> str:
    """
    Extract clean text from a Jupyter notebook.

    Concatenates markdown and code cells in order, separated by blank lines.
    Skips raw cells and empty cells. Cell outputs are ignored — source only.
    Falls back to raw UTF-8 read if the file is not valid JSON.
    """
    try:
        nb = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except json.JSONDecodeError:
        return path.read_text(encoding="utf-8", errors="replace")

    parts: list[str] = []
    for cell in nb.get("cells", []):
        cell_type = cell.get("cell_type", "")
        source = cell.get("source", [])
        text = "".join(source).strip() if isinstance(source, list) else str(source).strip()
        if not text:
            continue
        if cell_type == "markdown":
            parts.append(text)
        elif cell_type == "code":
            parts.append(f"```python\n{text}\n```")
    return "\n\n".join(parts)


# OLE2 magic bytes — Composite Document File V2 (binary .ppt format)
_OLE2_MAGIC = b"\xD0\xCF\x11\xE0"


def _parse_pptx(path: Path) -> str:
    """
    Extract text from a PowerPoint file using python-pptx.

    Used as fallback when docling fails to detect the PPTX format. Handles
    two cases:

    1. OLE2 binary .ppt disguised as .pptx (Composite Document File V2,
       detected by magic bytes): convert to real .pptx via LibreOffice
       headless, then parse with python-pptx.
    2. Genuine OpenXML .pptx that docling rejected: parse directly with
       python-pptx.

    Each slide is prefixed with "Slide N" so chunk boundaries remain
    interpretable during retrieval. Temp files are always cleaned up.
    """
    from pptx import Presentation  # docling dep — always present in this venv

    # Detect OLE2 binary .ppt by magic bytes
    with open(path, "rb") as fh:
        magic = fh.read(4)

    tmp_dir: str | None = None
    pptx_path = path
    if magic == _OLE2_MAGIC:
        logger.info(
            "OLE2 binary .ppt detected for %s — attempting LibreOffice conversion", path.name
        )
        tmp_dir = tempfile.mkdtemp()
        try:
            result = subprocess.run(
                [
                    "libreoffice", "--headless",
                    "--convert-to", "pptx",
                    "--outdir", tmp_dir,
                    str(path),
                ],
                check=False,
                timeout=120,
                capture_output=True,
            )
            pptx_path = Path(tmp_dir) / f"{path.stem}.pptx"
            if result.returncode != 0 or not pptx_path.exists():
                stderr = result.stderr.decode(errors="replace").strip()
                raise ValueError(
                    f"LibreOffice could not convert OLE2 .ppt '{path.name}' "
                    f"(file may be IRM/DRM-encrypted or corrupted): "
                    f"{stderr or 'no stderr'}"
                )
        except Exception:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

    try:
        prs = Presentation(str(pptx_path))
        parts: list[str] = []
        for slide_num, slide in enumerate(prs.slides, start=1):
            slide_lines: list[str] = []
            for shape in slide.shapes:
                if not shape.has_text_frame:
                    continue
                for para in shape.text_frame.paragraphs:
                    text = "".join(run.text for run in para.runs).strip()
                    if text:
                        slide_lines.append(text)
            if slide_lines:
                parts.append(f"Slide {slide_num}\n" + "\n".join(slide_lines))
        return "\n\n".join(parts)
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _parse_to_text(file_path: str, file_type: str) -> str:
    """
    Extract full text from a document.

    Structured formats (pdf, docx, pptx) go through docling → Markdown.
    PPTX fallback: python-pptx when docling format detection fails.
    IPYNB: dedicated cell extractor (avoids docling which doesn't support it).
    Text formats (txt, md, yaml, csv): direct UTF-8 read.

    Synchronous and potentially slow for large PDFs; callers use asyncio.to_thread().
    """
    path = Path(file_path)
    if file_type == "ipynb":
        return _parse_ipynb(path)
    if file_type == "jsonl":
        text, _, _ = _parse_claude_chat(path)
        return text
    if file_type not in _DOCLING_TYPES:
        return path.read_text(encoding="utf-8", errors="replace")

    try:
        result = _get_converter().convert(source=str(path))
        return result.document.export_to_markdown()
    except Exception as exc:
        if file_type == "pptx":
            logger.warning(
                "docling failed for %s (%s) — trying python-pptx fallback: %s",
                path.name, file_type, exc,
            )
            # No raw-read fallback for binary PPTX: if _parse_pptx raises, let it
            # propagate, so the caller records the item as failed rather than embedding garbage.
            return _parse_pptx(path)
        # No raw-read fallback for PDF or DOCX. A raw UTF-8 read of a binary file
        # produces garbage text that then gets embedded. Raise instead: the caller records the
        # item as failed, and the retry path then ends it as ignored.
        raise RuntimeError(f"docling failed: {exc}") from exc


def _is_text_like_type(raw_file_type: str) -> bool:
    """
    True when a file of this declared type is read directly as UTF-8 text.

    The rule by type alone: docling types and the ipynb and jsonl parsers are not text-like.
    is_text_like_path() adds the key-prefix rules on top of this, and ingest_content() uses it
    directly, because an endpoint upload's type comes from its request, not from a file name.
    """
    return raw_file_type not in _DOCLING_TYPES and raw_file_type not in {"ipynb", "jsonl"}


def is_text_like_path(file_path: str) -> bool:
    """
    True when ingest reads the file directly as UTF-8 text.

    Text-like files are not docling, ipynb, jsonl, opencode or claude-session sources.
    Only these get the size cap, the NUL sniff and the replacement-character check.
    Shared with utils/rag_purge_rescan.py so both tools apply one definition.
    """
    raw_file_type = Path(file_path).suffix.lstrip(".").lower()
    return (
        not file_path.startswith("opencode://")
        # A claude-session key has no file suffix, so without this it would look text-like.
        and not file_path.startswith("claude-session://")
        and _is_text_like_type(raw_file_type)
    )


def fffd_ratio(text: str) -> float:
    """
    Fraction of U+FFFD (replacement) characters in the decoded text.

    The UTF-8 decoder emits U+FFFD for undecodable bytes, so a high ratio means binary.
    Shared with utils/rag_purge_rescan.py so both tools apply one definition.
    """
    return text.count("\ufffd") / max(len(text), 1)


# ---------------------------------------------------------------------------
# Token-based chunking
# ---------------------------------------------------------------------------

def _split_tokens(text: str, chunk_size: int, overlap: int) -> list[str]:
    """Split text into token-bounded chunks with overlap. Preserves order."""
    tokens = _enc.encode(text)
    if not tokens:
        return []
    chunks: list[str] = []
    start = 0
    while start < len(tokens):
        end = min(start + chunk_size, len(tokens))
        chunks.append(_enc.decode(tokens[start:end]))
        if end == len(tokens):
            break
        start += chunk_size - overlap
    return chunks


def _chunk_parent_child(text: str) -> list[tuple[str, str]]:
    """
    Split text using the parent-child strategy.

    Each parent (~1000 tok) is split into smaller children (~250 tok).
    The child is what gets embedded (precise similarity); the parent is
    stored in the Qdrant payload and returned to the LLM for richer context.

    Returns: list of (child_text, parent_text) pairs.
    """
    pairs: list[tuple[str, str]] = []
    for parent in _split_tokens(text, _PARENT_CHUNK_SIZE, _PARENT_CHUNK_OVERLAP):
        for child in _split_tokens(parent, _CHILD_CHUNK_SIZE, _CHILD_CHUNK_OVERLAP):
            pairs.append((child, parent))
    return pairs


def _chunk_flat(text: str) -> list[tuple[str, str]]:
    """
    Flat chunking for compact files (PPTX, short TXT, YAML, CSV).

    Returns (chunk, chunk) pairs — parent == child (no extra context layer).
    """
    return [(c, c) for c in _split_tokens(text, _FLAT_CHUNK_SIZE, _FLAT_CHUNK_OVERLAP)]


def _select_chunks(text: str, file_type: str) -> tuple[list[tuple[str, str]], str]:
    """
    Choose chunking strategy and return (pairs, strategy_label).

    Flat when:
      - file_type is in _ALWAYS_FLAT_TYPES (pptx, yaml, csv), OR
      - document is short (< _SHORT_DOC_TOKENS tokens).
    Parent-child for everything else.
    """
    if file_type in _ALWAYS_FLAT_TYPES:
        return _chunk_flat(text), "flat"
    if len(_enc.encode(text)) < _SHORT_DOC_TOKENS:
        return _chunk_flat(text), "flat(short)"
    return _chunk_parent_child(text), "parent-child"


# ---------------------------------------------------------------------------
# Admission rules, used by ingest_content for every item it receives.
#
# Each helper returns the reason an item is ignored, or None when the item passes. The reason
# text is what the queue (mark_ignored) or the intake row (admission_reason) records, so both
# paths write the same wording for the same rule.
# ---------------------------------------------------------------------------

def _admission_limits(rag_cfg: dict) -> tuple[int, float, int]:
    """
    Return (max_text_bytes, fffd_threshold, max_chunks) from rag_cfg["admission"].

    Code defaults apply when a key is absent.
    """
    admission_cfg = rag_cfg.get("admission", {})
    max_text_bytes = admission_cfg.get("max_text_file_size_bytes", 5_000_000)
    fffd_threshold = admission_cfg.get("fffd_ratio_threshold", 0.005)
    max_chunks = admission_cfg.get("max_chunks_per_item", 5000)
    return max_text_bytes, fffd_threshold, max_chunks


def _size_cap_reason(size_bytes: int, max_text_bytes: int) -> str | None:
    """Size cap for a text-like file, checked from its size before it is read at all."""
    if size_bytes > max_text_bytes:
        return f"too large: {size_bytes} bytes > {max_text_bytes} byte cap"
    return None


def _replacement_reason(text: str, fffd_threshold: float) -> str | None:
    """Replacement-character check for a text-like file, on the decoded text. A high ratio means binary."""
    replacement_ratio = fffd_ratio(text)
    if replacement_ratio > fffd_threshold:
        return f"binary: {replacement_ratio:.1%} replacement chars"
    return None


def _printable_reason(text: str) -> str | None:
    """
    Printable-ratio gate for parsed text, applied to every item type.

    Documents below GATE_THRESHOLD are binary or corrupt. The rule lives in textquality.py so
    that ingest and the purge tool cannot drift apart.
    """
    printable = printable_ratio(text)
    if printable < GATE_THRESHOLD:
        return f"binary content: {printable:.0%} printable chars"
    return None


def _chunk_cap_reason(chunk_count: int, max_chunks: int) -> str | None:
    """Chunk cap, applied to every item type before any embedding happens."""
    if chunk_count > max_chunks:
        return f"too large: {chunk_count} chunks > {max_chunks} chunk cap"
    return None


def item_filter(source_id: str, key: str) -> Filter:
    """
    Return the Qdrant filter that selects the points of one item: one source, one key.

    Both conditions are required. A key alone is not enough to name an item, because another
    source could hold the same key string. The source_id is matched exactly, as stored.
    """
    return Filter(
        must=[
            FieldCondition(key=FIELD_SOURCE_ID, match=MatchValue(value=source_id)),
            FieldCondition(key=FIELD_SOURCE_PATH, match=MatchValue(value=key)),
        ]
    )


# ---------------------------------------------------------------------------
# Content-based ingestion for endpoint items (v0.3 session B1)
#
# An endpoint item is identified by the source it was submitted to and by its key. The key is an
# opaque string here: it is never opened as a file, and its prefix never decides the source. The
# caller passes the source id, taken from the authenticated request. Admission, parsing, chunking,
# embedding and the point payload are described on ingest_content. There is no StateDB: the intake
# worker records the result on the intake row.
# ---------------------------------------------------------------------------

# A declared file type becomes the suffix of a temporary link during parsing, so it must be a
# plain word. A dot or a slash could name a path outside the parse directory.
_FILE_TYPE_PATTERN = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class IngestResult:
    """
    What ingest_content did with one endpoint item.

    status:           'completed' (points written, or nothing to write) or 'ignored' (an admission
                      rule refused the item; reason says which). 'failed' is never returned here:
                      when ingest_content raises, the intake worker records the request as failed.
    reason:           the admission reason when ignored, else None.
    chunk_count:      points written (0 when ignored or empty).
    content_hash:     MD5 of the uploaded bytes, or of the text as UTF-8. None when no hash was
                      taken (ignored items and items with no chunks).
    pipeline_version: the pipeline version the points carry.
    """

    status: Literal["completed", "ignored", "failed"]
    reason: str | None
    chunk_count: int
    content_hash: str | None
    pipeline_version: str


def _declared_file_type(file_type: str) -> str:
    """
    Return a declared file type in canonical form: lowercase, no leading dot, a plain word.

    Raises ValueError for anything else. The declared type is used as a file suffix when the spool
    file is parsed, so an unchecked value could name a path outside the parse directory.
    """
    raw_file_type = file_type.lstrip(".").lower()
    if not _FILE_TYPE_PATTERN.fullmatch(raw_file_type):
        raise ValueError(f"invalid file type {file_type!r}: use letters and digits only")
    return raw_file_type


async def _parse_spool(spool_path: str, raw_file_type: str) -> str:
    """
    Parse a spool file by its declared file type, not by a file name.

    _parse_to_text() and docling choose a format from the file name, and a spool file has none.
    So the spool file is linked, inside a temporary directory, to a name whose suffix is the
    declared type, and the parse runs on that link. The spool file itself is not moved or renamed.
    The temporary directory is removed when the parse ends, whether or not it succeeded.
    """
    # A symlink target is resolved from the link's own directory when it is relative, so the target
    # is made absolute first.
    target = Path(spool_path).absolute()
    with tempfile.TemporaryDirectory(prefix="ragnart-parse-") as link_dir:
        link_path = Path(link_dir) / f"document.{raw_file_type}"
        os.symlink(target, link_path)
        return await asyncio.to_thread(_parse_to_text, str(link_path), raw_file_type)


async def _admit_and_parse_spool(
    spool_path: str,
    raw_file_type: str,
    max_text_bytes: int,
    fffd_threshold: float,
) -> tuple[str | None, str | None]:
    """
    Run the bytes admission checks, and parse the spool file.

    Order: size cap, NUL sniff (text-like files only, before any parse), parse, then the
    replacement-character check (text-like files only). Returns (text, None) when the item
    passes, or (None, reason) when it is ignored.
    """
    text_like = _is_text_like_type(raw_file_type)
    if text_like:
        size_reason = _size_cap_reason(Path(spool_path).stat().st_size, max_text_bytes)
        if size_reason is not None:
            return None, size_reason
        nul_reason = sniff_binary_text(Path(spool_path))
        if nul_reason is not None:
            return None, nul_reason

    text = await _parse_spool(spool_path, raw_file_type)

    if text_like:
        replacement_reason = _replacement_reason(text, fffd_threshold)
        if replacement_reason is not None:
            return None, replacement_reason
    return text, None


# The connector names, as ragnart/ingest_api.py CONNECTOR_* and the sources table spell them.
# ingest_content takes the connector from the source, and it decides what kind of item this is.
_CONNECTOR_FILESYSTEM = "filesystem"
_CONNECTOR_CLAUDE_CODE = "claude_code"
_CONNECTOR_OPENCODE = "opencode"


async def ingest_content(
    *,
    source_id: str,
    key: str,
    owner: str,
    rag_cfg: dict,
    qdrant_client: QdrantClient,
    connector: str = _CONNECTOR_FILESYSTEM,
    spool_path: str | None = None,
    file_type: str | None = None,
    text: str | None = None,
    chat_meta: dict | None = None,
    embed_concurrency: int = 5,
    modified_at: str | None = None,
    tag: str = "",
    project_id: str = GLOBAL_PROJECT_ID,
) -> IngestResult:
    """
    Parse, chunk, embed and upsert one endpoint item, replacing its earlier points.

    The connector of the source says what the item is, and which content form it takes:
      filesystem  spool_path + file_type: uploaded bytes. The file type is the one the request
                  declared. The item is stored with source_type 'file' and that file_type. Its
                  content hash is the MD5 of the bytes, the same hash the NAS side computes for a file.
      claude_code text: a Claude Code session rendered by the sender. Not parsed. Stored with
                  source_type 'claude_session' and file_type 'claude_chat'. Its content hash is the
                  MD5 of the text as UTF-8 (B1 behaviour, unchanged).
      opencode    text: an OpenCode session rendered by the sender. Not parsed. Stored with
                  source_type 'opencode_session' and file_type 'opencode'. No content hash, as
                  No hash is stored for OpenCode content (it lives only in OpenCode's DB).

    Args:
        source_id:   The source the item belongs to, in its stored case. Taken from the
                     authenticated request by the intake worker, never from the key.
        key:         The item's key within the source. Matched as a string; never opened.
        owner:       The owner written to every point: the item's own owner, or the source's.
        rag_cfg:     config["rag"]: embedding model, Ollama URL and rag.admission caps.
        qdrant_client: Qdrant client for the documents collection.
        connector:   The source's connector: 'filesystem', 'claude_code' or 'opencode' (see above).
                     Default 'filesystem', so a caller that sends bytes need not name it.
        spool_path:  Path of the uploaded bytes (see above). Read, not removed: the caller removes it.
        file_type:   The declared type of the uploaded bytes, e.g. 'pdf', 'md'.
        text:        The rendered text (see above).
        chat_meta:   Optional fields spread into every point: session_id, project, session_title
                     for chat text. The identity and freshness fields are written after it, so
                     they always win.
        embed_concurrency: Max parallel embedding requests for this item.
        modified_at: The item's modification time as an ISO-8601 UTC string, written unchanged to
                     every point. The API produces it as datetime.isoformat() of a UTC datetime.
                     None when the caller does not know it.
        tag:         The item's tag, written to every point. '' when the item has none.
        project_id:  The ragnart project the item attaches to, written to every point. 'global' when
                     the item attaches to none. The caller resolves it (the API checks it exists).

    Returns:
        An IngestResult. Admission refusals come back as status 'ignored' with the reason, the
        same wording the NAS side records. Exceptions (a parse failure, an embedding failure, a
        Qdrant error) propagate to the caller.

    Raises:
        ValueError: when the connector is unknown; when the content form does not match the connector
                    (bytes for a session connector, text for filesystem); when neither or both content
                    forms are given; when a spool is given with no file_type, or when file_type is not
                    a plain word.
    """
    if (spool_path is None) == (text is None):
        raise ValueError("ingest_content needs exactly one of spool_path (bytes) or text")
    if spool_path is not None and file_type is None:
        raise ValueError("ingest_content needs file_type with spool_path")

    # The connector decides the content form and the item's type. A mismatch is refused, not guessed.
    if connector == _CONNECTOR_FILESYSTEM:
        if spool_path is None:
            raise ValueError("a filesystem item is uploaded as bytes (spool_path), not as text")
    elif connector in (_CONNECTOR_CLAUDE_CODE, _CONNECTOR_OPENCODE):
        if text is None:
            raise ValueError(f"a {connector} item is sent as text, not as bytes (spool_path)")
    else:
        raise ValueError(f"unknown connector {connector!r}: use 'filesystem', 'claude_code' or 'opencode'")

    model      = rag_cfg.get("embedding_model", "bge-m3")
    ollama_url = rag_cfg.get("ollama_url", "http://192.168.1.95:11434/api/embeddings")
    max_text_bytes, fffd_threshold, max_chunks = _admission_limits(rag_cfg)
    chat_fields = dict(chat_meta or {})

    # The type stored on the points. Bytes keep their declared type. The session connectors store
    # their kind: claude_session/claude_chat, or opencode_session/opencode.
    if connector == _CONNECTOR_FILESYSTEM:
        raw_file_type = _declared_file_type(file_type)
        stored_file_type = raw_file_type
        source_type = "file"
    elif connector == _CONNECTOR_OPENCODE:
        raw_file_type = None
        stored_file_type = "opencode"
        source_type = "opencode_session"
    else:
        raw_file_type = None
        stored_file_type = "claude_chat"
        source_type = "claude_session"

    # ------------------------------------------------------------------
    # Step 0: delete stale points of this item (delete-before-insert). It runs
    # before admission, so an item that is now ignored loses its old points too.
    # ------------------------------------------------------------------
    ensure_collection(qdrant_client)
    qdrant_client.delete(
        collection_name=DOCUMENTS_COLLECTION,
        points_selector=FilterSelector(filter=item_filter(source_id, key)),
    )

    # ------------------------------------------------------------------
    # Admission and parse
    # ------------------------------------------------------------------
    if spool_path is not None:
        item_text, reason = await _admit_and_parse_spool(
            spool_path, raw_file_type, max_text_bytes, fffd_threshold,
        )
    else:
        # Rendered text is not parsed and has no size cap, NUL sniff or replacement check.
        item_text, reason = text, None
    if reason is not None:
        logger.warning("Skipping %s — %s", key, reason)
        return IngestResult("ignored", reason, 0, None, PIPELINE_VERSION_P1)

    # The file name shown on hits: the last part of the key, or the session title for chats.
    # The key is split as a string. It is not a path.
    file_name = key.rsplit("/", 1)[-1]
    if chat_fields.get(FIELD_SESSION_TITLE):
        file_name = chat_fields[FIELD_SESSION_TITLE]

    if not item_text.strip():
        logger.warning("Empty document, skipping: %s", key)
        return IngestResult("completed", None, 0, None, PIPELINE_VERSION_P1)

    reason = _printable_reason(item_text)
    if reason is not None:
        logger.warning("Skipping %s — %s (binary/corrupt content)", key, reason)
        return IngestResult("ignored", reason, 0, None, PIPELINE_VERSION_P1)

    # ------------------------------------------------------------------
    # Chunk
    # ------------------------------------------------------------------
    pairs, strategy = _select_chunks(item_text, stored_file_type)
    n = len(pairs)
    reason = _chunk_cap_reason(n, max_chunks)
    if reason is not None:
        logger.warning("Skipping %s — %s", file_name, reason)
        return IngestResult("ignored", reason, 0, None, PIPELINE_VERSION_P1)
    logger.info("Chunked %s → %d chunks  strategy=%s  file_type=%s", file_name, n, strategy, stored_file_type)
    if not pairs:
        logger.warning("No chunks produced, skipping: %s", key)
        return IngestResult("completed", None, 0, None, PIPELINE_VERSION_P1)

    # ------------------------------------------------------------------
    # Hash, taken before embedding and before the points are built:
    #   filesystem: MD5 of the uploaded bytes (the NAS side computes the same hash for a file).
    #   claude_code: MD5 of the rendered text as UTF-8 (B1 behaviour; see the docstring).
    #   opencode:   no hash. None is stored for OpenCode content, so none is stored here.
    # ------------------------------------------------------------------
    if connector == _CONNECTOR_FILESYSTEM:
        _size_bytes, content_hash = await asyncio.to_thread(hash_file, spool_path)
    elif connector == _CONNECTOR_CLAUDE_CODE:
        content_hash = hash_text(item_text)
    else:
        content_hash = None

    # ------------------------------------------------------------------
    # Embed the child texts
    # ------------------------------------------------------------------
    child_texts = [child for child, _ in pairs]
    vectors = await embed_batch(
        child_texts, model=model, ollama_url=ollama_url, concurrency=embed_concurrency,
    )

    # ------------------------------------------------------------------
    # Build the points. The payload carries the item's identity fields.
    # ------------------------------------------------------------------
    now_iso = datetime.now(timezone.utc).isoformat()
    sparse_ready = _is_sparse_ready(qdrant_client)
    points = []
    for chunk_idx, ((child_text, parent_text), vector) in enumerate(zip(pairs, vectors)):
        if sparse_ready:
            sparse_indices, sparse_values = embed_sparse(child_text)
            point_vector = {
                "": vector,  # "" is the Qdrant key for the unnamed dense vector
                SPARSE_VECTOR_NAME: SparseVector(indices=sparse_indices, values=sparse_values),
            }
        else:
            point_vector = vector  # dense-only until migration is complete

        points.append(
            PointStruct(
                id=str(uuid.uuid4()),
                vector=point_vector,
                payload={
                    # The chat fields come first, so the identity and freshness fields written
                    # after them always win. source_id is the one passed in, never one from the key.
                    **chat_fields,
                    FIELD_TEXT:        child_text,
                    FIELD_PARENT_TEXT: parent_text,
                    FIELD_OWNER:       owner,
                    FIELD_SOURCE_PATH: key,
                    FIELD_FILE_NAME:   file_name,
                    FIELD_FILE_TYPE:   stored_file_type,
                    FIELD_PAGE:        0,   # page-level tracking: future enhancement
                    FIELD_CHUNK_INDEX: chunk_idx,
                    FIELD_TAG:         tag,
                    FIELD_SOURCE_ID:   source_id,
                    FIELD_PIPELINE_VERSION: PIPELINE_VERSION_P1,
                    FIELD_PROJECT_ID:  project_id,
                    FIELD_SOURCE_TYPE: source_type,
                    FIELD_MODIFIED_AT: modified_at,
                    FIELD_INGESTED_AT: now_iso,
                    FIELD_CONTENT_HASH: content_hash,
                },
            )
        )

    # ------------------------------------------------------------------
    # Upsert in batches (see _UPSERT_BATCH_SIZE for why)
    # ------------------------------------------------------------------
    for batch_start in range(0, len(points), _UPSERT_BATCH_SIZE):
        qdrant_client.upsert(
            collection_name=DOCUMENTS_COLLECTION,
            points=points[batch_start : batch_start + _UPSERT_BATCH_SIZE],
        )

    logger.info(
        "Ingested %s: %d points  source=%s owner=%s", file_name, len(points), source_id, owner,
    )
    return IngestResult("completed", None, len(points), content_hash, PIPELINE_VERSION_P1)
