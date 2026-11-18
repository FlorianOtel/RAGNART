"""
Shared payload field name constants and owner derivation.

All ingestion and search code must import field names from here — no
string literals for Qdrant payload keys anywhere else in the codebase.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Qdrant payload field name constants
# ---------------------------------------------------------------------------

FIELD_TEXT = "text"                 # child chunk content (what was embedded)
FIELD_PARENT_TEXT = "parent_text"   # parent chunk (what gets returned to LLM)
FIELD_OWNER = "owner"               # "florian" | "eva" | "annika" | "laura" | "la-familia"
FIELD_SOURCE_PATH = "source_path"   # full NFS path — provenance returned to user
FIELD_FILE_NAME = "file_name"
FIELD_FILE_TYPE = "file_type"       # pdf | docx | pptx | txt | ipynb | md | yaml
FIELD_PAGE = "page"                 # page number / slide index / notebook cell index
FIELD_CHUNK_INDEX = "chunk_index"   # child chunk index within its parent
FIELD_TAG = "tag"                   # e.g. "certifications", "cisco-backup", "family"
FIELD_SESSION_ID = "session_id"     # UUID of the Claude Code session — indexed for Qdrant filter queries
FIELD_PROJECT = "project"           # NFS document folder or claude_chat project; absent on opencode points (session.directory is cwd-at-launch metadata, not topic classification)
FIELD_SESSION_TITLE = "session_title"  # claude_chat + opencode sessions; absent on NFS document points
FIELD_PROJECT_ID = "project_id"     # owning RAGNART project; GLOBAL_PROJECT_ID until ragnart projects exist
FIELD_SOURCE_TYPE = "source_type"   # kind of source the point came from (document, session, ...)
FIELD_MODIFIED_AT = "modified_at"   # source modification time (datetime-indexed)
FIELD_INGESTED_AT = "ingested_at"   # time this point was ingested (datetime-indexed)
FIELD_CONTENT_HASH = "content_hash" # MD5 of the source content; change/duplicate detection, not security
SPARSE_VECTOR_NAME = "sparse_text"   # named sparse vector space in the Qdrant collection

FIELD_SOURCE_ID = "source_id"               # the source (store) the point came from; see SOURCE_ID_* below
FIELD_PIPELINE_VERSION = "pipeline_version" # version of the ingestion pipeline that produced the point

# Reserved project id for items that do not belong to a user-defined project.
# Every item carries it until ragnart projects are introduced.
GLOBAL_PROJECT_ID = "global"


# ---------------------------------------------------------------------------
# Source ids (v0.3). A source id names a store, not a connector type: one per
# configured source. Reserved words (tag, name, all, project, web, none, global)
# can never be source ids; that rule is enforced when a source is registered.
# ---------------------------------------------------------------------------

SOURCE_ID_NAS = "nas"                                       # filesystem connector: the NAS crawl roots
SOURCE_ID_CLAUDE_LINUX_SHARED_NFS = "claude-linux-shared-nfs"  # Claude Code transcripts under the NFS-shared ~/.claude/projects
SOURCE_ID_OPENCODE_K11 = "opencode-k11"                     # OpenCode server on K11 (Server 2)

# The sources whose connector has the ragnart_projects capability, that is, the
# sources that can hold ragnart projects (design §4.5). scope.py reads this set
# instead of importing ragnart.connectors, because that module pulls in the
# ingestion code (docling), which the search service must never load.
# tests/test_connectors.py keeps this set in step with the connector table.
SOURCES_HOLDING_PROJECTS = frozenset({SOURCE_ID_NAS})

# The pipeline version every point and queue row carries from v0.3 on.
PIPELINE_VERSION_P1 = "p1"

# Key prefixes that name a non-filesystem source. A key with neither prefix is a
# filesystem path, and belongs to the NAS source.
_CLAUDE_KEY_PREFIX = "claude-session://"
_OPENCODE_KEY_PREFIX = "opencode://"


def source_id_for_key(key: str) -> str:
    """
    Return the source id of an item from its queue key.

    This is the only place the key-prefix rule is written. A key starting with
    'claude-session://' belongs to the Claude Code source, one starting with
    'opencode://' to the OpenCode source, and any other key is a NAS path.

    Args:
        key: The ingestion-queue key (file_path column): a NAS path, a
             claude-session:// key or an opencode:// key.

    Returns:
        One of the SOURCE_ID_* constants.
    """
    if key.startswith(_CLAUDE_KEY_PREFIX):
        return SOURCE_ID_CLAUDE_LINUX_SHARED_NFS
    if key.startswith(_OPENCODE_KEY_PREFIX):
        return SOURCE_ID_OPENCODE_K11
    return SOURCE_ID_NAS


# ---------------------------------------------------------------------------
# Owner derivation
# ---------------------------------------------------------------------------

def derive_owner(file_path: str, user_config: dict) -> str:
    """
    Map an NFS file path to its owner string.

    Checks per-user NFS roots first, then the shared root.
    Raises ValueError if the path is not under any configured root.

    Args:
        file_path:   Absolute NFS path (e.g. /mnt/nfs/Florian/docs/cert.pdf)
        user_config: Top-level config dict — must contain 'users' and 'shared' keys.

    Returns:
        Owner string, e.g. "florian", "eva", "la-familia".
    """
    for _email, cfg in user_config.get("users", {}).items():
        for root in cfg.get("nfs_roots", []):
            if file_path.startswith(root):
                return cfg["owner"]

    shared = user_config.get("shared", {})
    for root in shared.get("nfs_roots", []):
        if file_path.startswith(root):
            return shared["owner"]

    raise ValueError(
        f"File {file_path!r} is not under any configured NFS root. "
        f"Users: {list(user_config.get('users', {}).keys())}, "
        f"Shared: {shared.get('nfs_roots', [])}"
    )
