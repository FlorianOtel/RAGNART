"""
Connectors: the one-directional interface over the three built source types (v0.3, session A).

A connector is a type: the code that knows one kind of store. A source is one
configured instance of a connector (docs/RAGNART-design.md §4.7). This module
describes the seven contract elements of §4.7 for each built connector, and
points at the existing scanner and parser functions by reference. No logic
moves here, and nothing is rewired through it yet: the scanner and ingestion
code still call their own functions directly.

Dependency direction. This module imports ragnart.scanner and ragnart.ingest.
Neither of those may import this module. Doing so would create an import cycle
and let the connector table drive code it was only meant to describe.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .ingest import _parse_claude_chat, _parse_opencode_session, _parse_to_text
from .scanner import scan_claude_chats, scan_nfs_roots, scan_opencode_sessions
from .schema import (
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
)

# Capability name for a connector that can hold ragnart projects: it needs a tree
# and write access (design §4.5). Only the filesystem connector has it in v0.3.
# scope.py checks this name before it accepts a source-qualified path term.
CAPABILITY_RAGNART_PROJECTS = "ragnart_projects"


@dataclass(frozen=True)
class Connector:
    """
    The seven contract elements of docs/RAGNART-design.md §4.7, for one connector.

    The string fields describe the contract for people and for the design; they are
    not parsed by code. The callable fields are the existing functions, stored by
    reference. Their signatures differ per connector, because each keeps its own
    locator (see each field below).
    """

    # Identity of the source this connector backs, and the connector's type name.
    source_id: str
    connector_type: str  # "filesystem" | "claude_code" | "opencode"

    # Element 1: the key, the identity of an item within one source.
    key_format: str
    # Element 2: the locator, what is needed to read an item's content. Opaque outside the connector.
    locator_format: str
    # Element 3: the display label for people. No guarantee.
    display_format: str

    # Element 4: enumeration of the source's items, with a change token per item,
    # and the completeness signal that says whether the enumeration saw everything.
    # Signature: enumerate(state_db, config, user_filter=None) -> dict.
    enumerate: Callable[..., dict]
    completeness_signal: str

    # Element 5: the content of one item. The signature depends on the connector:
    # filesystem fetch(file_path, file_type); claude_code fetch(path: Path);
    # opencode fetch(path, oc_api_url, password_env_file).
    fetch: Callable[..., Any]

    # Element 6: optional capabilities, as names from this module or from the design.
    capabilities: frozenset[str]

    # Element 7: the connector's own rule for several copies of one item. Text only.
    duplicate_rule: str


# The filesystem connector: the files under the NAS crawl roots.
FILESYSTEM = Connector(
    source_id=SOURCE_ID_NAS,
    connector_type="filesystem",
    key_format="The full path of the file",
    locator_format="The same path",
    display_format="The file name",
    enumerate=scan_nfs_roots,
    completeness_signal=(
        "Incomplete when a crawl root is not a directory, or yields no file while "
        "queue rows exist under it"
    ),
    # Submits bytes: the hub parses them by file type (_parse_to_text).
    fetch=_parse_to_text,
    capabilities=frozenset({
        "exclusion",                    # .ragignore files (design §1.2)
        CAPABILITY_RAGNART_PROJECTS,    # can hold ragnart projects (design §4.5)
        "location",                     # resolves a location to its ragnart project (design §7.4)
    }),
    duplicate_rule=(
        "A file reached through several links is queued once (design §1.2, deviation C)"
    ),
)

# The Claude Code connector: transcripts under the NFS-shared ~/.claude/projects tree.
CLAUDE_CODE = Connector(
    source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    connector_type="claude_code",
    key_format="claude-session://<uuid>[/agent-<id>]",
    locator_format="The transcript's path",
    display_format="Session title",
    enumerate=scan_claude_chats,
    completeness_signal=(
        "Incomplete when a transcript root is not a directory, or yields no file while "
        "queue rows exist under it"
    ),
    # Submits text: the connector renders user and assistant turns (_parse_claude_chat).
    fetch=_parse_claude_chat,
    capabilities=frozenset(),
    duplicate_rule="One session uuid is one item; the newest copy wins (design §4.2)",
)

# The OpenCode connector: sessions read from the OpenCode server's HTTP API.
OPENCODE = Connector(
    source_id=SOURCE_ID_OPENCODE_K11,
    connector_type="opencode",
    key_format="opencode://<id>",
    # No locator: the session is read by its id through the OpenCode API.
    locator_format="None: the session is read by its id through the OpenCode API",
    display_format="Session title",
    enumerate=scan_opencode_sessions,
    completeness_signal="Incomplete when the OpenCode API cannot be read",
    # Submits text: the connector reads user and assistant turns from the API (_parse_opencode_session).
    fetch=_parse_opencode_session,
    capabilities=frozenset(),
    duplicate_rule="None",
)

# Every built connector, keyed by its source id.
BY_SOURCE_ID: dict[str, Connector] = {
    FILESYSTEM.source_id: FILESYSTEM,
    CLAUDE_CODE.source_id: CLAUDE_CODE,
    OPENCODE.source_id: OPENCODE,
}
