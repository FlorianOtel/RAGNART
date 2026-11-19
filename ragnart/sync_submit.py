"""
Submit half of the NAS ingestion side (v0.3 session B2): carries a SyncPlan out as ingestion API requests.

The hub is the only writer of the index. The NAS side (sources nas, claude-linux-shared-nfs and
opencode-k11) therefore submits every change through the ingestion API, with one HubClient per source id,
and keeps its own state database (rag_state.db) as the record of what it has submitted.

submit_plan() sends every change instead. It keeps the same order and the same crash-safety reasoning,
rewritten for requests. Each step is safe to repeat after a crash, and the state changes only once the
hub has accepted the request the change depends on:

1. Projects. Each marker is published, then each removal is sent. The hub writes its registry row
   synchronously, so the project exists when its answer comes back. A project the hub refuses or cannot
   be reached for is logged and skipped. Its items then attach to 'global' or to another project, and
   the attachment step skips a change the hub refuses.
2. Moves. change_key is sent first. Only after the hub accepts it is the scan's pending row at the new
   key deleted and the completed row renamed. A crash between the two leaves the move to be detected
   again on the next run: the old file is still gone, the new one is still pending, and change_key is
   sent again. Nothing is re-embedded in either case.
3. Reappeared rows: local only. missing_since is cleared.
4. Newly missing rows: local only. missing_since is set (the first time only). Their points are kept.
5. Purge. delete is sent for each row whose grace period is over. The row is removed only after the hub
   accepts the delete. A delete the hub accepts but later loses is repaired by reconciliation, which
   deletes a hub item with no local row.
6. Stale rows: local only (they have no points).
7. Attachment. change_metadata(project_id) is sent. The row's project_id is set after acceptance.
8. Upserts. Every pending row is submitted last, so each project it names exists (step 1) and each
   move has happened first (step 2). A file is uploaded as bytes. A claude or opencode session is
   rendered to text and sent as text. An accepted request marks the row 'processing' with its request
   id, so the status poll (ragnart/sync_poll.py) can follow it on the next run. An uploaded file's
   size, MD5 and hash time are recorded on the row at the same acceptance (see _send_item).

Outcomes of one upsert:
  - accepted by the hub: the row is 'processing', with its request id. Its launch_dir (sessions) and
    project_id are stored now, because attachment reads them from the row before the hub has finished.
    An uploaded file's size, MD5 and hash time are stored too.
  - refused at the door as too large (413): an admission refusal, so the item will never be accepted as
    it is. The item's old points are deleted first, as the hub would do itself for an item it ignores.
    Then the row is 'ignored', with the hub's reason. If that delete fails, the row stays pending and the
    next run tries again. This mirrors ragnart/endpoint.py for the same status.
  - refused as a bad request (422): a request error, such as an owner sent to a fixed-owner source. It is
    not an admission decision, so nothing is deleted and the row is NOT ignored. The row stays pending,
    an ERROR is logged with the hub's detail, and the run goes on. An operator must fix the cause.
  - any other hub failure, including a transport error: the row stays pending. The next run retries it.
    No attempt is counted, because the hub never saw the request.
  - the item itself could not be read or rendered (a file that vanished, a claude session that does not
    parse): the row is 'failed' with the reason, so the retry rule of ragnart/state.py applies.
  - an OpenCode session that cannot be read now (the OpenCode server is down, or its password is
    missing): the row stays pending, because the server's state is not the item's fault.

Source of each request: the source the row's key belongs to (ragnart/schema.py source_id_for_key). The
owner is the row's owner, which the scanner set from the configured root. It is always sent, because the three NAS-side sources must have no fixed owner in the registry.
check_nas_sources_have_no_fixed_owner() refuses to run when one has, before anything is sent.

Dependencies: httpx (through ragnart.endpoint_client), the chat renderers (ragnart.chat_render, docling-free),
and the state and registry modules. No qdrant_client, docling or embedder is loaded. The import contract is
enforced by tests/test_ingestion_side_imports.py.
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

from .chat_render import _parse_claude_chat, _parse_opencode_session
from .endpoint_client import HubClient, HubError
from .hashing import hash_file
from .projects import project_id_for_path
from .registry import RegistryDB
from .schema import (
    GLOBAL_PROJECT_ID,
    OPENCODE_KEY_PREFIX,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
    source_id_for_key,
)
from .state import StateDB
from .sync_plan_core import REASON_GRACE_EXPIRED, SyncPlan

logger = logging.getLogger(__name__)

# The three sources the NAS side submits for. submit_plan() needs a client for each of them.
REQUIRED_SOURCES = (SOURCE_ID_NAS, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11)

# 413: the hub refused the item as too large for its cap. This is an admission decision: the item will
# never be accepted as it is. The same status ragnart/endpoint.py treats as a refused upsert.
ADMISSION_REFUSED = 413

# 422: the hub refused the request itself, because it is malformed or does not match the source's
# configuration (for example, an owner sent to a source with a fixed owner). The item is not judged,
# so nothing is deleted and the row is not ignored. The request is simply not accepted, and it is
# sent again on the next run until the cause is fixed.
REQUEST_REFUSED = 422

# A hub failure of any kind: the hub's own HubError (a status answer), or a transport error (no answer).
HUB_ERRORS = (httpx.HTTPError, HubError)

CLAUDE_KEY_PREFIX = "claude-session://"

# The OpenCode server address when rag.opencode_api_url is not set.
DEFAULT_OPENCODE_API_URL = "http://localhost:4096"


def _iso_utc(last_modified: float) -> str:
    """The row's last_modified (UNIX seconds) as an ISO-8601 UTC string, the form stored as modified_at."""
    return datetime.fromtimestamp(last_modified, tz=timezone.utc).isoformat()


def _project_for(match_path: str | None, registered: list[tuple[str, str]]) -> str:
    """
    The project an item attaches to: the closest enclosing project of
    match_path, or 'global' when there is no match_path or no project encloses it.

    match_path is the file's own key for a file, and the launch directory for a session.
    """
    if not match_path:
        return GLOBAL_PROJECT_ID
    return project_id_for_path(match_path, registered, GLOBAL_PROJECT_ID)


def _registered_projects(registry_db: RegistryDB) -> list[tuple[str, str]]:
    """(id, path) of every registry project that has a path. Read after the project step, so a refused project is absent."""
    return [(project["id"], project["path"]) for project in registry_db.list_projects() if project["path"]]


def check_nas_sources_have_no_fixed_owner(registry_db: RegistryDB) -> None:
    """
    Refuse to submit when a NAS-side source has a fixed owner in the registry.

    submit_plan() sends each item with its own owner. The hub accepts an owner only for a source with
    no fixed owner (ragnart/ingest_api.py _checked_owner) and answers 422 otherwise. With a fixed owner
    set, every upload would be refused, so the run is stopped here instead, before any request is sent.

    A source that is not in the registry is not checked here. The registry seeds all three sources, and
    the hub refuses a request from an unknown source on its own.

    Raises:
        RuntimeError: at least one NAS-side source has a non-NULL owner. The message names each one.
    """
    fixed = []
    for source_id in REQUIRED_SOURCES:
        source = registry_db.source_by_id(source_id)
        if source is not None and source["owner"] is not None:
            fixed.append(f"{source['id']} (fixed owner {source['owner']!r})")
    if fixed:
        raise RuntimeError(
            "refusing to submit: the NAS-side source(s) have a fixed owner in the registry, so the hub "
            "would refuse every per-item owner: " + ", ".join(fixed)
            + ". Correct the hub registry so that these sources have no fixed owner, then run again."
        )


# ---------------------------------------------------------------------------
# Steps 1 to 7
# ---------------------------------------------------------------------------

def _submit_projects(plan: SyncPlan, nas: HubClient, counts: Counter) -> None:
    """Step 1. Publish each marker, then remove each project whose marker is gone. Failures are logged and skipped."""
    for marker in plan.project_upserts:
        try:
            answer = nas.publish_project(
                marker["id"], marker["name"], path=marker["path"],
                owner=marker["owner"], tags=marker["tags"],
            )
        except HUB_ERRORS as exc:
            counts["project_failed"] += 1
            logger.error("project %s not published (the hub failed; retried next run): %s", marker["id"], exc)
            continue
        if answer.get("refused"):
            counts["project_refused"] += 1
            logger.warning("project %s refused by the hub: %s", marker["id"], answer.get("reason"))
        else:
            counts["project_published"] += 1
            logger.info("project %s published from %s", marker["id"], marker["path"])

    for project_id in plan.project_removals:
        try:
            nas.remove_project(project_id)
        except HubError as exc:
            if exc.status_code == 404:
                # Already gone on the hub: the removal is done.
                counts["project_removed"] += 1
                logger.info("project %s was already absent from the hub", project_id)
            else:
                counts["project_refused"] += 1
                logger.warning("project %s removal refused by the hub: %s", project_id, exc.detail)
        except httpx.HTTPError as exc:
            counts["project_failed"] += 1
            logger.error("project %s not removed (the hub failed; retried next run): %s", project_id, exc)
        else:
            counts["project_removed"] += 1
            logger.info("Project removed (marker no longer found): %s", project_id)


def _submit_moves(plan: SyncPlan, state_db: StateDB, clients: dict[str, HubClient], counts: Counter) -> None:
    """
    Step 2. change_key first. The local rename follows only once the hub accepts the request.

    The move target must already have its pending row. That is checked before
    anything is sent, so no request is made for a move the local state cannot record.
    """
    pending_paths = {row["file_path"] for row in state_db.pending_rows()}
    for old_path, new_path in plan.moves.items():
        if new_path not in pending_paths:
            # The scan queued new_path as pending just before this step. Without that row the move
            # cannot be recorded, so stop rather than guess.
            raise RuntimeError(f"move target has no pending row, refusing to move: {new_path}")
        try:
            clients[SOURCE_ID_NAS].change_key(old_path, new_path)
        except HUB_ERRORS as exc:
            counts["move_failed"] += 1
            logger.warning("move %s -> %s not sent (retried next run): %s", old_path, new_path, exc)
            continue
        # Accepted. The pending row at the new key goes, and the completed row takes its place.
        last_modified = state_db.delete_pending_row(new_path)
        state_db.rename_key(old_path, new_path, last_modified, origin_path=new_path)
        counts["moved"] += 1
        logger.info("Moved %s -> %s (content unchanged, no re-embed)", old_path, new_path)


def _submit_local_tombstones(plan: SyncPlan, state_db: StateDB, now_iso: str, counts: Counter) -> None:
    """Steps 3, 4 and 6. These change the local state only, and need no request."""
    for path in plan.reappeared:
        state_db.clear_missing(path)
        counts["reappeared"] += 1
        logger.info("Reappeared, tombstone cleared: %s", path)

    for path in plan.newly_missing:
        state_db.mark_missing(path, now_iso)
        counts["newly_missing"] += 1
        logger.info(
            "Tombstoned (%s), points kept until the grace period ends: %s",
            plan.reasons.get(path, "gone"), path,
        )

    for path in plan.stale_rows:
        logger.info("Queue row removed (%s): %s", plan.reasons.get(path, "gone"), path)
    if plan.stale_rows:
        state_db.purge_deleted(plan.stale_rows)
        counts["stale_removed"] += len(plan.stale_rows)


def _submit_purges(plan: SyncPlan, state_db: StateDB, clients: dict[str, HubClient], counts: Counter) -> None:
    """Step 5. delete is sent first. The row is removed only once the hub accepts it."""
    if not plan.purge_ready:
        return
    tombstones = state_db.tombstoned_rows()
    for path in plan.purge_ready:
        details = tombstones[path]
        try:
            clients[source_id_for_key(path)].delete(path)
        except HUB_ERRORS as exc:
            counts["purge_failed"] += 1
            logger.warning("purge of %s not sent (retried next run): %s", path, exc)
            continue
        state_db.purge_deleted([path])
        counts["purged"] += 1
        logger.info(
            "purged %s chunks=%s reason=%s missing_since=%s",
            path, details["chunk_count"], REASON_GRACE_EXPIRED, details["missing_since"],
        )


def _submit_attachments(plan: SyncPlan, state_db: StateDB, clients: dict[str, HubClient], counts: Counter) -> None:
    """Step 7. change_metadata(project_id) first. The local project_id is set once the hub accepts it."""
    for path, project_id in plan.attachment_changes.items():
        try:
            clients[source_id_for_key(path)].change_metadata(path, project_id=project_id)
        except HUB_ERRORS as exc:
            counts["attach_failed"] += 1
            logger.warning("re-attach of %s to %s not sent (retried next run): %s", path, project_id, exc)
            continue
        state_db.set_project_id(path, project_id)
        counts["attached"] += 1
    if plan.attachment_changes:
        logger.info("Re-attached %d item(s)", len(plan.attachment_changes))


# ---------------------------------------------------------------------------
# Step 8: upserts
# ---------------------------------------------------------------------------

def _send_item(
    row: dict,
    client: HubClient,
    registered: list[tuple[str, str]],
    oc_api_url: str,
    oc_password_env_file: str | None,
) -> tuple[dict, str | None, str, tuple[int, str, str] | None]:
    """
    Render (for a session) and send one pending row.

    Returns (the hub's answer, launch_dir, project_id, fingerprint):
      - fingerprint is (size_bytes, content_hash, hashed_at) for an uploaded file, and None for a session.
        The caller stores it when the hub accepts the request.

    A file is uploaded as its bytes, read from origin_path (the file the scan found) or from the key.
    Its file type is the suffix of the key. A session is rendered with the chat parser of
    ragnart/chat_render.py, and sent as text with its chat metadata. launch_dir is None for a file.
    """
    key = row["file_path"]
    modified_at = _iso_utc(row["last_modified"])

    if key.startswith(CLAUDE_KEY_PREFIX):
        # The key is a session identity, not a path. The real .jsonl is origin_path.
        origin = row["origin_path"]
        if not origin:
            raise RuntimeError(f"claude-session key with no origin_path recorded: {key}")
        text, metadata, launch_dir = _parse_claude_chat(Path(origin))
        project_id = _project_for(launch_dir, registered)
        answer = client.upsert_text(
            key, text, metadata=metadata, modified_at=modified_at,
            owner=row["owner"], project_id=project_id,
        )
        return answer, launch_dir, project_id, None

    if key.startswith(OPENCODE_KEY_PREFIX):
        text, metadata, launch_dir = _parse_opencode_session(key, oc_api_url, oc_password_env_file)
        project_id = _project_for(launch_dir, registered)
        answer = client.upsert_text(
            key, text, metadata=metadata, modified_at=modified_at,
            owner=row["owner"], project_id=project_id,
        )
        return answer, launch_dir, project_id, None

    # A NAS file. Attachment matches the file's own key.
    project_id = _project_for(key, registered)
    file_type = Path(key).suffix.lstrip(".").lower()
    source_path = row["origin_path"] or key

    # The fingerprint is the size and MD5 of the file, taken with the same function the scanner uses
    # (ragnart/hashing.py hash_file). The client streams the file itself (ragnart/endpoint_client.py
    # upsert_bytes), so the hash comes from a read made just before the send, not from the stream.
    # If the file changes in between, its mtime is newer than the row's last_modified, so the next scan
    # queues it again and the fingerprint is taken afresh.
    size_bytes, content_hash = hash_file(source_path)
    hashed_at = datetime.now(timezone.utc).isoformat()
    answer = client.upsert_bytes(
        source_path, key, file_type, modified_at=modified_at,
        owner=row["owner"], project_id=project_id,
    )
    return answer, None, project_id, (size_bytes, content_hash, hashed_at)


def _refuse_at_door(key: str, client: HubClient, state_db: StateDB, exc: HubError, counts: Counter) -> None:
    """
    The hub refused the upsert at the door as too large (413). Delete the item's old points, then mark the row ignored.

    Only ADMISSION_REFUSED reaches here. A 422 is a request error and is handled in _submit_upserts,
    without a delete. The delete goes first, so a file that became too large does not keep its old text
    searchable. If the delete fails the row stays pending, and the next run tries the upsert again.
    """
    try:
        client.delete(key)
    except HUB_ERRORS as delete_exc:
        counts["refused_delete_failed"] += 1
        logger.warning("%s refused, but its old points could not be deleted (retried next run): %s", key, delete_exc)
        return
    state_db.mark_ignored(key, str(exc.detail))
    counts["refused"] += 1
    logger.info("refused by the hub %s: %s", key, exc.detail)


def _submit_upserts(state_db: StateDB, registry_db: RegistryDB, clients: dict[str, HubClient], config: dict, counts: Counter) -> None:
    """Step 8. Send every pending row, oldest first. Outcomes are as described in the module docstring."""
    registered = _registered_projects(registry_db)
    rag_cfg = config.get("rag", {})
    oc_api_url = rag_cfg.get("opencode_api_url", DEFAULT_OPENCODE_API_URL)
    oc_password_env_file = config.get("opencode", {}).get("password_env_file")

    for row in state_db.pending_rows():
        key = row["file_path"]
        client = clients[source_id_for_key(key)]
        try:
            answer, launch_dir, project_id, fingerprint = _send_item(
                row, client, registered, oc_api_url, oc_password_env_file,
            )
        except HubError as exc:
            if exc.status_code == ADMISSION_REFUSED:
                _refuse_at_door(key, client, state_db, exc, counts)
            elif exc.status_code == REQUEST_REFUSED:
                # A request error, not a decision on the item: nothing is deleted and the row is not
                # ignored. The row stays pending, and the ERROR line names the cause for the operator.
                counts["request_error"] += 1
                logger.error("%s refused by the hub as a bad request (422), left pending: %s", key, exc.detail)
            else:
                counts["hub_error"] += 1
                logger.warning("%s not accepted by the hub (retried next run): %s", key, exc)
            continue
        except httpx.HTTPError as exc:
            counts["transport"] += 1
            logger.warning("%s not sent, the hub was unreachable (retried next run): %s", key, exc)
            continue
        except (OSError, RuntimeError, ValueError) as exc:
            if key.startswith(OPENCODE_KEY_PREFIX):
                # OpenCode server down or misconfigured: not the item's fault, so it stays pending.
                counts["opencode_unavailable"] += 1
                logger.warning("%s not read from OpenCode (retried next run): %s", key, exc)
            else:
                state_db.mark_failed(key, str(exc))
                counts["render_failed"] += 1
                logger.error("%s could not be read or rendered: %s", key, exc)
            continue

        if key.startswith(CLAUDE_KEY_PREFIX) or key.startswith(OPENCODE_KEY_PREFIX):
            state_db.set_launch_dir(key, launch_dir)
        state_db.set_project_id(key, project_id)
        if fingerprint is not None:
            # An uploaded file: the size and MD5 of the bytes the hub accepted, for the row's fingerprint columns.
            state_db.set_fingerprint(key, *fingerprint)
        state_db.mark_processing(key, request_id=answer["request_id"])
        counts["submitted"] += 1
        logger.info("submitted %s (request %s)", key, answer["request_id"])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def submit_plan(
    plan: SyncPlan,
    state_db: StateDB,
    registry_db: RegistryDB,
    clients: dict[str, HubClient],
    config: dict,
    now_iso: str,
) -> Counter:
    """
    Carry out a plan through the ingestion API. Returns how many of each outcome there were.

    Args:
        plan:        The SyncPlan from build_sync_plan(). Read only.
        state_db:    The NAS side's rag_state.db. Written after each accepted request.
        registry_db: The hub registry, opened for reading. It is read for the project list used to attach
                     uploads, after the project step has run. Nothing is written to it here.
        clients:     One HubClient per source id, keyed by SOURCE_ID_* (see REQUIRED_SOURCES).
        config:      The full ragnart.yaml dict. Reads rag.opencode_api_url and opencode.password_env_file.
        now_iso:     The run's time, ISO-8601 UTC. Written to missing_since.

    Raises:
        ValueError:   a source has no client. Nothing is sent.
        RuntimeError: a NAS-side source has a fixed owner in the registry (see
                      check_nas_sources_have_no_fixed_owner). Nothing is sent.
        RuntimeError: a move's target has no pending row. Nothing is sent for that move, and the steps
                      before it have already run.
    """
    missing = [source for source in REQUIRED_SOURCES if source not in clients]
    if missing:
        raise ValueError(f"no HubClient for source(s): {', '.join(missing)}")
    # Checked before any request, because every upload carries an owner that a fixed-owner source refuses.
    check_nas_sources_have_no_fixed_owner(registry_db)

    counts: Counter = Counter()
    _submit_projects(plan, clients[SOURCE_ID_NAS], counts)
    _submit_moves(plan, state_db, clients, counts)
    _submit_local_tombstones(plan, state_db, now_iso, counts)
    _submit_purges(plan, state_db, clients, counts)
    _submit_attachments(plan, state_db, clients, counts)
    _submit_upserts(state_db, registry_db, clients, config, counts)
    return counts
