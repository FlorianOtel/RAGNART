#!/usr/bin/env python3
"""
Register an endpoint source on the hub: one id, one connector, one owner, one secret.

An endpoint source is a store that a host (for example the operator's Mac) submits to through
the ingestion API (ragnart/ingest_api.py). Its requests carry a Bearer secret. This tool creates
the source in the hub registry (sqlite/hub_registry.db, ragnart/registry.py) and writes the
secret once, to a file the operator then copies to the host.

The hub keeps only the SHA-256 hex digest of the secret. The plaintext goes to --secret-out and
nowhere else. It is never printed, logged or returned: stdout and stderr carry the id, the kind,
the connector, the owner and the path of the secret file, and nothing else.

Modes (the dry-run is the default):

    --dry-run (default)   Validates the id and reports whether it is already registered (the
                          comparison is case-insensitive). Writes nothing, takes no lock.

    --apply               Under the ingestion lock. Refuses if the id is already registered in any
                          letter case, or if --secret-out already exists. Then:
                          1. Creates --secret-out with O_EXCL and mode 0600, and writes the secret.
                          2. Registers the source with kind 'endpoint' and the hash of the secret.
                          If step 2 fails, the secret file is removed, so no orphan secret is left.
                          The file is written before the registration, so a secret never exists
                          without its hash, and a refused run never leaves a file behind.

Usage (run from project root):
    python utils/rag_source_register.py --id MBP --connector filesystem --owner florian
    python utils/rag_source_register.py --id MBP --connector filesystem --owner florian \\
        --apply --secret-out /path/to/secrets/MBP

Exit codes: 0 when the run registered (apply) or would register (dry-run). 1 when refused: an
invalid id, an id already registered, an existing --secret-out, or a registration that failed.
2 for a usage error (argparse), including --apply without --secret-out.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import secrets
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ragnart.cli import registry_db_path  # noqa: E402
from ragnart.lock import acquire_or_exit, lock_path_from_config  # noqa: E402
from ragnart.registry import RegistryDB  # noqa: E402
from ragnart.schema import validate_source_id  # noqa: E402

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "ragnart.yaml"

# The kind every source registered by this tool gets. Hub sources are created by the migrations.
ENDPOINT_KIND = "endpoint"

# The connectors an endpoint source may use in B1. Matches the connector names in ragnart/connectors.py.
CONNECTORS = ("filesystem", "claude_code")

# Bytes of entropy in the generated secret. token_urlsafe(32) gives a 43-character string.
SECRET_BYTES = 32


def secret_hash(secret: str) -> str:
    """Return the SHA-256 hex digest of the secret. Only this digest is ever stored."""
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def write_secret_file(path: str, secret: str) -> None:
    """
    Create `path` with mode 0600 and write the secret into it.

    O_EXCL makes the call fail if the file already exists, so an existing secret is never
    overwritten. The mode is set at creation, so the secret is never readable by others.
    """
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(secret)


def _load_config() -> dict:
    """Read ragnart.yaml. Read at call time, so tests can point CONFIG_PATH elsewhere."""
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def existing_source(registry_path: str, source_id: str) -> dict | None:
    """
    Return the registered source whose id matches source_id in any letter case, or None.

    The registry file may be absent (a fresh hub): then nothing is registered yet.
    """
    if not Path(registry_path).exists():
        return None
    registry = RegistryDB(registry_path, read_only=True)
    try:
        return registry.source_by_id(source_id)
    finally:
        registry.close()


def run_dry_run(registry_path: str, source_id: str, connector: str, owner: str | None) -> int:
    """Report what --apply would do. Writes nothing. Returns the exit code."""
    print("=== rag_source_register (DRY-RUN) ===")
    try:
        validate_source_id(source_id)
    except ValueError as exc:
        print(f"REFUSED: {exc}")
        return 1
    if not Path(registry_path).exists():
        print("registry (hub_registry.db): absent; --apply would create it")
    existing = existing_source(registry_path, source_id)
    if existing is not None:
        print(f"REFUSED (would refuse on --apply): {source_id!r} is already registered as "
              f"{existing['id']!r} (ids are compared case-insensitively)")
        return 1
    print(f"would register: id={source_id} kind={ENDPOINT_KIND} connector={connector} owner={owner}")
    print("(dry-run: nothing written. Re-run with --apply and --secret-out to register.)")
    return 0


def run_apply(
    registry_path: str, source_id: str, connector: str, owner: str, secret_out: str
) -> int:
    """
    Register the source and write its secret. The caller already holds the ingestion lock.

    Returns the exit code. The secret is never printed: only the path is reported.
    """
    try:
        validate_source_id(source_id)
    except ValueError as exc:
        print(f"REFUSED: {exc}")
        return 1
    if existing_source(registry_path, source_id) is not None:
        print(f"REFUSED: {source_id!r} is already registered (ids are compared case-insensitively). "
              "Nothing was written.")
        return 1
    if os.path.lexists(secret_out):
        print(f"REFUSED: {secret_out} already exists. Nothing was written.")
        return 1

    secret = secrets.token_urlsafe(SECRET_BYTES)
    # The file comes first: if it cannot be written, nothing is registered.
    try:
        write_secret_file(secret_out, secret)
    except OSError as exc:
        print(f"REFUSED: cannot write the secret file {secret_out}: {exc.strerror}. Nothing was registered.")
        return 1

    registry = RegistryDB(registry_path)
    try:
        registry.register_source(source_id, connector, ENDPOINT_KIND, owner, secret_hash(secret))
    except ValueError as exc:
        # Registration failed (a race with another registration, say). Remove the secret file so
        # no secret is left without a registered hash.
        os.remove(secret_out)
        print(f"REFUSED: {exc}. The secret file was removed; nothing was registered.")
        return 1
    finally:
        registry.close()

    print(f"registered: id={source_id} kind={ENDPOINT_KIND} connector={connector} owner={owner}")
    print(f"secret written to {secret_out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the process exit code (see the module docstring)."""
    parser = argparse.ArgumentParser(
        description="Register an endpoint source on the hub and write its secret to a file."
    )
    parser.add_argument("--id", required=True, help="The source id, e.g. MBP. Case is kept; comparisons ignore it.")
    parser.add_argument("--connector", required=True, choices=CONNECTORS, help="The source's connector.")
    parser.add_argument("--owner", required=True, help="The fixed owner of the source's items, e.g. florian.")
    parser.add_argument(
        "--secret-out",
        help="Where the plaintext secret is written (created with mode 0600, never overwritten). "
             "Required with --apply.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report only (the default).")
    mode.add_argument("--apply", action="store_true", help="Register the source and write the secret file.")
    args = parser.parse_args(argv)
    if args.apply and not args.secret_out:
        parser.error("--apply needs --secret-out: the secret is written only to that file")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = _load_config()
    registry_path = registry_db_path(config)
    if not args.apply:
        # The dry-run takes no lock and writes nothing.
        return run_dry_run(registry_path, args.id, args.connector, args.owner)

    # Apply: take the single-writer ingestion lock before the registry is opened for writing.
    acquire_or_exit(lock_path_from_config(config))
    return run_apply(registry_path, args.id, args.connector, args.owner, args.secret_out)


if __name__ == "__main__":
    sys.exit(main())
