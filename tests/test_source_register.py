"""
Tests for utils/rag_source_register.py: the dry-run writes nothing, the apply stores only the hash
of a secret that sits in a 0600 file, an existing or case-only-duplicate id is refused with no
file left behind, and the secret never reaches stdout or stderr.

The hub registry and ragnart.yaml are tmp files. The ingestion lock is replaced by a recorder, so
a test can assert that the lock is taken on --apply and never on the dry-run.
"""

from __future__ import annotations

import hashlib
import importlib.util
import stat
import sys
from pathlib import Path

import pytest
import yaml

from ragnart.cli import registry_db_path
from ragnart.registry import RegistryDB

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def tool(monkeypatch):
    """Load utils/rag_source_register.py as a module. It is a script, not a package."""
    name = "rag_source_register"
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "utils" / "rag_source_register.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def env(tmp_path, tool, monkeypatch):
    """
    A tmp ragnart.yaml pointing at a tmp db_base_path, and a recorder in place of the lock.

    Returns a dict with the config and the list of lock calls.
    """
    cfg = {
        "db_base_path": str(tmp_path / "db"),
        "rag": {"ingest_lock": str(tmp_path / "rag-ingest.lock")},
    }
    config_path = tmp_path / "ragnart.yaml"
    config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(tool, "CONFIG_PATH", config_path)

    lock_calls: list[str] = []
    monkeypatch.setattr(tool, "acquire_or_exit", lambda path: lock_calls.append(path))
    (tmp_path / "secrets").mkdir()
    return {"config": cfg, "lock_calls": lock_calls, "tmp": tmp_path}


def _registry(env) -> RegistryDB:
    return RegistryDB(registry_db_path(env["config"]))


def _apply_args(env, source_id: str, secret_out: Path, connector: str = "filesystem") -> list[str]:
    return ["--id", source_id, "--connector", connector, "--owner", "florian",
            "--apply", "--secret-out", str(secret_out)]


def _dry_args(source_id: str, connector: str = "filesystem") -> list[str]:
    return ["--id", source_id, "--connector", connector, "--owner", "florian"]


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def test_dry_run_writes_nothing_and_takes_no_lock(tool, env, capsys):
    """The default mode reports what would be registered, and creates no registry, secret or lock."""
    rc = tool.main(_dry_args("MBP"))

    assert rc == 0
    assert "would register: id=MBP kind=endpoint connector=filesystem owner=florian" in capsys.readouterr().out
    assert not Path(registry_db_path(env["config"])).exists()
    assert list((env["tmp"] / "secrets").iterdir()) == []
    assert env["lock_calls"] == []


def test_dry_run_refuses_an_id_already_registered_in_another_case(tool, env, capsys):
    """A dry-run reports a case-only duplicate as a refusal, and writes nothing."""
    registry = _registry(env)
    registry.register_source("mbp", "filesystem", "endpoint", "florian", "0" * 64)
    registry.close()

    rc = tool.main(_dry_args("MBP"))

    assert rc == 1
    assert "REFUSED" in capsys.readouterr().out
    # Only the 'mbp' row I registered is in that case-insensitive family: the dry-run added nothing.
    registry = _registry(env)
    try:
        family = [s["id"] for s in registry.list_sources() if s["id"].casefold() == "mbp"]
    finally:
        registry.close()
    assert family == ["mbp"]


@pytest.mark.parametrize("bad_id", ["a_b", "tag", "Global", "has space"])
def test_invalid_or_reserved_id_is_refused_in_dry_run(tool, env, bad_id):
    """An id with a character outside the shape, or a reserved word, is refused."""
    assert tool.main(_dry_args(bad_id)) == 1


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def test_apply_stores_the_hash_of_the_file_content(tool, env):
    """The registry holds sha256 of the file's content, which is the secret, and the lock was taken."""
    secret_file = env["tmp"] / "secrets" / "MBP"

    rc = tool.main(_apply_args(env, "MBP", secret_file))

    assert rc == 0
    secret = secret_file.read_text(encoding="utf-8")
    registry = _registry(env)
    try:
        row = registry.source_by_id("MBP")
    finally:
        registry.close()
    assert row["id"] == "MBP"
    assert row["kind"] == "endpoint"
    assert row["connector"] == "filesystem"
    assert row["owner"] == "florian"
    assert row["secret_hash"] == hashlib.sha256(secret.encode("utf-8")).hexdigest()
    assert len(env["lock_calls"]) == 1


def test_secret_file_is_mode_0600(tool, env):
    """The secret file is readable and writable by its owner only."""
    secret_file = env["tmp"] / "secrets" / "MBP"

    tool.main(_apply_args(env, "MBP", secret_file))

    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600


def test_existing_secret_out_is_refused_and_not_overwritten(tool, env):
    """An existing --secret-out is refused, its content is kept, and nothing is registered."""
    secret_file = env["tmp"] / "secrets" / "MBP"
    secret_file.write_text("old-content", encoding="utf-8")

    rc = tool.main(_apply_args(env, "MBP", secret_file))

    assert rc == 1
    assert secret_file.read_text(encoding="utf-8") == "old-content"
    registry = _registry(env)
    try:
        assert registry.source_by_id("MBP") is None
    finally:
        registry.close()


def test_case_only_duplicate_is_refused_and_leaves_no_file(tool, env):
    """Registering 'MBP' after 'mbp' is refused, and no secret file is left for the refused id."""
    first = env["tmp"] / "secrets" / "mbp"
    assert tool.main(_apply_args(env, "mbp", first)) == 0

    second = env["tmp"] / "secrets" / "MBP"
    rc = tool.main(_apply_args(env, "MBP", second))

    assert rc == 1
    assert not second.exists()
    registry = _registry(env)
    try:
        assert [s["id"] for s in registry.list_sources() if s["id"].casefold() == "mbp"] == ["mbp"]
    finally:
        registry.close()


def test_failed_registration_removes_the_secret_file(tool, env, monkeypatch):
    """If the registration fails after the file is written, the file is removed."""
    def fail(self, *args, **kwargs):
        raise ValueError("simulated registration failure")

    monkeypatch.setattr(tool.RegistryDB, "register_source", fail)
    secret_file = env["tmp"] / "secrets" / "MBP"

    rc = tool.main(_apply_args(env, "MBP", secret_file))

    assert rc == 1
    assert not secret_file.exists()


def test_apply_without_secret_out_is_a_usage_error(tool, env):
    """--apply needs --secret-out. argparse exits with 2 and nothing is taken or written."""
    with pytest.raises(SystemExit) as exc:
        tool.main(["--id", "MBP", "--connector", "filesystem", "--owner", "florian", "--apply"])
    assert exc.value.code == 2
    assert env["lock_calls"] == []


def test_connector_outside_the_b1_set_is_a_usage_error(tool, env):
    """Only filesystem and claude_code are accepted as connectors."""
    with pytest.raises(SystemExit) as exc:
        tool.main(_dry_args("MBP", connector="opencode"))
    assert exc.value.code == 2


# ---------------------------------------------------------------------------
# The secret is never printed
# ---------------------------------------------------------------------------

def test_stdout_and_stderr_never_contain_the_secret(tool, env, capsys):
    """Neither output stream carries the secret. The path of the secret file is reported."""
    secret_file = env["tmp"] / "secrets" / "MBP"

    rc = tool.main(_apply_args(env, "MBP", secret_file))

    captured = capsys.readouterr()
    secret = secret_file.read_text(encoding="utf-8")
    assert rc == 0
    assert secret not in captured.out
    assert secret not in captured.err
    assert f"secret written to {secret_file}" in captured.out
    assert "id=MBP kind=endpoint connector=filesystem owner=florian" in captured.out


# ---------------------------------------------------------------------------
# Secret rotation (--rotate-secret)
# ---------------------------------------------------------------------------

def _seed_endpoint(env, source_id: str = "MBP", connector: str = "filesystem", owner: str = "florian") -> str:
    """Register an endpoint source directly in the registry, with a known hash. Returns the hash."""
    known_hash = "e" * 64
    registry = _registry(env)
    try:
        registry.register_source(source_id, connector, "endpoint", owner, known_hash)
    finally:
        registry.close()
    return known_hash


def _stored_hash(env, source_id: str):
    registry = _registry(env)
    try:
        row = registry.source_by_id(source_id)
    finally:
        registry.close()
    return None if row is None else row["secret_hash"]


def _rotate_args(source_id: str, secret_out: Path | None = None) -> list[str]:
    args = ["--rotate-secret", "--id", source_id]
    if secret_out is not None:
        args += ["--apply", "--secret-out", str(secret_out)]
    return args


def test_rotate_dry_run_writes_nothing_and_takes_no_lock(tool, env, capsys):
    """The rotation dry-run reports the source, and leaves the stored hash, the files and the lock alone."""
    old_hash = _seed_endpoint(env)

    rc = tool.main(_rotate_args("MBP"))

    assert rc == 0
    assert "would rotate the secret of: id=MBP kind=endpoint connector=filesystem owner=florian" in capsys.readouterr().out
    assert _stored_hash(env, "MBP") == old_hash
    assert list((env["tmp"] / "secrets").iterdir()) == []
    assert env["lock_calls"] == []


def test_rotate_refuses_an_unknown_id_in_dry_run_and_apply(tool, env):
    """A source that is not registered cannot be rotated. Neither mode writes a secret file."""
    secret_file = env["tmp"] / "secrets" / "ghost"

    assert tool.main(_rotate_args("ghost")) == 1
    assert tool.main(_rotate_args("ghost", secret_file)) == 1
    assert not secret_file.exists()
    assert env["lock_calls"] == [str(env["tmp"] / "rag-ingest.lock")]


def test_rotate_apply_replaces_the_hash_with_the_hash_of_the_new_file(tool, env):
    """After --apply the stored hash is the SHA-256 of the new file's content, and the old hash is gone."""
    old_hash = _seed_endpoint(env)
    secret_file = env["tmp"] / "secrets" / "MBP"

    rc = tool.main(_rotate_args("MBP", secret_file))

    assert rc == 0
    secret = secret_file.read_text(encoding="utf-8")
    assert _stored_hash(env, "MBP") == hashlib.sha256(secret.encode("utf-8")).hexdigest()
    assert _stored_hash(env, "MBP") != old_hash
    assert len(env["lock_calls"]) == 1


def test_rotate_writes_the_new_secret_with_mode_0600(tool, env):
    """The rotated secret file is created with owner-only permissions."""
    _seed_endpoint(env)
    secret_file = env["tmp"] / "secrets" / "MBP"

    tool.main(_rotate_args("MBP", secret_file))

    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600


def test_rotate_keeps_the_kind_connector_and_owner_of_a_hub_source(tool, env, capsys):
    """Rotating works on a hub source (here the seeded nas row) and changes nothing but its hash."""
    secret_file = env["tmp"] / "secrets" / "nas"
    registry = _registry(env)
    try:
        before = registry.source_by_id("nas")
    finally:
        registry.close()

    rc = tool.main(_rotate_args("nas", secret_file))

    assert rc == 0
    registry = _registry(env)
    try:
        after = registry.source_by_id("nas")
    finally:
        registry.close()
    assert (after["kind"], after["connector"], after["owner"]) == (before["kind"], before["connector"], before["owner"])
    assert after["secret_hash"] is not None
    assert "rotated: id=nas kind=hub connector=filesystem owner=None" in capsys.readouterr().out


def test_rotate_matches_the_id_in_any_letter_case(tool, env):
    """Rotating 'mbp' finds the source registered as 'MBP', and keeps the stored case."""
    _seed_endpoint(env, "MBP")
    secret_file = env["tmp"] / "secrets" / "mbp"

    assert tool.main(_rotate_args("mbp", secret_file)) == 0

    secret = secret_file.read_text(encoding="utf-8")
    registry = _registry(env)
    try:
        row = registry.source_by_id("MBP")
    finally:
        registry.close()
    assert row["id"] == "MBP"
    assert row["secret_hash"] == hashlib.sha256(secret.encode("utf-8")).hexdigest()


def test_rotate_refuses_an_existing_secret_out_and_keeps_the_hash(tool, env):
    """An existing --secret-out is refused, its content is kept, and the stored hash does not change."""
    old_hash = _seed_endpoint(env)
    secret_file = env["tmp"] / "secrets" / "MBP"
    secret_file.write_text("old-content", encoding="utf-8")

    rc = tool.main(_rotate_args("MBP", secret_file))

    assert rc == 1
    assert secret_file.read_text(encoding="utf-8") == "old-content"
    assert _stored_hash(env, "MBP") == old_hash


def test_rotate_removes_the_new_file_when_the_hash_is_not_replaced(tool, env, monkeypatch):
    """If the hash cannot be replaced after the file is written, the new file is removed and the refusal is reported."""
    old_hash = _seed_endpoint(env)
    monkeypatch.setattr(tool.RegistryDB, "set_secret_hash", lambda self, source_id, secret_hash: False)
    secret_file = env["tmp"] / "secrets" / "MBP"

    rc = tool.main(_rotate_args("MBP", secret_file))

    assert rc == 1
    assert not secret_file.exists()
    assert _stored_hash(env, "MBP") == old_hash


@pytest.mark.parametrize("extra", [["--connector", "filesystem"], ["--owner", "eva"]])
def test_rotate_with_connector_or_owner_is_a_usage_error(tool, env, extra):
    """A rotation keeps the source's connector and owner, so passing either is a usage error."""
    with pytest.raises(SystemExit) as exc:
        tool.main(["--rotate-secret", "--id", "MBP", *extra])
    assert exc.value.code == 2
    assert env["lock_calls"] == []


def test_rotate_apply_without_secret_out_is_a_usage_error(tool, env):
    """--rotate-secret --apply needs --secret-out, like the register apply."""
    with pytest.raises(SystemExit) as exc:
        tool.main(["--rotate-secret", "--id", "MBP", "--apply"])
    assert exc.value.code == 2
    assert env["lock_calls"] == []


def test_register_without_connector_or_owner_is_a_usage_error(tool, env):
    """Registering still needs --connector and --owner, now checked after parsing."""
    with pytest.raises(SystemExit) as exc:
        tool.main(["--id", "MBP", "--owner", "florian"])
    assert exc.value.code == 2


def test_rotate_stdout_and_stderr_never_contain_the_secret(tool, env, capsys):
    """The rotated secret reaches only its file: neither output stream carries it."""
    _seed_endpoint(env)
    secret_file = env["tmp"] / "secrets" / "MBP"

    rc = tool.main(_rotate_args("MBP", secret_file))

    captured = capsys.readouterr()
    secret = secret_file.read_text(encoding="utf-8")
    assert rc == 0
    assert secret not in captured.out
    assert secret not in captured.err
    assert f"secret written to {secret_file}" in captured.out
