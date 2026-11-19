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
