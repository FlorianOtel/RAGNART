"""
Scaffold check: the test harness and the pinned runtime/test dependencies import.

These tests guard the environment itself. If they fail, the shared venv does not
match the pins in pyproject.toml and no other test result can be trusted.
"""

from importlib.metadata import version


def test_pytest_imports_at_pinned_version():
    # pytest is the test-only dependency declared under [project.optional-dependencies].
    import pytest

    assert pytest.__version__ == "9.0.3"
    assert version("pytest") == "9.0.3"
