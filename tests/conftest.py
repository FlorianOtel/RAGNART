"""
Pytest configuration shared by every test module.

The tests import the `ragnart` package and the `utils/` scripts directly from the
repository checkout. The repository root is not installed into the shared venv
(an editable install is a separate, operator-approved step), so we put the root
on sys.path here. This mirrors the `sys.path.insert(...)` pattern used at the top
of the `utils/rag_*.py` scripts.
"""

import sys
from pathlib import Path

# tests/conftest.py -> parent is tests/, parent.parent is the repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Insert at the front so the checkout's `ragnart` package wins over any installed copy.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
