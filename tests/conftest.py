from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repository root is on sys.path so tests can import the local package
# without requiring an editable install.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
