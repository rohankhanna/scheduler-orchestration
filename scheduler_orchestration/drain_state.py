from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class DrainState:
    enabled: bool
    updated_at: str


def is_drain_enabled(state_path: Path) -> bool:
    """Return True if drain mode is enabled.

    Drain mode is repository-local operator state (not a Slurm cluster-level drain).
    When enabled, submission should be blocked at the CLI / adapter boundary.
    """

    if not state_path.exists():
        return False

    data = json.loads(state_path.read_text(encoding="utf-8"))
    return bool(data.get("enabled", False))


def set_drain_mode(state_path: Path, enabled: bool) -> DrainState:
    """Persist drain mode state to disk."""

    state_path.parent.mkdir(parents=True, exist_ok=True)

    updated_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "enabled": bool(enabled),
        "updated_at": updated_at,
    }
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return DrainState(enabled=bool(enabled), updated_at=updated_at)
