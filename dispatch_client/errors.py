from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DispatchAPIError(RuntimeError):
    """Raised when Dispatch returns a non-2xx response."""

    status_code: int
    error: str | None = None
    message: str | None = None
    body: Any | None = None
    url: str | None = None

    def __str__(self) -> str:
        parts = [f"status={self.status_code}"]
        if self.error:
            parts.append(f"error={self.error}")
        if self.message:
            parts.append(f"message={self.message}")
        if self.url:
            parts.append(f"url={self.url}")
        return "DispatchAPIError(" + ", ".join(parts) + ")"
