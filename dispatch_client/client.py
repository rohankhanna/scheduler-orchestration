from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx

from dispatch_client.errors import DispatchAPIError


@dataclass(frozen=True)
class DispatchClientConfig:
    base_url: str
    api_key: str
    timeout_seconds: float = 5.0


class DispatchClient:
    """Synchronous Python client for the Dispatch local API.

    Canonical auth header: X-API-Key.

    This client is intended to be the single source of truth for API mechanics
    (paths, params, headers, error parsing) so downstream repos do not need shims.
    """

    def __init__(self, *, config: DispatchClientConfig, http: httpx.Client | None = None) -> None:
        if not config.base_url or not config.base_url.strip():
            raise ValueError("base_url is required")
        if not config.api_key or not config.api_key.strip():
            raise ValueError("api_key is required")
        if config.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self.config = config
        self._owns_http = http is None
        self.http = http or httpx.Client(base_url=config.base_url, timeout=config.timeout_seconds)

    def close(self) -> None:
        if self._owns_http:
            self.http.close()

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self.config.api_key}

    def _raise_for_status(self, response: httpx.Response) -> None:
        if 200 <= response.status_code < 300:
            return

        error = None
        message = None
        body: Any | None = None
        try:
            body = response.json()
            if isinstance(body, dict):
                if isinstance(body.get("error"), str):
                    error = body.get("error")
                if isinstance(body.get("message"), str):
                    message = body.get("message")
        except Exception:
            try:
                body = response.text
            except Exception:
                body = None

        raise DispatchAPIError(
            status_code=int(response.status_code),
            error=error,
            message=message,
            body=body,
            url=str(response.request.url) if response.request else None,
        )

    def submit_job(self, spec: dict[str, Any]) -> dict[str, Any]:
        r = self.http.post(
            "/v1/jobs",
            headers=self._headers(),
            json={"spec": spec},
            timeout=self.config.timeout_seconds,
        )
        self._raise_for_status(r)
        return dict(r.json())

    def get_job(self, server_job_id: str, *, refresh: bool = False) -> dict[str, Any]:
        q = urlencode({"refresh": int(bool(refresh))}) if refresh else ""
        path = f"/v1/jobs/{server_job_id}" + (f"?{q}" if q else "")
        r = self.http.get(path, headers=self._headers(), timeout=self.config.timeout_seconds)
        self._raise_for_status(r)
        return dict(r.json())

    def get_job_logs(self, server_job_id: str) -> dict[str, Any]:
        r = self.http.get(
            f"/v1/jobs/{server_job_id}/logs",
            headers=self._headers(),
            timeout=self.config.timeout_seconds,
        )
        self._raise_for_status(r)
        return dict(r.json())

    def list_jobs(self, *, refresh: bool = False) -> dict[str, Any]:
        q = urlencode({"refresh": int(bool(refresh))}) if refresh else ""
        path = "/v1/jobs" + (f"?{q}" if q else "")
        r = self.http.get(path, headers=self._headers(), timeout=self.config.timeout_seconds)
        self._raise_for_status(r)
        return dict(r.json())
