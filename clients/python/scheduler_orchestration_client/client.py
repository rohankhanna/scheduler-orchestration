from __future__ import annotations

from typing import Any

import httpx


class SchedulerOrchestrationClient:
    """Synchronous Python client for the local orchestration server API."""

    DEFAULT_TIMEOUT_SECONDS = 5.0

    def __init__(
        self,
        *,
        http: httpx.Client,
        api_key: str,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self.http = http
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self.api_key}

    def submit_job(self, spec: dict[str, Any]) -> dict[str, Any]:
        r = self.http.post(
            "/v1/jobs",
            headers=self._headers(),
            json={"spec": spec},
            timeout=self.timeout_seconds,
        )
        r.raise_for_status()
        return dict(r.json())

    def get_job(self, server_job_id: str) -> dict[str, Any]:
        r = self.http.get(
            f"/v1/jobs/{server_job_id}",
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        r.raise_for_status()
        return dict(r.json())

    def list_queue(self) -> dict[str, Any]:
        r = self.http.get(
            "/v1/queue",
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        r.raise_for_status()
        return dict(r.json())

    def set_drain(self, enabled: bool) -> dict[str, Any]:
        r = self.http.post(
            "/v1/drain",
            headers=self._headers(),
            json={"enabled": bool(enabled)},
            timeout=self.timeout_seconds,
        )
        r.raise_for_status()
        return dict(r.json())


class AsyncSchedulerOrchestrationClient:
    """Async Python client for the local orchestration server API.

    Security defaults:
    - requires explicit API key
    - uses bounded timeouts by default
    - sends credentials only via header

    Note: This is the easiest variant to test against an ASGI app using
    httpx.ASGITransport.
    """

    DEFAULT_TIMEOUT_SECONDS = 5.0

    def __init__(
        self,
        *,
        http: httpx.AsyncClient,
        api_key: str,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self.http = http
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self.api_key}

    async def submit_job(self, spec: dict[str, Any]) -> dict[str, Any]:
        r = await self.http.post(
            "/v1/jobs",
            headers=self._headers(),
            json={"spec": spec},
            timeout=self.timeout_seconds,
        )
        r.raise_for_status()
        return dict(r.json())

    async def get_job(self, server_job_id: str) -> dict[str, Any]:
        r = await self.http.get(
            f"/v1/jobs/{server_job_id}",
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        r.raise_for_status()
        return dict(r.json())

    async def list_queue(self) -> dict[str, Any]:
        r = await self.http.get(
            "/v1/queue",
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        r.raise_for_status()
        return dict(r.json())

    async def set_drain(self, enabled: bool) -> dict[str, Any]:
        r = await self.http.post(
            "/v1/drain",
            headers=self._headers(),
            json={"enabled": bool(enabled)},
            timeout=self.timeout_seconds,
        )
        r.raise_for_status()
        return dict(r.json())
