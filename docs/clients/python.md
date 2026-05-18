# Official Python client (Dispatch)

Dispatch is the single source of truth for Dispatch API mechanics.

Consumers must **not** implement local HTTP shims/adapters (paths, headers, auth quirks, error parsing). If the API contract is missing or inconvenient, fix it upstream in Dispatch and update the client here.

## Install

From a checkout (recommended during early iteration):

- python -m pip install -e .

Or upgrade to a released version once you have one available:

- python -m pip install -U dispatch-orchestration

## Import path

- `from dispatch_client import DispatchClient, DispatchClientConfig, DispatchAPIError`

## Canonical auth header

The canonical auth header is:

- `X-API-Key: <api key>`

The official client always uses `X-API-Key`.

For additive compatibility on jobs endpoints, the server also accepts:

- `Authorization: Bearer <api key>`
- `Authorization: ApiKey <api key>`

This alias exists to reduce operator confusion; consumers should still prefer the official client and canonical `X-API-Key` header.

## Example

```python
from dispatch_client import DispatchClient, DispatchClientConfig

cfg = DispatchClientConfig(
    base_url="http://127.0.0.1:8780",
    api_key="...",
    timeout_seconds=5.0,
)

client = DispatchClient(config=cfg)
try:
    submit = client.submit_job({"job_name": "example", "payload": {"argv": ["/usr/bin/env", "bash", "-lc", "echo hello"]}})
    job_id = submit["server_job_id"]
    job = client.get_job(job_id, refresh=True)
    logs = client.get_job_logs(job_id)
    listing = client.list_jobs(refresh=False)
finally:
    client.close()
```

## Consumer update recipe (replacing local shims)

1) Delete any local wrapper that hardcodes HTTP details (paths, headers, refresh query params, error parsing).
2) Add a dependency on this repo/package.
3) Replace calls with the official client methods:

- `submit_job(spec)`
- `get_job(server_job_id, refresh=...)`
- `get_job_logs(server_job_id)`
- `list_jobs(refresh=...)`

If you find a mismatch, fix Dispatch + the official client here and bump the version.
