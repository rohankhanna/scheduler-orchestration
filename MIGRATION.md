# Dispatch consumer migration guide (remove local shims)

This document is for *consumer repositories* that call the Dispatch HTTP API.

Goal
- Consumers must not carry local Dispatch HTTP shims.
- Consumers should use the official Dispatch Python client shipped by this repo: `dispatch_client`.

Why this matters
- The API contract and auth rules live in Dispatch.
- Consumer shims drift (headers, endpoints, response shapes, retries), then every consumer becomes a bespoke fork.
- Dispatch fixes should be made once (in this repo) and propagated by version bump, not reimplemented per consumer.


1) Generic, repo-agnostic migration checklist

A. Inventory and delete typical shim patterns

Delete (or collapse into workload-specific code) any consumer code that does *API mechanics*.

Typical shim patterns to delete:
- Custom HTTP wrappers around Dispatch
  - `urllib.request`, `requests`, raw `http.client`, ad-hoc `httpx` usage
  - helper functions like `dispatch_get()`, `dispatch_post()`, `call_dispatch_api()`
- Custom auth/header logic
  - code that sets `X-API-Key` (or worse, `Authorization`) manually
  - code that reads environment variables and injects headers for Dispatch
- Endpoint path hardcoding
  - strings like `/v1/jobs`, `/v1/jobs/{id}`, `/v1/jobs/{id}/logs`
  - query params like `?refresh=true` built by hand
- Bespoke retry/backoff around Dispatch endpoints
  - generic retries for `GET /v1/jobs` / `GET /v1/jobs/{id}` / `list_jobs`
  - keep workload-specific retries only (e.g., “wait for a job to finish”, “poll until output exists”), but do not retry/translate Dispatch transport errors in a separate shim layer

Keep (consumer responsibility):
- Workload-specific “build spec” code
  - turning domain inputs into a Dispatch job spec payload
  - choosing resources, argv, environment, input/output paths
- Consumer runtime and artifact conventions
  - paths, run IDs, dataset naming, result collection
- Workload-specific orchestration logic
  - job dependency structure at the consumer layer (if the consumer is building DAGs)
  - polling/wait loops that encode *workload semantics* (not HTTP mechanics)


B. Replace-with snippet (official client)

This snippet is intentionally minimal: it shows the only supported way for consumers to call Dispatch.

Prereqs
- Consumer environment provides a base URL and API key.
- Dispatch uses the canonical auth header `X-API-Key` (handled by the official client).

Runnable example (Python)

  python -c "from dispatch_client import DispatchClient, DispatchClientConfig; import os, json; base_url=os.environ.get('DISPATCH_BASE_URL','http://127.0.0.1:8780'); api_key=os.environ['DISPATCH_API_KEY']; c=DispatchClient(DispatchClientConfig(base_url=base_url, api_key=api_key)); spec={'payload': {'argv': ['/usr/bin/env','bash','-lc','echo dispatch-smoke && exit 0']}}; r=c.submit_job(spec); assert r['accepted'] is True, r; job_id=r['server_job_id']; j=c.get_job(job_id, refresh=True); assert j.get('server_job_id')==job_id, j; print(json.dumps({'accepted': r['accepted'], 'server_job_id': job_id, 'state': j.get('state')}, indent=2))"

Notes
- `payload.argv` must be a list of strings and should be self-contained (absolute paths preferred).
- Do not reintroduce endpoint strings or header logic in the consumer.


2) Bespoke consumer propagation plan (NO edits here)

Bespoke should depend on `dispatch_client` and delete its Dispatch HTTP shim.

A. Where the Dispatch API mechanics are likely to live in bespoke

Search targets (file patterns and strings):
- Filenames that often contain a shim:
  - `*dispatch*client*.py`, `*dispatch*api*.py`, `*orchestrat*client*.py`, `*scheduler*client*.py`, `*http*dispatch*.py`
  - `transport.py`, `client.py`, `api.py` under any `dispatch/`, `orchestration/`, `scheduler/`, or `integration/` package
- Code strings that strongly indicate shim code:
  - `X-API-Key`
  - `127.0.0.1:8780` or `DISPATCH_BASE_URL`
  - `/v1/jobs` or `/v1/jobs/`
  - `requests.` / `urllib.request` / `httpx.` / `http.client`

Recommended search commands to run inside bespoke (examples):

  rg -n "X-API-Key|/v1/jobs|DISPATCH_BASE_URL|127\.0\.0\.1:8780|requests\.|urllib\.request|httpx\." .
  rg -n "dispatch.*(get|post|delete)|call_dispatch|dispatch_api" .

B. What should be deleted vs what should remain

Delete in bespoke:
- Bespoke-side Dispatch HTTP client shim module(s)
  - anything whose job is: build URLs, attach auth headers, do JSON encode/decode, interpret HTTP errors
- Any “compat layers” around Dispatch response shapes
  - code that maps Dispatch responses into bespoke-specific dataclasses purely to compensate for drift
- Endpoint constants for Dispatch
  - `DISPATCH_JOBS_PATH = '/v1/jobs'` etc.

Keep in bespoke:
- Workload-specific spec construction
  - functions that build the job spec dict: argv selection, resource requests, env vars, mounts, artifact paths
- Workload-specific semantics
  - “wait until done”, “poll until logs contain X”, “treat exit code N as retryable” (these are workload rules)
- Observability/reporting integrations
  - metrics, structured logs, run summaries (as long as they are not re-implementing the Dispatch transport)

C. Replacement wiring in bespoke

Replace the shim usage with the official client:
- Instantiate once (or per logical component) using `DispatchClientConfig(base_url=..., api_key=...)`.
- Call only:
  - `submit_job(spec)`
  - `get_job(server_job_id, refresh=True/False)`
  - `list_jobs(refresh=True/False)`
  - `get_job_logs(server_job_id)`


3) Verification plan for consumers

A. Single command: submit a smoke job and confirm it is visible

This should be run in the consumer environment (after the consumer has migrated).

  python -c "from dispatch_client import DispatchClient, DispatchClientConfig; import os; base_url=os.environ.get('DISPATCH_BASE_URL','http://127.0.0.1:8780'); api_key=os.environ['DISPATCH_API_KEY']; c=DispatchClient(DispatchClientConfig(base_url=base_url, api_key=api_key)); spec={'payload': {'argv': ['/usr/bin/env','bash','-lc','echo dispatch-consumer-smoke && exit 0']}}; r=c.submit_job(spec); assert r['accepted'] is True, r; job_id=r['server_job_id']; jobs=c.list_jobs(refresh=True); assert any(j.get('server_job_id')==job_id for j in jobs), jobs; j=c.get_job(job_id, refresh=True); assert j.get('server_job_id')==job_id, j; print('OK server_job_id='+job_id)"

What this validates
- `accepted == true`
- `server_job_id` is returned
- the job is visible via `list_jobs(refresh=True)`
- `get_job(refresh=True)` returns a record for that job

B. Single command: fetch logs for a real job

Once real execution is enabled for the consumer workload, use:

  python -c "from dispatch_client import DispatchClient, DispatchClientConfig; import os, sys; base_url=os.environ.get('DISPATCH_BASE_URL','http://127.0.0.1:8780'); api_key=os.environ['DISPATCH_API_KEY']; job_id=os.environ['DISPATCH_JOB_ID']; c=DispatchClient(DispatchClientConfig(base_url=base_url, api_key=api_key)); print(c.get_job_logs(job_id))"


Appendix: template PR message for consumers

Title
- Remove bespoke Dispatch HTTP shim; adopt official `dispatch_client`

Summary
- Deleted bespoke Dispatch HTTP wrapper/shim code.
- Replaced direct HTTP calls + endpoint constants with `dispatch_client.DispatchClient`.
- Preserved workload-specific job spec construction and artifact conventions.

Why
- Prevents API drift and duplicated auth/path logic.
- Dispatch is the source of truth for API mechanics; consumers should depend on the official client.

Verification
- Ran the consumer smoke job submission command from `scheduler-orchestration/MIGRATION.md`.
- Confirmed `accepted == true`, a `server_job_id` was returned, job visible via `list_jobs(refresh=True)`, and logs retrievable via `get_job_logs`.
