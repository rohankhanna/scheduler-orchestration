# scheduler-orchestration

A small, local-first orchestration integration layer for running long-horizon, resource-aware, dependency-aware job graphs on a single machine.

This project is explicitly not a scheduler core. It prefers integrating mature schedulers (iteration 1: Slurm) while exposing a stable local API for other applications to submit, observe, and control work.

## What you get

- An always-on local server with a stable API (submit, status, list, cancel, drain/resume).
- A durable job ledger on disk so operator state survives reboot.
- Backend adapters that construct scheduler commands and (optionally) execute them.

## Direction (usable-first)

The intended direction is to become a minimal, dependable substrate that other applications can target early.

That means:

- ship a stable API and contract even if individual backends are incomplete
- persist enough state to support restart and post-mortem investigation
- capture failure evidence (at least: exit code and stdout/stderr for the direct backend)
- iterate based on real workload feedback, not on speculative completeness

## Architecture

- ARCHITECTURE.md
- docs/architecture/slurm-integration-scope.md
- docs/architecture/backend-ops-execution-contract.md

## Verification (canonical)

Run the test suite:

- pytest -q

## Run the server (dev)

The repo ships an installable operator CLI named `dispatch`.

Install (editable):

- python -m pip install -e .

Then you can run from any directory:

- dispatch --help
- dispatch server start --runtime-dir /abs/path/to/dispatch-runtime

Then:

- UI: http://127.0.0.1:8780/ui
- Logs: dispatch server logs -f --runtime-dir /abs/path/to/dispatch-runtime
- Stop: dispatch server stop --runtime-dir /abs/path/to/dispatch-runtime
