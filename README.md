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

## Operations

- docs/clients/python.md
- docs/operations/restart-recovery-and-logs.md
- docs/operations/dispatch-local-slurm.md
- docs/operations/dispatch-graceful-shutdown.md

## Run the server (dev)

The repo ships an installable operator CLI named `dispatch`.

Install (editable):

- python -m pip install -e .

Then you can run from any directory.

Canonical local runtime dir:

- /home/rohankhanna/.local/share/scheduler-orchestration/runtime

Managed server mode (normal operator mode):

- dispatch --help
- dispatch server start --runtime-dir /home/rohankhanna/.local/share/scheduler-orchestration/runtime
- dispatch server status --runtime-dir /home/rohankhanna/.local/share/scheduler-orchestration/runtime

Then:

- UI: http://127.0.0.1:8780/ui
- Logs: dispatch server logs -f --runtime-dir /home/rohankhanna/.local/share/scheduler-orchestration/runtime
- Stop: dispatch server stop --runtime-dir /home/rohankhanna/.local/share/scheduler-orchestration/runtime

Foreground mode is development-only. It intentionally does not write pid/log files, so `dispatch server status` and `dispatch server stop` cannot manage a foreground launch. Use managed server mode for anything expected to survive operator handoff or machine restart/restore.
