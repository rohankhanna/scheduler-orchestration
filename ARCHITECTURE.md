# Architecture

This repository provides a thin orchestration integration layer for running long-horizon job graphs on a single machine.

It is explicitly not a scheduler core.

## Components

- Job contract: a repository-local job specification schema used to describe work, resources, dependencies, and operator controls.
- Scheduler adapter: a thin integration that submits and observes jobs through an off-the-shelf scheduler (iteration 1: Slurm).
- Orchestration server: an always-on local service that exposes a stable API for job submission, status, queue inspection, cancel, and drain / resume.
- Language client libraries: small libraries (per language) that call the server API.
- Optional operator command-line interface: a thin wrapper around the client library for manual debugging only.
- Durable run ledger: a local record of submitted jobs that supports recovery after reboot.

## Key constraints

- Prefer integrating mature schedulers (for example Slurm).
- No host-internal mutation from this repository; host changes must be authored in (your system-state repository).

## Source-of-truth documents

- docs/architecture/slurm-integration-boundary.md
- docs/architecture/slurm-integration-scope.md
- docs/architecture/architecture.html
