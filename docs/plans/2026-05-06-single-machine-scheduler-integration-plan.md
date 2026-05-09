# Single-Machine Scheduler Integration Plan For Long-Horizon Job Graphs

Goal: replace ad hoc background chaining with a durable, always-on scheduler path that supports queued waiting, resource-aware admission, dependency-aware execution, and graceful stop-at-boundary controls for long-horizon workflow jobs.

Architecture: use Slurm as the centralized scheduler server on one machine, keep workflow scripts as payload executors, and add a thin job submission and status layer inside this repository. The first increment focuses on one backend, one job specification schema, and one workflow family. Do not build a custom scheduler core in iteration one.

Tech stack (iteration 1): Slurm (slurmctld, slurmd, slurmrestd), a small always-on local server exposing an API, language client libraries, repository-local schemas, a thin adapter module, tests for the contract + server API + adapter behavior, and operator documentation.

---

## Phase 0: Safety, boundaries, and prerequisites

### Task 1: Confirm source-of-truth boundaries before host-internal mutation

Objective: ensure operating system changes are planned through the canonical dotfiles repository before any live machine scheduler installation.

Files:
- Create: docs/architecture/slurm-integration-boundary.md

Constraints:
- This repository must not perform live host changes under /etc or systemd.
- Any host-internal Slurm installation or service changes must be authored in /home/rohankhanna/Desktop/dotfiles first.

Verification:
- Boundary document explicitly states no live host changes from this repository.

### Task 2: Freeze first-scope requirements and non-goals

Objective: prevent scope drift and keep iteration one narrow.

Files:
- Create: docs/architecture/slurm-integration-scope.md

Required first-scope requirements:
- Pending queue with wait-until-available behavior
- Resource bundle declarations for graphics processing unit, central processing unit cores, and memory
- Dependency chaining through directed acyclic graph semantics
- Graceful drain mode for stop-at-boundary behavior
- Durable state across reboot through scheduler service restart behavior

Explicit non-goals in iteration one:
- Multi-node cluster rollout
- Multi-backend abstraction layer
- Automatic optimizer for global throughput

Verification:
- Scope file includes success criteria and non-goals.

---

## Phase 1: Job contract and adapter surface

### Task 3: Define scheduler job specification schema

Objective: represent workflow jobs and dependencies in a stable repository-local contract.

Planned files:
- schemas/scheduler_job_spec.schema.json
- schemas/examples/scheduler_job_spec.example.json
- tests/test_scheduler_job_spec_schema.py

Schema fields (initial):
- job_name
- workflow_preset
- device_preference
- resources with graphics_processing_units, central_processing_unit_cores, memory_gibibytes
- dependencies as parent job identifiers and dependency policy
- environment_overrides
- artifacts_root
- retry_policy
- drain_behavior

---

## Phase 2: Workflow integration

### Task 4+: Build adapter, server API, and payload wrappers

Implementation should proceed in small, test-backed increments, committing after each task.
