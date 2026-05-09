# Single-Machine Scheduler Integration Plan For Embedding Workflow Jobs

> For Hermes: Use subagent-driven-development skill to implement this plan task-by-task.

Goal: replace ad hoc background chaining with a durable, always-on scheduler path that supports queued waiting, resource-aware admission, dependency-aware execution, and graceful stop-at-boundary controls for embedding workflow jobs.

Architecture: use Slurm as the centralized scheduler server on one machine, keep current workflow scripts as payload executors, and add a thin job submission and status layer inside this repository. The first increment focuses on one backend, one job specification schema, and one workflow family. Do not build a custom scheduler core in iteration one.

Tech Stack: Slurm (slurmctld, slurmd, slurmrestd), Python command-line integration in bespoke_cli, JSON schemas, pytest, and repository documentation plus architecture decision records.

---

## Phase 0: Safety, boundaries, and prerequisites

### Task 1: Confirm source-of-truth boundaries before host-internal mutation
Objective: ensure operating system changes are planned through the canonical dotfiles repository before any live machine scheduler installation.

Files:
- Create: `docs/plans/2026-05-06-single-machine-scheduler-integration-plan.md` (this file)
- Create: `docs/architecture/slurm-integration-boundary.md`
- Reference: `/home/rohankhanna/Desktop/dotfiles`

Step 1: Document that live Slurm installation is blocked until dotfiles source-of-truth changes are prepared.
Step 2: Document rollback expectations and verification checkpoints before host mutation.
Step 3: Commit documentation-only boundary statement.

Verification:
- `pytest -q` remains green.
- Boundary document explicitly states no live host changes from this repository.

### Task 2: Freeze first-scope requirements and non-goals
Objective: prevent scope drift and keep iteration one narrow.

Files:
- Create: `docs/architecture/slurm-integration-scope.md`
- Update: `ARCHITECTURE.md`

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

## Phase 1: Job contract and adapter surface

### Task 3: Define scheduler job specification schema
Objective: represent embedding jobs and dependencies in a stable repository-local contract.

Files:
- Create: `schemas/scheduler_job_spec.schema.json`
- Create: `schemas/examples/scheduler_job_spec.example.json`
- Create: `tests/test_scheduler_job_spec_schema.py`

Schema fields:
- `job_name`
- `workflow_preset`
- `device_preference`
- `resources` with `graphics_processing_units`, `central_processing_unit_cores`, `memory_gibibytes`
- `dependencies` as parent job identifiers and dependency policy
- `environment_overrides`
- `artifacts_root`
- `retry_policy`
- `drain_behavior`

Step 1: Write failing schema test for required fields.
Step 2: Implement schema.
Step 3: Re-run tests.

Verification:
- `pytest -q tests/test_scheduler_job_spec_schema.py`

### Task 4: Add scheduler adapter module with dry-run mode
Objective: centralize all scheduler interactions in one Python module.

Files:
- Create: `src/bespoke_cli/scheduler_adapter.py`
- Create: `tests/test_scheduler_adapter.py`

Adapter functions:
- `submit_job(spec)`
- `query_job(job_identifier)`
- `cancel_job(job_identifier)`
- `set_drain_mode(enabled)`
- `list_queue(filter_by_workflow=None)`

Step 1: Write failing tests with command mocking.
Step 2: Implement command construction for Slurm command-line calls and optional REST endpoint.
Step 3: Add structured parse of returned identifiers and states.

Verification:
- `pytest -q tests/test_scheduler_adapter.py`

## Phase 2: Embedding workflow integration

### Task 5: Add scheduler submission command-line entrypoint
Objective: allow current operators to submit embedding jobs through scheduler contracts instead of direct script invocation.

Files:
- Create: `src/bespoke_cli/scheduler_cli.py`
- Update: `src/bespoke_cli/__main__.py`
- Create: `tests/test_scheduler_cli.py`

Initial commands:
- `bespoke-cli scheduler submit --spec <path>`
- `bespoke-cli scheduler status --job <identifier>`
- `bespoke-cli scheduler queue`
- `bespoke-cli scheduler drain --enable|--disable`

Verification:
- `pytest -q tests/test_scheduler_cli.py`

### Task 6: Wrap existing embedding workflow script payloads
Objective: preserve existing workflow scripts while moving admission control to scheduler submission.

Files:
- Create: `scripts/slurm/embedding_workflow_job_wrapper.sh`
- Update: `scripts/run_embedding_workflow_job.sh` (if needed for wrapper compatibility)
- Create: `tests/test_scheduler_payload_wrapper.py`

Requirements:
- Pass through existing retry environment values
- Preserve artifact output paths
- Emit machine-readable completion summary with job identifier

Verification:
- `pytest -q tests/test_scheduler_payload_wrapper.py`

### Task 7: Add dependency-aware chaining helper
Objective: replace manual chaining with explicit dependency submission.

Files:
- Create: `src/bespoke_cli/scheduler_chain.py`
- Create: `tests/test_scheduler_chain.py`

Behavior:
- Submit one job
- Capture returned identifier
- Submit next job with dependency field pointing to predecessor
- Support bounded chain length for safety

Verification:
- `pytest -q tests/test_scheduler_chain.py`

## Phase 3: Observability, recovery, and operator controls

### Task 8: Add durable run ledger for scheduler-submitted jobs
Objective: keep recoverable local state independent of terminal sessions.

Files:
- Create: `runtime/scheduler-job-ledger/README.md`
- Create: `src/bespoke_cli/scheduler_ledger.py`
- Create: `tests/test_scheduler_ledger.py`

Ledger fields:
- scheduler job identifier
- submitted specification hash
- submission timestamp
- state transitions
- artifact paths
- terminal exit signal

Verification:
- `pytest -q tests/test_scheduler_ledger.py`

### Task 9: Add graceful stop-at-boundary operator command
Objective: allow draining queue without killing currently running jobs.

Files:
- Update: `src/bespoke_cli/scheduler_cli.py`
- Update: `tests/test_scheduler_cli.py`

Behavior:
- Enable drain mode
- Confirm no new jobs are admitted
- Running jobs continue
- Pending jobs remain pending until drain is disabled or explicitly canceled

Verification:
- `pytest -q tests/test_scheduler_cli.py -k drain`

### Task 10: Add health checks and failure triage command
Objective: make scheduler state debuggable from this repository.

Files:
- Create: `src/bespoke_cli/scheduler_health.py`
- Create: `tests/test_scheduler_health.py`

Health checks:
- scheduler daemon reachable
- controller responsive
- queue readable
- recent failed job sample retrieval

Verification:
- `pytest -q tests/test_scheduler_health.py`

## Phase 4: Documentation and architecture synchronization

### Task 11: Update architecture source-of-truth and diagrams
Objective: keep architecture documentation current with scheduler integration.

Files:
- Update: `ARCHITECTURE.md`
- Update: `docs/architecture/embedding-workflow-job.md`
- Create: `docs/architecture/scheduler-integration-flow.md`
- Create or update: diagram under `docs/architecture/`

Verification:
- `pytest -q`
- Manual check that architecture docs describe queue admission, dependencies, and drain semantics.

### Task 12: Record architecture decision and operational runbook
Objective: preserve rationale and operator procedures.

Files:
- Create: `docs/adr/0007-single-machine-slurm-admission-and-dependency-orchestration.md`
- Create: `docs/runbooks/scheduler-operations.md`
- Update: `README.md` with brief operator-facing pointer

Runbook minimums:
- submit, status, queue, drain, resume
- reboot recovery expectations
- failure triage checklist
- rollback path to direct script invocation

Verification:
- `pytest -q`
- Runbook commands validated in a disposable local test path.

## Phase 5: Controlled rollout and acceptance gates

### Task 13: Shadow mode rollout
Objective: prove parity with existing direct workflow execution before making scheduler path the default.

Files:
- Create: `docs/plans/2026-05-06-scheduler-shadow-rollout-checklist.md`
- Create: `tests/integration/test_scheduler_shadow_mode.py`

Shadow mode behavior:
- submit scheduled jobs while still keeping direct script path available
- compare artifact completeness and key metrics
- verify no regression in recommended default model outputs

Verification:
- `pytest -q tests/integration/test_scheduler_shadow_mode.py`

### Task 14: Default flip with rollback gate
Objective: switch default submission path only after objective acceptance criteria pass.

Files:
- Update: `src/bespoke_cli/config.py` (or existing configuration surface)
- Update: `docs/runbooks/scheduler-operations.md`
- Update: `tests/test_config_defaults.py`

Acceptance criteria:
- at least ten consecutive successful scheduled deep workflow runs
- no missing artifacts versus baseline
- drain and resume validated
- recovery after reboot validated

Verification:
- `pytest -q`
- documented acceptance report under `feedback/decisions/`.

---

## Implementation order and commit cadence

1. Complete Phase 0 and commit documentation boundaries.
2. Complete Phase 1 contracts and adapter tests.
3. Complete Phase 2 integration commands and wrappers.
4. Complete Phase 3 observability and controls.
5. Complete Phase 4 documentation and decision records.
6. Complete Phase 5 shadow rollout and default flip.

Commit after each task with small, test-backed changes.

## Canonical verification path per task

- Primary: `pytest -q`
- Task-local tests as specified above
- Do not skip failing-test-first sequence for new modules.

## Stop conditions

Stop and reassess if any of the following occurs:
- host-internal changes are requested from this repository without dotfiles source-of-truth updates
- scheduler behavior cannot preserve state safely across reboot
- queue admission cannot enforce resource bundles deterministically
- architecture documentation falls behind implementation changes
