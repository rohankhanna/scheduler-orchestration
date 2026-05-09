# Slurm integration scope (iteration 1)

This document freezes iteration-1 scope for the single-machine scheduler integration.

## Success criteria (iteration 1)

A minimal integration is complete when the repository provides a job contract and an adapter surface such that an operator can:

- submit a job that waits in a pending state until declared resources are available
- declare a resource bundle (graphics processing unit count, central processing unit core count, memory)
- submit a dependency chain using scheduler-native directed acyclic graph semantics
- enable a "drain / stop-at-boundary" mode that prevents new admissions while allowing in-flight jobs to complete
- recover operator state after reboot (scheduler job identifiers and their intended specs are recoverable from durable records)

## In-scope requirements (iteration 1)

- Single machine
- Slurm-first backend
- One job specification schema
- One workflow family as the initial payload target
- Dry-run mode for adapter submission (construct commands without executing)

## Explicit non-goals (iteration 1)

- Multi-node cluster rollout
- Multi-backend abstraction layer
- A custom scheduler core
- An automatic global throughput optimizer
- A full web service for the orchestration layer

## Notes

- Integrate mature schedulers rather than rebuilding core scheduling primitives.
- Keep iteration-1 surfaces thin and testable.
