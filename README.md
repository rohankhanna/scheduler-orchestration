# scheduler-orchestration

A small, local-first orchestration repository for running long-horizon, resource-aware, dependency-aware job graphs on a single machine.

Initial intended use:
- act as the durable scheduler layer for embedding and clustering workflows that need queued waiting, multi-resource admission, dependency chaining, and drain controls.

This repository should prefer integrating mature schedulers (for example Slurm) over building a new scheduler core.

Interface policy:
- The primary user surface is an always-on local server with a stable API.
- Language client libraries call that API.
- A command-line interface is optional and must be a thin wrapper around the API client, intended only for manual debugging.
