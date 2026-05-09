# Draft: host resource governance (option C, no VMs)

Status: draft notes captured for revisit after reboot. Not applied to the host yet.

Goal
- Use this machine as a job scheduling system.
- Ensure no single application/thread/program can hog machine resources indefinitely.
- Ensure the machine stays usable at all times.
- Prefer N-1 CPU cores for workloads, with 1 core effectively reserved for interactivity.
- Also enforce memory caps and disk I/O weights/limits to prevent swap/I/O induced UI freezes.

Key clarification: scheduler backend vs orchestration server
- “Orchestration server” (this repo): always-on local API that admits work, launches/controls jobs, and enforces policies at the API boundary.
- “Scheduler backend” (example: Slurm): separate system that enforces resource allocation and scheduling policy.
- These are not the same thing.

Option C (whole-machine policy) in concrete Linux terms
- Use systemd + cgroups v2 slices/scopes to enforce resource policy by default.
- Ensure workload processes (jobs) run inside a constrained slice that:
  - cannot use at least one reserved CPU
  - has memory governance to prevent global reclaim/swap storms
  - has disk I/O governance so it cannot freeze interactivity
- Keep interactive/system processes outside that constrained slice.

CPU: reserve 1 core for interactivity
- Machine has 20 CPU cores.
- Proposed static choice: reserve CPU 0.
  - Workloads (jobs) AllowedCPUs: 1-19
  - Interactive/system AllowedCPUs: 0-19

Important: “reserve CPU 0” does NOT mean interactivity is pinned to CPU 0.
- It means job workloads are not allowed to run on CPU 0, so the kernel scheduler always has at least one CPU that is never stolen by job workloads.
- Interactive/system work can still run on any CPU.

Memory: prevent swap storms and desktop lockups
- CPU reservation alone is not sufficient.
- Add memory governance to workload slice:
  - MemoryHigh: soft pressure threshold (begin reclaim before the system becomes unusable)
  - MemoryMax: hard cap (prevents total RAM exhaustion and swap death)
- Optionally protect interactive slice with MemoryMin (later).

Disk I/O: prevent I/O induced UI freezes
- Workload slice should have lower IOWeight than interactive/system.
- Optionally add hard throughput caps for known heavy writers (later).

Fairness / anti-hog policy (10 projects, equal priority)
There are two parts:
1) Enforcement (cgroups)
- Prevents any one workload from taking resources outside its envelope.

2) Scheduling / admission / time limits
- Prevents “run forever” behavior and enforces equal share.
- Can be implemented by:
  - a scheduler backend (example: Slurm), OR
  - the orchestration server itself (queueing + fair admission + timeouts), OR
  - a hybrid.

For “no job runs indefinitely”, enforce:
- mandatory walltime per job
- cancellation/requeue on walltime breach
- per-project concurrency caps (for example: max running jobs, max CPU cores, max GPUs)

Mitigations: avoid locking yourself out with strict policy
- Main risk: misconfiguration that makes it hard to recover.

Mitigation techniques:
1) Roll out in phases (start soft, then hard)
- Phase 1: weights + soft limits only
  - CPUWeight (lower for workloads, higher for interactive)
  - IOWeight (lower for workloads, higher for interactive)
  - MemoryHigh for workloads
- Phase 2: add hard CPU reservation (AllowedCPUs / cpuset)
- Phase 3: add MemoryMax hard caps
- Phase 4: optional hard I/O caps (io.max)

2) Keep an escape hatch that does not depend on GUI
- root TTY login (Ctrl+Alt+F3) that is not placed into the restricted slice, OR
- ssh to localhost with an admin account not subject to restrictions, OR
- a systemd maintenance target you can switch to from TTY.

3) Automatic rollback timer during rollout
- When trialing a new restrictive setting:
  - apply policy
  - schedule automatic revert after ~2 minutes
  - cancel revert only after confirming system remains usable under load

Baseline recommendation (host-only, no VM)
- Implement option C (host-level slices/cgroups) with:
  - N-1 CPUs for workloads (reserve CPU 0)
  - workload memory governance (MemoryHigh, then MemoryMax)
  - workload disk I/O governance (IOWeight, optionally io.max)
- Ensure the orchestration server places every launched job into the workload slice/scope.
- If a scheduler backend (example: Slurm) is used later, keep it as the scheduling engine; the server remains the policy/admission API.

Open items for tomorrow
- Determine total RAM to choose initial MemoryHigh/MemoryMax values.
  - command: free -h
- Decide initial IOWeight values and whether hard io.max caps are needed.
- Decide whether to keep “scheduler exec” gated behind an explicit enable flag (already present) and how that relates to host slices.
- Draft concrete systemd unit/slice examples (as host-configuration handoff, not applied automatically by this repo).
