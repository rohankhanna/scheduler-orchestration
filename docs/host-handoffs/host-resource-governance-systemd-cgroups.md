# Host handoff: workload-only resource governance with systemd + cgroups v2 (option C)

Status
- This document is a host-configuration handoff.
- It is NOT applied automatically by this repository.
- Apply explicitly on the host under /etc/systemd/system when ready.

Goal
- Keep the machine usable at all times.
- Enforce resource governance ONLY for workloads launched by the orchestration server.
- Keep LLM command line interfaces (and other interactive processes) outside workload governance.

Host facts (captured)
- CPUs: 20 (0-19)
- RAM: 121 GiB
- cgroups v2 controllers: cpuset cpu io memory

Policy targets (agreed)
- Reserve 2 CPU cores for interactivity.
  - Workloads AllowedCPUs: 2-19
- Maintain an interactive RAM reserve of approximately 4-8 GiB.
- Use IO weights to reduce disk I/O interference from workloads.

Important limitation (do not skip)
- If the orchestration server submits work to a scheduler backend (example: Slurm), then the long-running workload processes may be created by scheduler daemons, not by the orchestration server process.
- In that case:
  - systemd slice placement for the sbatch client process does NOT enforce limits on the eventual job runtime.
  - you must enable scheduler-side cgroup enforcement for the actual job processes.

Therefore, there are two enforcement modes:

Mode 1 (workloads launched directly by the orchestration server)
- Strong guarantee: systemd cgroup limits apply to the actual workload processes.

Mode 2 (workloads launched via a scheduler backend)
- Strong guarantee requires scheduler cgroup enforcement.
- systemd slice placement still helps for any non-scheduler helper processes, but is not sufficient alone.

Recommended rollout phases (with mitigations)

Phase 1: soft governance only (lowest risk)
- Configure a jobs slice with:
  - CPUWeight lower than default
  - IOWeight lower than default
  - MemoryHigh set
- Do NOT apply AllowedCPUs or MemoryMax yet.

Phase 2: reserve CPUs (hard guarantee for responsiveness)
- Add AllowedCPUs=2-19 to the jobs slice.

Phase 3: add a hard memory ceiling
- Add MemoryMax to the jobs slice.

Phase 4: optional hard I/O throughput caps
- Add io.max only if IOWeight is insufficient.

Mitigation: automatic rollback timer (strongly recommended)
- When testing Phase 2 or Phase 3 changes, apply them and schedule an automatic revert in 2 minutes.
- Cancel the revert only after verifying the desktop remains usable under load.

Escape hatch (mandatory)
- Ensure you can recover without the desktop:
  - root TTY login (Ctrl+Alt+F3), or
  - ssh to localhost as an admin user.

Suggested slice names
- sched-orch-jobs.slice: workload execution envelope
- sched-orch-server.service: orchestration server (keep responsive)

Template files (copy to /etc/systemd/system)
- templates/systemd/sched-orch-jobs.slice
- templates/systemd/sched-orch-server.service.d/override.conf

Suggested initial values (start safe, adjust later)

CPU reservation
- AllowedCPUs=2-19

Memory
- Option A (8 GiB reserve, safer start):
  - MemoryHigh=108G
  - MemoryMax=113G
- Option B (4 GiB reserve, tighter):
  - MemoryHigh=112G
  - MemoryMax=117G

Disk I/O
- IOWeight=100 (default is typically 100; pick a lower value only after verifying your baseline)
- Start with IOWeight=80 for workloads and keep interactive at default.

Verification checklist after applying
- Start the server.
- Launch a synthetic CPU load job through the server.
- Confirm the desktop remains usable (mouse movement, window drag, audio).
- Confirm workload processes are in the intended slice:
  - systemd-cgls
  - systemctl status sched-orch-jobs.slice

