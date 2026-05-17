# Dispatch + local Slurm GPU (GRES) operator note

CPU-only Slurm works with the baseline single-node setup.
GPU scheduling does not work until Slurm is configured to advertise GPU GRES resources.

This doc captures the minimum host-level Slurm configuration and a Dispatch end-to-end proof path.

## Host-level requirement (dotfiles-owned)

Dispatch uses `sbatch --gpus=<N>` when `resources.graphics_processing_units > 0`.
If Slurm does not advertise GPU GRES, GPU requests must fail closed (remain pending or be rejected).

A minimal single-node GPU setup requires:

1) In slurm.conf:

- `GresTypes=gpu`
- Node definition includes a GPU count, e.g. `Gres=gpu:1`

2) A matching gres.conf mapping, e.g.:

- `Name=gpu File=/dev/nvidia0`

Keep `Gres=gpu:<count>` and gres.conf in sync.

Note: Dispatch does not require Slurm accounting for terminal state; local installs may keep
`AccountingStorageType=none`.

## Verification (Slurm advertises GPU)

1) Verify partition shows GPU GRES:

- `sinfo -o '%P %G %t %N'`

Expected shape:
- `local* gpu:1 idle <node>`

2) Verify node shows non-null Gres:

- `scontrol show node <node> | grep -i Gres`

Expected:
- `Gres=gpu:1`

3) Verify a job that explicitly requests a GPU can see it:

- `srun --gpus=1 --pty /usr/bin/env bash -lc 'nvidia-smi -L'

If GRES is not configured, this should fail closed (pending/error), not succeed without a GPU.

## Dispatch end-to-end GPU proof

Submit a job with `resources.graphics_processing_units=1` and a payload that prints GPU evidence:

Payload suggestion:

- `/usr/bin/env bash -lc 'nvidia-smi -L; python3 -c "import torch; print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else None)"'`

Evidence to capture:

1) `/v1/jobs/{id}?refresh=1` includes:
- `execution_backend = "slurm"`
- `scheduler_job_id` is non-null
- job reaches terminal `state = "succeeded"`

2) `/v1/jobs/{id}/logs` stdout includes:
- `nvidia-smi -L` output
- `True` for `torch.cuda.is_available()`

Operator hint
- `dispatch doctor --base-url http://127.0.0.1:8780 --runtime-dir <RUNTIME_DIR>` prints `active_execution_backend`.
- If `active_execution_backend=slurm` and Slurm is not advertising GPU GRES, doctor prints a WARN.
