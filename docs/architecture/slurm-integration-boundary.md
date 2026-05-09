# Slurm integration boundary (no host mutation from this repository)

This repository defines contracts, adapters, and operator documentation for submitting and controlling jobs through an off-the-shelf scheduler.

It does not perform live host-internal mutation.

## Hard boundary

- Do not install Slurm from this repository.
- Do not change /etc, systemd units, daemon configuration, firewall rules, or other host internals from this repository.
- Any host-internal Slurm installation, configuration, or service enablement must be authored in the canonical dotfiles/system-state repository first:
  - (your system-state repository)

## What this repository may do

- Define a scheduler job specification (schema + examples).
- Implement a thin client adapter that submits and queries jobs through Slurm CLI tools or slurmrestd.
- Provide a command-line interface for operators.
- Provide tests for contract validity and adapter command construction.
- Provide runbooks for safe operation and recovery.

## Verification checkpoints before any live host work

When this repo eventually needs a live Slurm environment, the prerequisite work must land in dotfiles first, with:

- version-controlled source
- an apply path
- a verify path
- rollback guidance
