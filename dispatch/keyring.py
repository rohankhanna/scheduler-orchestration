from __future__ import annotations

import argparse
import os
import sys
from datetime import timedelta
from pathlib import Path

from dispatch.api_keyring import mint_api_key


def _runtime_dir() -> Path:
    return Path(os.environ.get("SCHED_ORCH_RUNTIME_DIR", "runtime"))


def _parse_ttl(raw: str) -> int:
    s = raw.strip().lower()
    if not s:
        raise ValueError("ttl is required")

    # Accept either integer seconds or a simple <int><unit> form.
    # Units: s, m, h, d
    if s.isdigit():
        return int(s)

    n_part = ""
    unit_part = ""
    for ch in s:
        if ch.isdigit() and not unit_part:
            n_part += ch
        else:
            unit_part += ch

    if not n_part or not unit_part:
        raise ValueError("invalid ttl")

    n = int(n_part)
    unit = unit_part

    if unit == "s":
        return n
    if unit == "m":
        return int(timedelta(minutes=n).total_seconds())
    if unit == "h":
        return int(timedelta(hours=n).total_seconds())
    if unit == "d":
        return int(timedelta(days=n).total_seconds())

    raise ValueError("invalid ttl unit")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m dispatch.keyring")
    sub = p.add_subparsers(dest="cmd", required=True)

    mint = sub.add_parser("mint", help="Mint a new API key into the local keyring")
    mint.add_argument("--ttl", required=True, help="TTL: integer seconds or <n>[s|m|h|d] (e.g. 30d)")
    mint.add_argument("--label", default="", help="Human label (optional)")
    mint.add_argument("--keyring-path", default="", help="Override keyring path")

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.cmd == "mint":
        ttl_seconds = _parse_ttl(args.ttl)
        keyring_path = Path(args.keyring_path) if args.keyring_path else None
        api_key = mint_api_key(
            label=args.label or None,
            ttl_seconds=ttl_seconds,
            runtime_dir=_runtime_dir(),
            keyring_path=keyring_path,
        )
        # Print only the key as the last line (easy to capture in scripts).
        sys.stdout.write(f"{api_key}\n")
        return 0

    raise RuntimeError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
