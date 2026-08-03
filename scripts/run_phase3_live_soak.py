"""Run an explicit continuous public-feed Phase 3 soak (six hours by default)."""

from __future__ import annotations

import sys

from cvf.main import main


def _has_option(arguments: list[str], name: str) -> bool:
    return name in arguments or any(
        argument.startswith(f"{name}=") for argument in arguments
    )


if __name__ == "__main__":
    forwarded = sys.argv[1:]
    missing = [
        option
        for option in ("--output", "--feature-output")
        if not _has_option(forwarded, option)
    ]
    if missing:
        print(
            "Phase 3 live soak requires explicit, disjoint "
            f"{' and '.join(missing)} paths.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if not _has_option(forwarded, "--duration"):
        forwarded = ["--duration", "21600", *forwarded]
    raise SystemExit(main(["collect", *forwarded]))
