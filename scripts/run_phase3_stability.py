"""Run the packaged Phase 3 stability command from a source checkout."""

from __future__ import annotations

import sys

from cvf.main import main

if __name__ == "__main__":
    raise SystemExit(main(["stability-phase3", *sys.argv[1:]]))
