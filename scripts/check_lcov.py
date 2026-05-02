#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_lcov.py <path/to/lcov.info>", file=sys.stderr)
        return 2

    lcov_path = Path(sys.argv[1])
    uncovered: list[tuple[str, int]] = []
    current_file: str | None = None
    total_lines = 0

    for raw_line in lcov_path.read_text().splitlines():
        if raw_line.startswith("SF:"):
            current_file = raw_line[3:]
            continue
        if raw_line.startswith("DA:") and current_file is not None:
            line_no_text, count_text, *_ = raw_line[3:].split(",")
            total_lines += 1
            if int(count_text) == 0:
                uncovered.append((current_file, int(line_no_text)))

    if uncovered:
        print("lcov uncovered lines detected:", file=sys.stderr)
        for path, line_no in uncovered[:50]:
            print(f"  {path}:{line_no}", file=sys.stderr)
        if len(uncovered) > 50:
            print(f"  ... and {len(uncovered) - 50} more", file=sys.stderr)
        return 1

    print(f"lcov line check passed: {total_lines} covered lines, 0 uncovered lines")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
