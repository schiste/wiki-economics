#!/usr/bin/env python3
"""LCOV gate for the wiki-economics test suite.

Hard-fails on any uncovered `DA:` (line) records. Branch records (`BRDA:`)
are parsed and reported but enforcement is opt-in: `cargo-llvm-cov` only
emits branch records when run with the unstable `--branch` flag on a
nightly toolchain. The repo pins `rust-toolchain.toml` to stable, so the
default LCOV output contains line records only and the branch summary is
informational. If a contributor runs with `--branch` on nightly, set
`--require-branches` to make any uncovered branch fail the gate.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class CoverageReport:
    total_lines: int = 0
    uncovered_lines: list[tuple[str, int]] = field(default_factory=list)
    total_branches: int = 0
    uncovered_branches: list[tuple[str, int, str, str]] = field(default_factory=list)


def parse_lcov(lcov_path: Path) -> CoverageReport:
    report = CoverageReport()
    current_file: str | None = None

    for raw_line in lcov_path.read_text().splitlines():
        if raw_line.startswith("SF:"):
            current_file = raw_line[3:]
            continue
        if current_file is None:
            continue
        if raw_line.startswith("DA:"):
            line_no_text, count_text, *_ = raw_line[3:].split(",")
            report.total_lines += 1
            if int(count_text) == 0:
                report.uncovered_lines.append((current_file, int(line_no_text)))
            continue
        if raw_line.startswith("BRDA:"):
            # Format: BRDA:<line>,<block>,<branch>,<taken>
            parts = raw_line[5:].split(",")
            if len(parts) < 4:
                continue
            line_no_text, block, branch, taken = parts[0], parts[1], parts[2], parts[3]
            report.total_branches += 1
            # `taken` is "-" when the branch was never executed and an integer
            # otherwise. "0" is also uncovered (executed but never taken on
            # this side of the branch).
            if taken in {"-", "0"}:
                report.uncovered_branches.append(
                    (current_file, int(line_no_text), block, branch)
                )

    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lcov_path", type=Path, help="Path to lcov.info")
    parser.add_argument(
        "--require-branches",
        action="store_true",
        help=(
            "Fail when any branch record is uncovered. Off by default because "
            "`cargo-llvm-cov` only emits branches with the nightly --branch flag; "
            "stable runs produce zero branch records and would always pass."
        ),
    )
    args = parser.parse_args(argv)

    report = parse_lcov(args.lcov_path)

    if report.uncovered_lines:
        print("lcov uncovered lines detected:", file=sys.stderr)
        for path, line_no in report.uncovered_lines[:50]:
            print(f"  {path}:{line_no}", file=sys.stderr)
        if len(report.uncovered_lines) > 50:
            print(
                f"  ... and {len(report.uncovered_lines) - 50} more",
                file=sys.stderr,
            )
        return 1

    print(
        f"lcov line check passed: {report.total_lines} covered lines, 0 uncovered lines"
    )

    if report.total_branches == 0:
        print(
            "lcov branch check: no BRDA records found "
            "(emit with `cargo +nightly llvm-cov --branch ...` to enforce)"
        )
    else:
        covered = report.total_branches - len(report.uncovered_branches)
        print(
            f"lcov branch check: {covered}/{report.total_branches} branches covered, "
            f"{len(report.uncovered_branches)} uncovered"
        )
        if report.uncovered_branches:
            for path, line_no, block, branch in report.uncovered_branches[:50]:
                print(
                    f"  {path}:{line_no} block={block} branch={branch}",
                    file=sys.stderr,
                )
            if len(report.uncovered_branches) > 50:
                print(
                    f"  ... and {len(report.uncovered_branches) - 50} more",
                    file=sys.stderr,
                )
            if args.require_branches:
                print("lcov branch check failed: --require-branches set", file=sys.stderr)
                return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
