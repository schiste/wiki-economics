"""Tests for scripts/check_lcov.py."""

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from tempfile import TemporaryDirectory

from check_lcov import main as check_lcov_main, parse_lcov


def _write_lcov(tmpdir: Path, body: str) -> Path:
    path = tmpdir / "lcov.info"
    path.write_text(body)
    return path


class CheckLcovTests(unittest.TestCase):
    def test_passes_when_every_line_is_covered(self) -> None:
        with TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            lcov = _write_lcov(
                tmp,
                "SF:src/a.rs\nDA:1,3\nDA:2,1\nend_of_record\n",
            )
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = check_lcov_main([str(lcov)])
            self.assertEqual(exit_code, 0)
            self.assertIn("2 covered lines", stdout.getvalue())
            self.assertIn("no BRDA records", stdout.getvalue())

    def test_fails_on_any_uncovered_line(self) -> None:
        with TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            lcov = _write_lcov(
                tmp,
                "SF:src/a.rs\nDA:1,3\nDA:7,0\nend_of_record\n",
            )
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = check_lcov_main([str(lcov)])
            self.assertEqual(exit_code, 1)
            self.assertIn("src/a.rs:7", stderr.getvalue())

    def test_branch_summary_warns_when_branches_present_but_not_required(self) -> None:
        with TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            lcov = _write_lcov(
                tmp,
                (
                    "SF:src/a.rs\n"
                    "DA:1,3\n"
                    "BRDA:1,0,0,3\n"
                    "BRDA:1,0,1,-\n"
                    "BRDA:2,0,0,5\n"
                    "BRDA:2,0,1,0\n"
                    "end_of_record\n"
                ),
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = check_lcov_main([str(lcov)])
            # No --require-branches so uncovered branches are reported but do
            # not fail the gate.
            self.assertEqual(exit_code, 0)
            self.assertIn("2/4 branches covered", stdout.getvalue())
            self.assertIn("src/a.rs:1", stderr.getvalue())
            self.assertIn("src/a.rs:2", stderr.getvalue())

    def test_branch_failure_when_required(self) -> None:
        with TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            lcov = _write_lcov(
                tmp,
                (
                    "SF:src/a.rs\n"
                    "DA:1,3\n"
                    "BRDA:1,0,0,3\n"
                    "BRDA:1,0,1,-\n"
                    "end_of_record\n"
                ),
            )
            exit_code = check_lcov_main([str(lcov), "--require-branches"])
            self.assertEqual(exit_code, 1)

    def test_parser_extracts_records(self) -> None:
        with TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            lcov = _write_lcov(
                tmp,
                (
                    "SF:src/a.rs\n"
                    "DA:1,1\n"
                    "DA:2,0\n"
                    "BRDA:5,0,0,1\n"
                    "BRDA:5,0,1,0\n"
                    "end_of_record\n"
                    "SF:src/b.rs\n"
                    "DA:1,1\n"
                    "end_of_record\n"
                ),
            )
            report = parse_lcov(lcov)
            self.assertEqual(report.total_lines, 3)
            self.assertEqual(report.uncovered_lines, [("src/a.rs", 2)])
            self.assertEqual(report.total_branches, 2)
            self.assertEqual(len(report.uncovered_branches), 1)


if __name__ == "__main__":
    unittest.main()
