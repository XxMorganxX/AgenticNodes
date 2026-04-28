from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from graph_agent.runtime.python_script_runner import (
    DEFAULT_SCRIPT_TIMEOUT_SECONDS,
    PYTHON_SCRIPT_RUNNER_MODE,
    PYTHON_SCRIPT_RUNNER_PROVIDER_ID,
    run_script,
)


class PythonScriptRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_path = Path(self._tmp.name)

    def _write(self, name: str, body: str) -> str:
        path = self.tmp_path / name
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        return str(path)

    def test_run_true_is_success(self) -> None:
        path = self._write("ok.py", """
            def run():
                return True
        """)
        result = run_script(path)
        self.assertTrue(result.success)
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(result.timed_out)
        self.assertIsNone(result.error)

    def test_run_false_is_failure(self) -> None:
        path = self._write("false.py", """
            def run():
                return False
        """)
        result = run_script(path)
        self.assertFalse(result.success)
        self.assertEqual(result.exit_code, 1)
        self.assertIsNotNone(result.error)
        self.assertEqual(result.error["type"], "python_script_run_returned_false")

    def test_run_raises_is_failure_with_traceback(self) -> None:
        path = self._write("raises.py", """
            def run():
                raise RuntimeError("boom")
        """)
        result = run_script(path)
        self.assertFalse(result.success)
        self.assertEqual(result.exit_code, 2)
        self.assertIn("RuntimeError", result.stderr)
        self.assertIn("boom", result.stderr)
        self.assertEqual(result.error["type"], "python_script_unhandled_exception")

    def test_missing_run_symbol_is_failure(self) -> None:
        path = self._write("no_run.py", """
            def other():
                return True
        """)
        result = run_script(path)
        self.assertFalse(result.success)
        self.assertEqual(result.exit_code, 3)
        self.assertEqual(result.error["type"], "python_script_missing_run_symbol")

    def test_import_error_is_bootstrap_failure(self) -> None:
        path = self._write("broken.py", """
            import a_module_that_does_not_exist_xyz

            def run():
                return True
        """)
        result = run_script(path)
        self.assertFalse(result.success)
        # Import error is caught as an unhandled exception during module load.
        self.assertEqual(result.exit_code, 2)
        self.assertIn("ModuleNotFoundError", result.stderr)

    def test_timeout_kills_long_script(self) -> None:
        path = self._write("slow.py", """
            import time

            def run():
                time.sleep(10)
                return True
        """)
        result = run_script(path, timeout_seconds=1)
        self.assertFalse(result.success)
        self.assertTrue(result.timed_out)
        self.assertIsNotNone(result.error)
        self.assertEqual(result.error["type"], "python_script_timeout")

    def test_stdin_payload_round_trip(self) -> None:
        path = self._write("echo.py", """
            import json
            import sys

            def run():
                payload = json.load(sys.stdin)
                print(json.dumps(payload))
                return payload.get("ok", False)
        """)
        result = run_script(path, payload_json='{"ok": true, "emails": ["a@b.com"]}')
        self.assertTrue(result.success)
        self.assertIn('"emails"', result.stdout)
        self.assertIn("a@b.com", result.stdout)

    def test_missing_script_path(self) -> None:
        result = run_script("")
        self.assertFalse(result.success)
        self.assertEqual(result.error["type"], "python_script_missing_path")

    def test_nonexistent_script_path(self) -> None:
        result = run_script(str(self.tmp_path / "nope.py"))
        self.assertFalse(result.success)
        self.assertEqual(result.error["type"], "python_script_not_found")

    def test_constants_match_expected_values(self) -> None:
        self.assertEqual(PYTHON_SCRIPT_RUNNER_PROVIDER_ID, "core.python_script_runner")
        self.assertEqual(PYTHON_SCRIPT_RUNNER_MODE, "python_script_runner")
        self.assertEqual(DEFAULT_SCRIPT_TIMEOUT_SECONDS, 30)


if __name__ == "__main__":
    unittest.main()
