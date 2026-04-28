from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import selectors
import subprocess
import sys
import time
from typing import Any


PYTHON_SCRIPT_RUNNER_PROVIDER_ID = "core.python_script_runner"
PYTHON_SCRIPT_RUNNER_MODE = "python_script_runner"
DEFAULT_SCRIPT_TIMEOUT_SECONDS = 30
MAX_CAPTURED_STREAM_BYTES = 256 * 1024


BOOTSTRAP_SOURCE = """\
import importlib.util
import sys
import traceback

if len(sys.argv) < 2:
    sys.stderr.write("python_script_runner: missing script path argument\\n")
    sys.exit(4)

script_path = sys.argv[1]
spec = importlib.util.spec_from_file_location("graph_agent_user_script", script_path)
if spec is None or spec.loader is None:
    sys.stderr.write("python_script_runner: could not load script at {}\\n".format(script_path))
    sys.exit(4)

module = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(module)
except BaseException:
    traceback.print_exc()
    sys.exit(2)

run = getattr(module, "run", None)
if not callable(run):
    sys.stderr.write("python_script_runner: script is missing a callable run()\\n")
    sys.exit(3)

try:
    result = run()
except BaseException:
    traceback.print_exc()
    sys.exit(2)

sys.exit(0 if bool(result) else 1)
"""


@dataclass
class PythonScriptRun:
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int
    timed_out: bool = False
    error: dict[str, Any] | None = None
    script_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "duration_ms": self.duration_ms,
            "timed_out": self.timed_out,
            "script_path": self.script_path,
        }


def run_script(
    script_path: str,
    *,
    payload_json: str = "{}",
    timeout_seconds: float = DEFAULT_SCRIPT_TIMEOUT_SECONDS,
    python_executable: str | None = None,
    env: dict[str, str] | None = None,
) -> PythonScriptRun:
    resolved_path = str(script_path or "").strip()
    if not resolved_path:
        return _failure(
            error_type="python_script_missing_path",
            message="No script path was provided.",
            script_path="",
        )
    script = Path(resolved_path)
    if not script.exists() or not script.is_file():
        return _failure(
            error_type="python_script_not_found",
            message=f"Script file not found: {resolved_path}",
            script_path=resolved_path,
        )

    interpreter = python_executable or sys.executable
    limit = max(float(timeout_seconds or 0), 1.0)
    command = [interpreter, "-c", BOOTSTRAP_SOURCE, str(script.resolve())]
    process_env = dict(os.environ)
    if env:
        process_env.update(env)

    start = time.monotonic()
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=process_env,
            text=False,
        )
    except OSError as exc:
        duration_ms = int((time.monotonic() - start) * 1000)
        return PythonScriptRun(
            success=False,
            exit_code=-1,
            stdout="",
            stderr="",
            duration_ms=duration_ms,
            script_path=str(script.resolve()),
            error={
                "type": "python_script_spawn_failed",
                "message": f"Failed to start interpreter: {exc}",
            },
        )

    stdout_chunks: list[bytes] = []
    stderr_chunks: list[bytes] = []
    stdout_overflow = False
    stderr_overflow = False
    timed_out = False

    try:
        if process.stdin is not None:
            try:
                process.stdin.write((payload_json or "").encode("utf-8"))
            except (BrokenPipeError, OSError):
                pass
            try:
                process.stdin.close()
            except OSError:
                pass

        selector = selectors.DefaultSelector()
        if process.stdout is not None:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        if process.stderr is not None:
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")

        deadline = start + limit
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            for key, _ in selector.select(timeout=min(remaining, 0.25)):
                stream = key.fileobj
                try:
                    chunk = stream.read1(4096)
                except (OSError, ValueError):
                    selector.unregister(stream)
                    continue
                if not chunk:
                    selector.unregister(stream)
                    continue
                if key.data == "stdout":
                    if not stdout_overflow:
                        stdout_chunks.append(chunk)
                        if sum(len(part) for part in stdout_chunks) > MAX_CAPTURED_STREAM_BYTES:
                            stdout_overflow = True
                else:
                    if not stderr_overflow:
                        stderr_chunks.append(chunk)
                        if sum(len(part) for part in stderr_chunks) > MAX_CAPTURED_STREAM_BYTES:
                            stderr_overflow = True

        if timed_out:
            process.kill()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass
            for stream, chunks, overflow_flag in (
                (process.stdout, stdout_chunks, stdout_overflow),
                (process.stderr, stderr_chunks, stderr_overflow),
            ):
                if stream is None:
                    continue
                try:
                    remaining = stream.read()
                except (OSError, ValueError):
                    remaining = b""
                if remaining and not overflow_flag:
                    chunks.append(remaining)
        else:
            try:
                process.wait(timeout=max(deadline - time.monotonic(), 0.0) or 0.1)
            except subprocess.TimeoutExpired:
                timed_out = True
                process.kill()
                process.wait(timeout=1.0)
    finally:
        try:
            selector.close()
        except Exception:  # noqa: BLE001
            pass
        for stream in (process.stdout, process.stderr, process.stdin):
            if stream is None:
                continue
            try:
                stream.close()
            except Exception:  # noqa: BLE001
                pass

    duration_ms = int((time.monotonic() - start) * 1000)
    exit_code = process.returncode if process.returncode is not None else -1
    stdout_text = _decode_stream(stdout_chunks, overflow=stdout_overflow)
    stderr_text = _decode_stream(stderr_chunks, overflow=stderr_overflow)

    if timed_out:
        return PythonScriptRun(
            success=False,
            exit_code=exit_code,
            stdout=stdout_text,
            stderr=stderr_text,
            duration_ms=duration_ms,
            timed_out=True,
            script_path=str(script.resolve()),
            error={
                "type": "python_script_timeout",
                "message": f"Script exceeded {int(limit)}s timeout and was killed.",
                "timeout_seconds": int(limit),
            },
        )

    success = exit_code == 0
    error: dict[str, Any] | None = None
    if not success:
        error = {
            "type": _error_type_for_exit_code(exit_code),
            "message": _error_message_for_exit_code(exit_code, stderr_text),
            "exit_code": exit_code,
        }

    return PythonScriptRun(
        success=success,
        exit_code=exit_code,
        stdout=stdout_text,
        stderr=stderr_text,
        duration_ms=duration_ms,
        script_path=str(script.resolve()),
        error=error,
    )


def _decode_stream(chunks: list[bytes], *, overflow: bool) -> str:
    raw = b"".join(chunks)
    text = raw.decode("utf-8", errors="replace")
    if overflow:
        text = f"{text}\n[...output truncated at {MAX_CAPTURED_STREAM_BYTES} bytes]"
    return text


def _error_type_for_exit_code(exit_code: int) -> str:
    return {
        1: "python_script_run_returned_false",
        2: "python_script_unhandled_exception",
        3: "python_script_missing_run_symbol",
        4: "python_script_bootstrap_error",
    }.get(exit_code, "python_script_failed")


def _error_message_for_exit_code(exit_code: int, stderr: str) -> str:
    base = {
        1: "run() returned False.",
        2: "run() raised an unhandled exception.",
        3: "Script does not define a callable run().",
        4: "Script bootstrap failed before run() was called.",
    }.get(exit_code, f"Script exited with code {exit_code}.")
    trimmed_stderr = (stderr or "").strip()
    if trimmed_stderr:
        return f"{base} {trimmed_stderr.splitlines()[-1]}"
    return base


def _failure(*, error_type: str, message: str, script_path: str) -> PythonScriptRun:
    return PythonScriptRun(
        success=False,
        exit_code=-1,
        stdout="",
        stderr="",
        duration_ms=0,
        script_path=script_path,
        error={"type": error_type, "message": message},
    )
