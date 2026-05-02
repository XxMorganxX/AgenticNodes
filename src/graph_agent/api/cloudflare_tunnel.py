"""Managed `cloudflared` subprocess for named Cloudflare tunnels.

The tunnel token is read from the process environment using the env-var name
stored in :class:`CloudflareConfigStore` — never from persisted JSON.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable

from graph_agent.api.cloudflare_store import CloudflareConfigStore

LOGGER = logging.getLogger(__name__)

LOG_TAIL_MAX_LINES = 48
CLOUDFLARED_ENV_VAR = "GRAPH_AGENT_CLOUDFLARED_PATH"

SubprocessFactory = Callable[..., Any]


class CloudflareTunnelManager:
    """Start one shared ``cloudflared`` process while inbound-webhook listener sessions are active."""

    def __init__(
        self,
        config_store: CloudflareConfigStore | None = None,
        *,
        environ: dict[str, str] | None = None,
        which_cloudflared: Callable[[], str | None] | None = None,
        subprocess_popen: SubprocessFactory | None = None,
    ) -> None:
        self._config_store = config_store or CloudflareConfigStore()
        self._environ = environ if environ is not None else os.environ
        self._which_cloudflared = which_cloudflared or (lambda: shutil.which("cloudflared"))
        self._subprocess_popen = subprocess_popen or subprocess.Popen

        self._lock = threading.RLock()
        self._active_graph_ids: set[str] = set()
        self._proc: subprocess.Popen[bytes] | None = None
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._watcher_thread: threading.Thread | None = None
        self._log_tail: deque[str] = deque(maxlen=LOG_TAIL_MAX_LINES)
        self._state: str = "stopped"
        self._last_error: str | None = None
        self._last_exit_code: int | None = None
        self._pid: int | None = None

    def acquire_for_inbound_webhook(self, graph_id: str) -> None:
        """Ensure the tunnel is running; ref-count by graph id."""
        normalized = str(graph_id or "").strip()
        if not normalized:
            return
        with self._lock:
            self._active_graph_ids.add(normalized)
            if self._proc is not None and self._proc.poll() is None:
                return
        try:
            self._start_locked_outer()
        except Exception:
            with self._lock:
                self._active_graph_ids.discard(normalized)
            raise

    def release_for_inbound_webhook(self, graph_id: str) -> None:
        normalized = str(graph_id or "").strip()
        if not normalized:
            return
        with self._lock:
            self._active_graph_ids.discard(normalized)
            if self._active_graph_ids:
                return
        self._stop_process()

    def shutdown(self) -> None:
        """Force-stop the tunnel and clear all refs (runtime reset / process exit)."""
        with self._lock:
            self._active_graph_ids.clear()
        self._stop_process()

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            active = sorted(self._active_graph_ids)
            state = self._state
            pid = self._pid
            if self._proc is not None and self._proc.poll() is None:
                state = "running"
            tail = list(self._log_tail)
            return {
                "tunnel_state": state,
                "tunnel_pid": pid,
                "tunnel_ref_count": len(self._active_graph_ids),
                "tunnel_active_graph_ids": active,
                "tunnel_last_error": self._last_error,
                "tunnel_last_exit_code": self._last_exit_code,
                "tunnel_log_tail": tail,
            }

    # --- internal ---

    def _start_locked_outer(self) -> None:
        try:
            self._start_process()
        except Exception as exc:
            with self._lock:
                # Caller may still hold session state; clear our ref on failure path from acquire.
                self._last_error = str(exc)
                self._state = "failed"
            raise

    def _start_process(self) -> None:
        cfg = self._config_store.get()
        token_env = str(cfg.get("tunnel_token_env_var") or "").strip()
        if not token_env:
            token_env = "CLOUDFLARE_TUNNEL_TOKEN"
        token = str(self._environ.get(token_env, "") or "").strip()
        if not token:
            raise RuntimeError(
                f"Tunnel token env-var {token_env!r} is not set or empty in the API process environment."
            )

        binary = self._resolve_cloudflared_binary()
        if not binary:
            raise RuntimeError(
                "cloudflared binary not found. Install cloudflared and ensure it is on PATH, "
                f"or set {CLOUDFLARED_ENV_VAR} to the full path."
            )

        args = [binary, "tunnel", "--no-autoupdate", "run", "--token", token]

        with self._lock:
            self._stop_process_locked()
            self._log_tail.clear()
            self._last_error = None
            self._last_exit_code = None
            self._state = "starting"

        creationflags = 0
        if os.name == "nt":
            creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))

        try:
            proc = self._subprocess_popen(
                args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self._environ,
                creationflags=creationflags,
            )
        except OSError as exc:
            with self._lock:
                self._state = "failed"
                self._last_error = str(exc)
            raise RuntimeError(f"Failed to start cloudflared: {exc}") from exc

        with self._lock:
            self._proc = proc
            self._pid = proc.pid
            self._state = "running"

        self._stdout_thread = threading.Thread(
            target=self._drain_stream,
            args=(proc.stdout, "stdout"),
            name="cloudflared-stdout",
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._drain_stream,
            args=(proc.stderr, "stderr"),
            name="cloudflared-stderr",
            daemon=True,
        )
        self._stdout_thread.start()
        self._stderr_thread.start()

        self._watcher_thread = threading.Thread(target=self._watch_process, name="cloudflared-watcher", daemon=True)
        self._watcher_thread.start()

        LOGGER.info("Started cloudflared tunnel process pid=%s", proc.pid)

    def _resolve_cloudflared_binary(self) -> str | None:
        override = str(self._environ.get(CLOUDFLARED_ENV_VAR) or "").strip()
        if override:
            p = Path(override)
            if p.is_file():
                return str(p)
        return self._which_cloudflared()

    def _drain_stream(self, stream: Any, label: str) -> None:
        if stream is None:
            return
        try:
            for raw in iter(stream.readline, b""):
                if not raw:
                    break
                line = raw.decode("utf-8", errors="replace").rstrip()
                entry = f"[{label}] {line}"
                with self._lock:
                    self._log_tail.append(entry)
        except Exception:  # noqa: BLE001
            LOGGER.debug("cloudflared %s drain ended", label, exc_info=True)

    def _watch_process(self) -> None:
        proc = self._proc
        if proc is None:
            return
        try:
            code = proc.wait()
        except Exception:  # noqa: BLE001
            code = -1
        with self._lock:
            self._last_exit_code = int(code) if code is not None else None
            if self._proc is proc:
                self._proc = None
                self._pid = None
                if self._active_graph_ids:
                    self._state = "failed"
                    self._last_error = (
                        f"cloudflared exited unexpectedly (code {self._last_exit_code}). "
                        "Stop and restart the listener session after fixing cloudflared or the token."
                    )
                    LOGGER.error("%s", self._last_error)
                else:
                    self._state = "stopped"

    def _stop_process(self) -> None:
        with self._lock:
            self._stop_process_locked()

    def _stop_process_locked(self) -> None:
        proc = self._proc
        if proc is None:
            return
        self._proc = None
        self._pid = None
        if proc.poll() is None:
            try:
                proc.terminate()
                for _ in range(40):
                    if proc.poll() is not None:
                        break
                    time.sleep(0.05)
                if proc.poll() is None:
                    proc.kill()
            except OSError as exc:
                LOGGER.warning("Error stopping cloudflared: %s", exc)
        self._state = "stopped"
