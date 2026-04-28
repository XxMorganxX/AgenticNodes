from __future__ import annotations

from collections import OrderedDict, deque
from copy import deepcopy
import logging
import os
from threading import Condition, Event, Thread
from time import monotonic
from typing import Any, Protocol

from graph_agent.api.supabase_run_store import SUPABASE_RUN_STORE_REQUEST_TIMEOUT_SECONDS
from graph_agent.runtime.event_contract import normalize_runtime_event_dict, normalize_runtime_state_snapshot


LOGGER = logging.getLogger(__name__)


def _is_transient_supabase_delegate_error(exc: BaseException) -> bool:
    """Network-ish failures from SupabaseRunStore where a full traceback is usually noise."""
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, RuntimeError):
        msg = str(exc)
        return msg.startswith("Supabase run store request timed out") or "Supabase run store network error" in msg
    return False


class RunStore(Protocol):
    def initialize_run(self, state: dict[str, Any]) -> None: ...

    def append_event(self, run_id: str, event: dict[str, Any]) -> None: ...

    def write_state(self, run_id: str, state: dict[str, Any]) -> None: ...

    def load_manifest(self, run_id: str) -> dict[str, Any] | None: ...

    def load_events(self, run_id: str) -> list[dict[str, Any]]: ...

    def load_state(self, run_id: str) -> dict[str, Any] | None: ...

    def recover_run_state(self, run_id: str) -> dict[str, Any] | None: ...

    def list_runs(self, *, graph_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]: ...


class CompositeRunStore:
    def __init__(self, primary: RunStore, *mirrors: RunStore) -> None:
        self.primary = primary
        self.mirrors = [store for store in mirrors if store is not primary]

    def initialize_run(self, state: dict[str, Any]) -> None:
        self.primary.initialize_run(state)
        self._fan_out("initialize_run", state, run_id=str(state.get("run_id") or ""))

    def append_event(self, run_id: str, event: dict[str, Any]) -> None:
        self.primary.append_event(run_id, event)
        self._fan_out("append_event", run_id, event, run_id=run_id, event_type=str(event.get("event_type") or ""))

    def write_state(self, run_id: str, state: dict[str, Any]) -> None:
        self.primary.write_state(run_id, state)
        self._fan_out("write_state", run_id, state, run_id=run_id)

    def load_manifest(self, run_id: str) -> dict[str, Any] | None:
        return self.primary.load_manifest(run_id)

    def load_events(self, run_id: str) -> list[dict[str, Any]]:
        return self.primary.load_events(run_id)

    def load_state(self, run_id: str) -> dict[str, Any] | None:
        return self.primary.load_state(run_id)

    def recover_run_state(self, run_id: str) -> dict[str, Any] | None:
        return self.primary.recover_run_state(run_id)

    def list_runs(self, *, graph_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self.primary.list_runs(graph_id=graph_id, limit=limit)

    def flush(self, timeout_seconds: float | None = None) -> None:
        flush_run_store(self.primary, timeout_seconds=timeout_seconds)
        for store in self.mirrors:
            flush_run_store(store, timeout_seconds=timeout_seconds)

    def close(self, timeout_seconds: float | None = None) -> None:
        flush_run_store(self.primary, timeout_seconds=timeout_seconds)
        for store in self.mirrors:
            flush_run_store(store, timeout_seconds=timeout_seconds)
        for store in self.mirrors:
            close_run_store(store, timeout_seconds=timeout_seconds)
        close_run_store(self.primary, timeout_seconds=timeout_seconds)

    def _fan_out(self, method_name: str, *args: Any, **context: str) -> None:
        context_str = ", ".join(f"{k}={v!r}" for k, v in context.items() if v)
        for store in self.mirrors:
            try:
                getattr(store, method_name)(*args)
            except Exception as exc:  # noqa: BLE001
                if _is_transient_supabase_delegate_error(exc):
                    LOGGER.warning(
                        "Mirrored run-store write failed (transient): method=%s store=%s%s — %s",
                        method_name,
                        type(store).__name__,
                        f" ({context_str})" if context_str else "",
                        exc,
                    )
                    continue
                LOGGER.exception(
                    "Mirrored run-store write failed: method=%s store=%s%s",
                    method_name,
                    type(store).__name__,
                    f" ({context_str})" if context_str else "",
                )


_SUPABASE_URL_ENV_VARS = ("GRAPH_AGENT_SUPABASE_URL", "SUPABASE_URL")
_SUPABASE_KEY_ENV_VARS = (
    "GRAPH_AGENT_SUPABASE_SECRET_KEY",
    "GRAPH_AGENT_SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_SECRET_KEY",
)
_DEFAULT_MIRROR_FLUSH_INTERVAL_MS = 100
_DEFAULT_MIRROR_EVENT_BATCH_SIZE = 40
_DEFAULT_MIRROR_RUN_BATCH_SIZE = 10
# Must exceed worst-case Supabase HTTP timeout so terminal flushes don't give up early.
_DEFAULT_MIRROR_FLUSH_TIMEOUT_SECONDS = max(35.0, float(SUPABASE_RUN_STORE_REQUEST_TIMEOUT_SECONDS) + 25.0)


def _read_positive_int_env(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return default
    try:
        return max(int(raw), 1)
    except ValueError:
        return default


def _read_bool_env(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _read_positive_float_env(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return default
    try:
        return max(float(raw), 0.001)
    except ValueError:
        return default


def flush_run_store(store: RunStore, *, timeout_seconds: float | None = None) -> None:
    flush = getattr(store, "flush", None)
    if callable(flush):
        flush(timeout_seconds=timeout_seconds)


def close_run_store(store: RunStore, *, timeout_seconds: float | None = None) -> None:
    close = getattr(store, "close", None)
    if callable(close):
        close(timeout_seconds=timeout_seconds)


class AsyncBatchingRunStoreMirror:
    def __init__(
        self,
        delegate: RunStore,
        *,
        flush_interval_ms: int | None = None,
        event_batch_size: int | None = None,
        run_batch_size: int | None = None,
        flush_timeout_seconds: float | None = None,
    ) -> None:
        self.delegate = delegate
        self.flush_interval_seconds = (
            float(flush_interval_ms) / 1000.0
            if flush_interval_ms is not None
            else _read_positive_int_env(
                "GRAPH_AGENT_RUN_STORE_MIRROR_FLUSH_INTERVAL_MS",
                _DEFAULT_MIRROR_FLUSH_INTERVAL_MS,
            )
            / 1000.0
        )
        self.event_batch_size = (
            max(int(event_batch_size), 1)
            if event_batch_size is not None
            else _read_positive_int_env(
                "GRAPH_AGENT_RUN_STORE_MIRROR_EVENT_BATCH_SIZE",
                _DEFAULT_MIRROR_EVENT_BATCH_SIZE,
            )
        )
        self.run_batch_size = (
            max(int(run_batch_size), 1)
            if run_batch_size is not None
            else _read_positive_int_env(
                "GRAPH_AGENT_RUN_STORE_MIRROR_RUN_BATCH_SIZE",
                _DEFAULT_MIRROR_RUN_BATCH_SIZE,
            )
        )
        self.flush_timeout_seconds = (
            max(float(flush_timeout_seconds), 0.001)
            if flush_timeout_seconds is not None
            else _read_positive_float_env(
                "GRAPH_AGENT_RUN_STORE_MIRROR_FLUSH_TIMEOUT_SECONDS",
                _DEFAULT_MIRROR_FLUSH_TIMEOUT_SECONDS,
            )
        )
        self._lock = Condition()
        self._wake_event = Event()
        self._pending_initializations: deque[dict[str, Any]] = deque()
        self._pending_events: deque[tuple[str, dict[str, Any]]] = deque()
        self._pending_states: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._processing = False
        self._closing = False
        self._closed = False
        self._worker = Thread(
            target=self._worker_loop,
            name=f"run-store-mirror-{id(self):x}",
            daemon=True,
        )
        self._worker.start()

    def initialize_run(self, state: dict[str, Any]) -> None:
        snapshot = normalize_runtime_state_snapshot(state) or deepcopy(state)
        if self._queue_write("initialize", snapshot=snapshot):
            return
        self._log_passthrough_failure("initialize_run", run_id=str(snapshot.get("run_id") or ""), payload=snapshot)

    def append_event(self, run_id: str, event: dict[str, Any]) -> None:
        normalized = normalize_runtime_event_dict(event)
        if self._queue_write("event", run_id=run_id, payload=normalized):
            return
        self._log_passthrough_failure("append_event", run_id=run_id, payload=normalized)

    def write_state(self, run_id: str, state: dict[str, Any]) -> None:
        snapshot = normalize_runtime_state_snapshot(state) or deepcopy(state)
        if self._queue_write("state", run_id=run_id, payload=snapshot):
            return
        self._log_passthrough_failure("write_state", run_id=run_id, payload=snapshot)

    def load_manifest(self, run_id: str) -> dict[str, Any] | None:
        return self.delegate.load_manifest(run_id)

    def load_events(self, run_id: str) -> list[dict[str, Any]]:
        return self.delegate.load_events(run_id)

    def load_state(self, run_id: str) -> dict[str, Any] | None:
        return self.delegate.load_state(run_id)

    def recover_run_state(self, run_id: str) -> dict[str, Any] | None:
        return self.delegate.recover_run_state(run_id)

    def list_runs(self, *, graph_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self.delegate.list_runs(graph_id=graph_id, limit=limit)

    def flush(self, timeout_seconds: float | None = None) -> None:
        timeout = self.flush_timeout_seconds if timeout_seconds is None else max(float(timeout_seconds), 0.001)
        deadline = monotonic() + timeout
        with self._lock:
            if self._closed and not self._has_pending_locked():
                return
            self._wake_event.set()
            while self._has_pending_locked() or self._processing:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    LOGGER.warning(
                        "Timed out waiting for async run-store mirror to flush pending writes (delegate=%s).",
                        type(self.delegate).__name__,
                    )
                    return
                self._lock.wait(timeout=remaining)

    def close(self, timeout_seconds: float | None = None) -> None:
        with self._lock:
            if self._closed:
                return
            self._closing = True
            self._wake_event.set()
        self.flush(timeout_seconds=timeout_seconds)
        self._worker.join(timeout=self.flush_timeout_seconds if timeout_seconds is None else timeout_seconds)
        with self._lock:
            if self._worker.is_alive():
                LOGGER.warning(
                    "Async run-store mirror worker did not exit cleanly before timeout (delegate=%s).",
                    type(self.delegate).__name__,
                )
            self._closed = True
            self._lock.notify_all()
        close_run_store(self.delegate, timeout_seconds=timeout_seconds)

    def _queue_write(
        self,
        write_type: str,
        *,
        run_id: str | None = None,
        payload: dict[str, Any] | None = None,
        snapshot: dict[str, Any] | None = None,
    ) -> bool:
        with self._lock:
            if self._closed or self._closing:
                return False
            if write_type == "initialize" and snapshot is not None:
                self._pending_initializations.append(snapshot)
            elif write_type == "event" and run_id is not None and payload is not None:
                self._pending_events.append((run_id, payload))
            elif write_type == "state" and run_id is not None and payload is not None:
                self._pending_states[run_id] = payload
                self._pending_states.move_to_end(run_id)
            else:
                return False
            if self._should_wake_locked():
                self._wake_event.set()
            self._lock.notify_all()
            return True

    def _should_wake_locked(self) -> bool:
        return (
            len(self._pending_initializations) >= self.run_batch_size
            or len(self._pending_events) >= self.event_batch_size
            or len(self._pending_states) >= self.run_batch_size
            or self._closing
        )

    def _has_pending_locked(self) -> bool:
        return bool(self._pending_initializations or self._pending_events or self._pending_states)

    def _worker_loop(self) -> None:
        while True:
            self._wake_event.wait(self.flush_interval_seconds)
            self._wake_event.clear()
            self._drain_pending_batches()
            with self._lock:
                if self._closing and not self._has_pending_locked() and not self._processing:
                    self._closed = True
                    self._lock.notify_all()
                    return

    def _drain_pending_batches(self) -> None:
        while True:
            with self._lock:
                if not self._has_pending_locked():
                    self._processing = False
                    self._lock.notify_all()
                    return
                initialize_batch: list[dict[str, Any]] = []
                while self._pending_initializations and len(initialize_batch) < self.run_batch_size:
                    initialize_batch.append(self._pending_initializations.popleft())
                event_batch: list[tuple[str, dict[str, Any]]] = []
                while self._pending_events and len(event_batch) < self.event_batch_size:
                    event_batch.append(self._pending_events.popleft())
                state_batch: list[tuple[str, dict[str, Any]]] = []
                while self._pending_states and len(state_batch) < self.run_batch_size:
                    run_id, state = self._pending_states.popitem(last=False)
                    state_batch.append((run_id, state))
                self._processing = True
            self._write_initialize_batch(initialize_batch)
            self._write_event_batch(event_batch)
            self._write_state_batch(state_batch)
            with self._lock:
                self._processing = self._has_pending_locked()
                self._lock.notify_all()

    def _write_initialize_batch(self, states: list[dict[str, Any]]) -> None:
        if not states:
            return
        initialize_batch = getattr(self.delegate, "initialize_runs_batch", None)
        try:
            if callable(initialize_batch):
                initialize_batch(states)
            else:
                for state in states:
                    self.delegate.initialize_run(state)
        except Exception as exc:  # noqa: BLE001
            if _is_transient_supabase_delegate_error(exc):
                LOGGER.warning(
                    "Async run-store mirror dropped initialize batch after transient delegate failure "
                    "(delegate=%s, count=%d): %s",
                    type(self.delegate).__name__,
                    len(states),
                    exc,
                )
                return
            LOGGER.exception(
                "Async run-store mirror dropped initialize batch after delegate failure (delegate=%s, count=%d).",
                type(self.delegate).__name__,
                len(states),
            )

    def _write_event_batch(self, items: list[tuple[str, dict[str, Any]]]) -> None:
        if not items:
            return
        append_batch = getattr(self.delegate, "append_events_batch", None)
        try:
            if callable(append_batch):
                append_batch(items)
            else:
                for run_id, event in items:
                    self.delegate.append_event(run_id, event)
        except Exception as exc:  # noqa: BLE001
            if _is_transient_supabase_delegate_error(exc):
                LOGGER.warning(
                    "Async run-store mirror dropped event batch after transient delegate failure "
                    "(delegate=%s, count=%d): %s",
                    type(self.delegate).__name__,
                    len(items),
                    exc,
                )
                return
            LOGGER.exception(
                "Async run-store mirror dropped event batch after delegate failure (delegate=%s, count=%d).",
                type(self.delegate).__name__,
                len(items),
            )

    def _write_state_batch(self, items: list[tuple[str, dict[str, Any]]]) -> None:
        if not items:
            return
        write_batch = getattr(self.delegate, "write_states_batch", None)
        try:
            if callable(write_batch):
                write_batch(items)
            else:
                for run_id, state in items:
                    self.delegate.write_state(run_id, state)
        except Exception as exc:  # noqa: BLE001
            if _is_transient_supabase_delegate_error(exc):
                LOGGER.warning(
                    "Async run-store mirror dropped state batch after transient delegate failure "
                    "(delegate=%s, count=%d): %s",
                    type(self.delegate).__name__,
                    len(items),
                    exc,
                )
                return
            LOGGER.exception(
                "Async run-store mirror dropped state batch after delegate failure (delegate=%s, count=%d).",
                type(self.delegate).__name__,
                len(items),
            )

    def _log_passthrough_failure(self, method_name: str, *, run_id: str, payload: dict[str, Any]) -> None:
        try:
            getattr(self.delegate, method_name)(run_id, payload) if method_name != "initialize_run" else self.delegate.initialize_run(payload)
        except Exception:  # noqa: BLE001
            LOGGER.exception(
                "Async run-store mirror fallback write failed after close (delegate=%s, method=%s, run_id=%r).",
                type(self.delegate).__name__,
                method_name,
                run_id,
            )


def build_default_run_store() -> RunStore:
    backend = os.environ.get("GRAPH_AGENT_RUN_STORE", "filesystem").strip().lower() or "filesystem"
    if backend == "supabase":
        from graph_agent.api.supabase_run_store import SupabaseRunStore
        from graph_agent.api.run_log_store import FilesystemRunStore

        supabase_store = SupabaseRunStore.from_env()
        mirror_disabled = _read_bool_env("GRAPH_AGENT_RUN_STORE_MIRROR_DISABLED", False)
        supabase_primary = _read_bool_env("GRAPH_AGENT_RUN_STORE_SUPABASE_PRIMARY", False)

        # Legacy: Supabase is the only persistence/query target (still wrapped in AsyncBatchingRunStoreMirror by default).
        if supabase_primary:
            if mirror_disabled:
                return supabase_store
            return AsyncBatchingRunStoreMirror(supabase_store)

        # Default: durable local filesystem + async mirror to Supabase (mirroring drops do not lose local runs).
        fs_store = FilesystemRunStore()
        if mirror_disabled:
            return CompositeRunStore(fs_store, supabase_store)
        return CompositeRunStore(fs_store, AsyncBatchingRunStoreMirror(supabase_store))
    has_url = any(os.environ.get(name, "").strip() for name in _SUPABASE_URL_ENV_VARS)
    has_key = any(os.environ.get(name, "").strip() for name in _SUPABASE_KEY_ENV_VARS)
    if has_url and has_key:
        LOGGER.warning(
            "Supabase credentials are present in the environment but GRAPH_AGENT_RUN_STORE is %r; "
            "run logs stay under .logs/runs/ unless you set GRAPH_AGENT_RUN_STORE=supabase "
            "(filesystem primary + Supabase mirror by default). Use GRAPH_AGENT_RUN_STORE_SUPABASE_PRIMARY=1 "
            "only if you need legacy Supabase-only reads.",
            backend,
        )
    from graph_agent.api.run_log_store import FilesystemRunStore

    return FilesystemRunStore()
