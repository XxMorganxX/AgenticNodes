"""Minimal CLI for launching graph runs against a running backend.

Usage:
    agents                                                # list graphs + currently running runs
    agents --group <id|name> [--Agent <id|name>] [--Input <json|@file>]
                                                          # launch in background, print run_id, exit
    agents --group <id|name> ... --Follow                 # launch + stream events until terminal
    agents view [<run_id>] [--swimlane]                   # tail a run's log (raw or swimlane)
    agents runs --group <id|name> [--Limit N]             # list recent runs for a graph
    agents --kill                                         # soft-cancel every active run on the backend

Talks to the FastAPI server at $GRAPH_AGENT_API_URL (default http://127.0.0.1:8000).

Background-by-default: launching does not hold the terminal, so multiple runs can be
fired in succession without each blocking the next. Each run writes two log files:

    ~/.agents/runs/<run_id>.log              # raw event stream
    ~/.agents/runs/<run_id>.swimlane.log     # per-agent tagged view, useful for multi-agent

Watch one with:

    agents view                  # tails the latest raw log
    agents view --swimlane       # tails the latest swimlane log
    tail -f ~/.agents/runs/<run_id>.log

Log paths in the launch output and listings are emitted as OSC 8 hyperlinks, so
Cmd-click in iTerm2 / Terminal.app / Warp / VS Code opens them directly.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, TextIO

import httpx


DEFAULT_API_URL = "http://127.0.0.1:8000"
TERMINAL_FAILURE_EVENTS = {"run.failed", "run.cancelled", "run.interrupted"}
TERMINAL_RUN_STATUSES = {"completed", "failed", "cancelled", "interrupted"}
FAILED_RUN_STATUSES = {"failed", "cancelled", "interrupted"}
STATUS_COLORS = {
    "running": "cyan",
    "queued": "cyan",
    "completed": "green",
    "failed": "magenta",
    "cancelled": "magenta",
    "interrupted": "yellow",
}

LOG_DIR = Path.home() / ".agents" / "runs"

_ANSI = {
    "reset": "\x1b[0m",
    "bold": "\x1b[1m",
    "dim": "\x1b[2m",
    "cyan": "\x1b[36m",
    "yellow": "\x1b[33m",
    "green": "\x1b[32m",
    "magenta": "\x1b[35m",
}
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _use_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty()


def _c(text: str, *codes: str) -> str:
    if not _use_color():
        return text
    return "".join(_ANSI[c] for c in codes) + text + _ANSI["reset"]


def _emit(line: str, *, file: TextIO | None, to_stdout: bool) -> None:
    """Write a (possibly ANSI-colored) line to stdout and/or a log file (plain)."""
    if to_stdout:
        print(line, flush=True)
    if file is not None:
        file.write(_ANSI_RE.sub("", line) + "\n")
        file.flush()


def _link(text: str, uri: str) -> str:
    """Wrap text in an OSC 8 hyperlink (Cmd-clickable in modern terminals).

    Falls back to plain text when stdout is not a TTY or NO_COLOR is set.
    """
    if not _use_color():
        return text
    return f"\x1b]8;;{uri}\x1b\\{text}\x1b]8;;\x1b\\"


def _parse_input(raw: str | None) -> Any:
    if raw is None:
        return None
    if raw.startswith("@"):
        path = raw[1:]
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = fh.read()
        except OSError as exc:
            raise SystemExit(f"error: could not read --Input file '{path}': {exc}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"error: --Input is not valid JSON: {exc}")


def _detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text or f"HTTP {response.status_code}"
    if isinstance(body, dict) and "detail" in body:
        return str(body["detail"])
    return response.text or f"HTTP {response.status_code}"


def _label(item: dict[str, Any], fallback: str) -> str:
    name = str(item.get("name") or "").strip()
    return name or fallback


def _resolve_graph(client: httpx.Client, given: str) -> tuple[str, dict[str, Any]]:
    """Look up a graph by graph_id (slug) first, then by display name."""
    try:
        resp = client.get(f"/api/graphs/{given}")
    except httpx.HTTPError as exc:
        raise SystemExit(f"error: cannot reach backend at {client.base_url}: {exc}")
    if resp.status_code == 200:
        return given, resp.json()
    if resp.status_code != 404:
        raise SystemExit(f"error: failed to load graph '{given}': {_detail(resp)}")

    list_resp = client.get("/api/graphs")
    if list_resp.status_code != 200:
        raise SystemExit(f"error: failed to list graphs: {_detail(list_resp)}")
    graphs = list_resp.json().get("graphs") or []
    matches = [g for g in graphs if str(g.get("name") or "").strip() == given]
    if not matches:
        raise SystemExit(f"error: unknown graph '{given}'")
    if len(matches) > 1:
        ids = ", ".join(str(m.get("graph_id") or "") for m in matches)
        raise SystemExit(
            f"error: multiple graphs named '{given}'. Disambiguate with graph_id: {ids}"
        )
    gid = str(matches[0].get("graph_id") or "")
    full = client.get(f"/api/graphs/{gid}")
    if full.status_code != 200:
        raise SystemExit(f"error: failed to load resolved graph '{gid}': {_detail(full)}")
    return gid, full.json()


def _validate_agent_flag(graph: dict[str, Any], graph_id: str, agent: str | None) -> list[str] | None:
    graph_type = str(graph.get("graph_type") or "graph")
    agents = graph.get("agents") if isinstance(graph.get("agents"), list) else []
    is_test_env = graph_type == "test_environment"

    if not is_test_env:
        if agent is not None:
            raise SystemExit(
                f"error: --Agent is only valid for test_environment graphs; '{graph_id}' is a single graph"
            )
        return None

    available = [
        (str(a.get("agent_id")), str(a.get("name") or "").strip())
        for a in agents
        if isinstance(a, dict) and a.get("agent_id")
    ]
    labels = [name or aid for aid, name in available]
    listing = ", ".join(labels) if labels else "(none defined)"

    if agent is None:
        raise SystemExit(
            f"error: --Agent is required for test_environment '{graph_id}'. Available agents: {listing}"
        )

    by_id = next((aid for aid, _ in available if aid == agent), None)
    if by_id is not None:
        return [by_id]
    by_name = [aid for aid, name in available if name == agent]
    if len(by_name) == 1:
        return by_name
    if len(by_name) > 1:
        ids = ", ".join(by_name)
        raise SystemExit(
            f"error: multiple agents named '{agent}' in '{graph_id}'. Disambiguate with agent_id: {ids}"
        )
    raise SystemExit(
        f"error: agent '{agent}' not found in '{graph_id}'. Available agents: {listing}"
    )


def _list_graphs(client: httpx.Client) -> int:
    try:
        resp = client.get("/api/graphs")
    except httpx.HTTPError as exc:
        raise SystemExit(f"error: cannot reach backend at {client.base_url}: {exc}")
    if resp.status_code != 200:
        raise SystemExit(f"error: failed to list graphs: {_detail(resp)}")
    graphs = resp.json().get("graphs") or []
    if not graphs:
        print("No graphs found.")
        return 0

    rows = sorted(
        graphs,
        key=lambda g: (
            str(g.get("graph_type") or ""),
            _label(g, str(g.get("graph_id") or "")).lower(),
        ),
    )
    label_w = max(len(_label(g, str(g.get("graph_id") or ""))) for g in rows)
    type_w = max(len(str(g.get("graph_type") or "graph")) for g in rows) + 2

    print(_c(f"GRAPHS  ({len(rows)})", "bold"))
    print()
    for idx, g in enumerate(rows):
        graph_id = str(g.get("graph_id") or "")
        graph_type = str(g.get("graph_type") or "graph")
        is_env = graph_type == "test_environment"
        label = _label(g, graph_id)

        bullet = _c("●", "yellow" if is_env else "cyan")
        glabel = _c(label.ljust(label_w), "bold", "cyan")
        gtype = _c(f"[{graph_type}]".ljust(type_w), "dim")
        slug_suffix = _c(graph_id, "dim") if label != graph_id else ""
        print(f"  {bullet}  {glabel}  {gtype}  {slug_suffix}".rstrip())

        if is_env:
            agents = g.get("agents") if isinstance(g.get("agents"), list) else []
            agent_pairs = [
                (str(a.get("agent_id")), _label(a, str(a.get("agent_id") or "")))
                for a in agents
                if isinstance(a, dict) and a.get("agent_id")
            ]
            for i, (aid, alabel) in enumerate(agent_pairs):
                last = i == len(agent_pairs) - 1
                connector = "└─" if last else "├─"
                print(f"     {_c(connector, 'dim')} {_c(alabel, 'green')}")
                cont = "   " if last else _c("│", "dim") + "  "
                cmd = (
                    f"agents --group {shlex.quote(label)} "
                    f"--Agent {shlex.quote(alabel)} --Input '{{}}'"
                )
                print(f"     {cont}{_c(cmd, 'dim')}")
        else:
            cmd = f"agents --group {shlex.quote(label)} --Input '{{}}'"
            print(f"     {_c(cmd, 'dim')}")

        if idx != len(rows) - 1:
            print()

    _print_active_runs(client, rows)
    return 0


def _print_active_runs(client: httpx.Client, graphs: list[dict[str, Any]]) -> None:
    """Fetch recent runs for each graph, filter non-terminal ones, print a section.

    Best-effort: a per-graph short timeout so one wedged graph doesn't hang the listing.
    """
    active: list[tuple[dict[str, Any], dict[str, Any]]] = []
    skipped = 0
    for g in graphs:
        gid = str(g.get("graph_id") or "")
        if not gid:
            continue
        try:
            resp = client.get(f"/api/graphs/{gid}/runs", params={"limit": 20}, timeout=3.0)
        except httpx.HTTPError:
            skipped += 1
            continue
        if resp.status_code != 200:
            skipped += 1
            continue
        for r in resp.json().get("runs") or []:
            status = str(r.get("status") or "")
            if status and status not in TERMINAL_RUN_STATUSES:
                active.append((g, r))

    if not active:
        if skipped:
            print()
            print(_c(f"(active runs: skipped {skipped} graph{'s' if skipped != 1 else ''} — backend slow)", "dim"))
        return

    print()
    print(_c(f"RUNNING  ({len(active)})", "bold"))
    print()

    def _row_label(graph: dict[str, Any], run: dict[str, Any]) -> str:
        glabel = _label(graph, str(graph.get("graph_id") or ""))
        agent_name = run.get("agent_name") or run.get("agent_id")
        if agent_name:
            return f"{glabel} · {agent_name}"
        if run.get("parent_run_id"):
            return f"{glabel} · (envelope)"
        return glabel

    rows = [(g, r, _row_label(g, r)) for g, r in active]
    label_w = max(len(label) for _, _, label in rows)
    rid_w = max(len(str(r.get("run_id") or "")) for _, r, _ in rows)
    for g, r, label in rows:
        run_id = str(r.get("run_id") or "")
        status = str(r.get("status") or "?")
        started = _format_ts(r.get("started_at")) if r.get("started_at") else "?"
        color = STATUS_COLORS.get(status, "cyan")
        bullet = _c("●", color)
        agent_name = r.get("agent_name") or r.get("agent_id")
        if agent_name:
            glabel = _label(g, str(g.get("graph_id") or ""))
            label_text = (
                _c(glabel, "bold", "cyan")
                + _c(" · ", "dim")
                + _c(str(agent_name), "green")
            )
        elif r.get("parent_run_id"):
            glabel = _label(g, str(g.get("graph_id") or ""))
            label_text = _c(glabel, "bold", "cyan") + _c(" · (envelope)", "dim")
        else:
            label_text = _c(_label(g, str(g.get("graph_id") or "")), "bold", "cyan")
        pad = " " * max(0, label_w - len(label))
        log_path = _log_path_for(run_id)
        log_text = ""
        if log_path.exists():
            log_text = "  " + _c("log:", "dim") + " " + _link(str(log_path), log_path.as_uri())
        print(
            f"  {bullet}  "
            f"{label_text}{pad}  "
            f"{_c(run_id.ljust(rid_w), 'dim')}  "
            f"{_c(status, color)}  "
            f"{_c('started ' + started, 'dim')}"
            f"{log_text}"
        )


def _format_ts(raw: str | None) -> str:
    if not raw:
        return _local_ts()
    text = str(raw)
    if "T" in text:
        time_part = text.split("T", 1)[1]
        time_part = time_part.split("+", 1)[0].split("-", 1)[0]
        return time_part.split(".", 1)[0].rstrip("Z")
    return text


def _local_ts() -> str:
    return time.strftime("%H:%M:%S", time.localtime())


def _row_suffix(payload: dict[str, Any]) -> str:
    row_number = payload.get("iterator_row_number")
    row_index = payload.get("iterator_row_index")
    total = payload.get("iterator_total_rows")
    if isinstance(total, int) and total > 0:
        if isinstance(row_number, int) and isinstance(row_index, int):
            return f"  (row {row_number} ({row_index})/{total})"
        if isinstance(row_number, int):
            return f"  (row {row_number}/{total})"
        if isinstance(row_index, int):
            return f"  (row {row_index}/{total})"
    return ""


def _format_error(err: Any, limit: int = 200) -> str:
    if err is None:
        return ""
    if isinstance(err, dict):
        msg = err.get("message") or err.get("detail") or err.get("error") or err.get("type")
        text = str(msg) if msg else json.dumps(err, separators=(",", ":"))
    else:
        text = str(err)
    text = text.replace("\n", " ").strip()
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return text


def _log_path_for(run_id: str) -> Path:
    return LOG_DIR / f"{run_id}.log"


def _swimlane_log_path_for(run_id: str) -> Path:
    return LOG_DIR / f"{run_id}.swimlane.log"


def _print_launch_header(
    graph_label: str,
    agent_label: str | None,
    run_id: str,
    *,
    file: TextIO | None,
    to_stdout: bool,
) -> None:
    header = f"{graph_label} / {agent_label}" if agent_label else graph_label
    ts = _c(_local_ts(), "dim")
    _emit(f"{ts}  " + _c(f"▶ {header}", "bold"), file=file, to_stdout=to_stdout)
    _emit(f"{' ' * 8}  " + _c(f"run {run_id}", "dim"), file=file, to_stdout=to_stdout)


def _stream_events(
    client: httpx.Client,
    run_id: str,
    *,
    file: TextIO | None = None,
    swimlane_file: TextIO | None = None,
    to_stdout: bool = True,
) -> int:
    final_event_type: str | None = None
    step = 0
    pending: dict[str, int] = {}
    last_node_error = False
    swim = _SwimlaneRenderer(swimlane_file) if swimlane_file is not None else None
    try:
        with client.stream("GET", f"/api/runs/{run_id}/events", timeout=None) as response:
            if response.status_code == 404:
                response.read()
                raise SystemExit(f"error: unknown run '{run_id}'")
            if response.status_code != 200:
                response.read()
                raise SystemExit(f"error: SSE stream failed: {_detail(response)}")
            for line in response.iter_lines():
                if not line or not line.startswith("data:"):
                    continue
                raw = line[5:].lstrip()
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict):
                    continue
                etype = str(event.get("event_type") or "")
                payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                base = etype[len("agent."):] if etype.startswith("agent.") else etype
                ts = _c(_format_ts(event.get("timestamp")), "dim")

                if base == "node.started":
                    step += 1
                    node_id = str(payload.get("node_id") or "?")
                    pending[node_id] = step
                    idx = _c(f"[{step:>3}]", "dim")
                    arrow = _c("▶", "cyan")
                    suffix = _c(_row_suffix(payload), "dim")
                    _emit(f"{ts}  {idx} {arrow} {node_id}{suffix}", file=file, to_stdout=to_stdout)
                elif base == "node.completed":
                    node_id = str(payload.get("node_id") or "?")
                    num = pending.pop(node_id, step)
                    idx = _c(f"[{num:>3}]", "dim")
                    err = payload.get("error")
                    suffix = _c(_row_suffix(payload), "dim")
                    if err:
                        last_node_error = True
                        mark = _c("✗", "magenta")
                        err_text = _c(f"  — {_format_error(err)}", "magenta")
                        _emit(f"{ts}  {idx} {mark} {node_id}{suffix}{err_text}", file=file, to_stdout=to_stdout)
                    else:
                        last_node_error = False
                        mark = _c("✓", "green")
                        _emit(f"{ts}  {idx} {mark} {node_id}{suffix}", file=file, to_stdout=to_stdout)
                elif base == "run.completed":
                    _emit(f"{ts}  " + _c("✔ run.completed", "bold", "green"), file=file, to_stdout=to_stdout)
                elif base == "run.failed":
                    if last_node_error:
                        _emit(f"{ts}  " + _c("✗ run.failed", "bold", "magenta"), file=file, to_stdout=to_stdout)
                    else:
                        err = _format_error(payload.get("error"))
                        tail = f": {err}" if err else ""
                        _emit(f"{ts}  " + _c(f"✗ run.failed{tail}", "bold", "magenta"), file=file, to_stdout=to_stdout)
                elif base == "run.cancelled":
                    _emit(f"{ts}  " + _c("✗ run.cancelled", "bold", "magenta"), file=file, to_stdout=to_stdout)
                elif base == "run.interrupted":
                    reason = payload.get("reason") or ""
                    _emit(f"{ts}  " + _c(f"⚠ run.interrupted: {reason}", "bold", "yellow"), file=file, to_stdout=to_stdout)

                if swim is not None:
                    swim.feed(event, base)

                if etype:
                    final_event_type = etype
    except KeyboardInterrupt:
        if to_stdout:
            print(_c("\n(detached — run continues in background; log is still being written)", "dim"), flush=True)
        return 0
    return 1 if final_event_type in TERMINAL_FAILURE_EVENTS else 0


class _SwimlaneRenderer:
    """Renders a tagged chronological swimlane view to a plain-text log file.

    Per-agent step counters keep each lane's progress legible; the agent column
    is right-padded to the widest agent label seen so far so columns align as
    new agents appear. Non-multi-agent runs degrade to a single unlabeled lane.
    """

    def __init__(self, file: TextIO) -> None:
        self._file = file
        self._steps: dict[str, int] = {}
        self._pending: dict[tuple[str, str], int] = {}
        self._lane_width = 0
        self._has_header = False

    def _lane_key(self, event: dict[str, Any]) -> str:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        return str(
            payload.get("agent_name")
            or payload.get("agent_id")
            or event.get("agent_id")
            or ""
        )

    def _ensure_lane_width(self, lane: str) -> None:
        if len(lane) > self._lane_width:
            self._lane_width = len(lane)

    def _write(self, line: str) -> None:
        self._file.write(_ANSI_RE.sub("", line) + "\n")
        self._file.flush()

    def feed(self, event: dict[str, Any], base: str) -> None:
        ts = _format_ts(event.get("timestamp"))
        lane = self._lane_key(event)
        if lane:
            self._ensure_lane_width(lane)
        lane_col = lane.ljust(self._lane_width) if self._lane_width else ""
        prefix = f"{ts}  {lane_col}  " if lane_col else f"{ts}  "
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}

        if base == "node.started":
            self._steps[lane] = self._steps.get(lane, 0) + 1
            step = self._steps[lane]
            node_id = str(payload.get("node_id") or "?")
            self._pending[(lane, node_id)] = step
            self._write(f"{prefix}[{step:>3}] ▶ {node_id}{_row_suffix(payload)}")
        elif base == "node.completed":
            node_id = str(payload.get("node_id") or "?")
            step = self._pending.pop((lane, node_id), self._steps.get(lane, 0))
            err = payload.get("error")
            if err:
                self._write(
                    f"{prefix}[{step:>3}] ✗ {node_id}{_row_suffix(payload)}  — {_format_error(err)}"
                )
            else:
                self._write(f"{prefix}[{step:>3}] ✓ {node_id}{_row_suffix(payload)}")
        elif base == "run.started":
            self._write(f"{prefix}▶ run.started")
        elif base == "run.completed":
            self._write(f"{prefix}✔ run.completed")
        elif base == "run.failed":
            err = _format_error(payload.get("error"))
            tail = f": {err}" if err else ""
            self._write(f"{prefix}✗ run.failed{tail}")
        elif base == "run.cancelled":
            self._write(f"{prefix}✗ run.cancelled")
        elif base == "run.interrupted":
            reason = payload.get("reason") or ""
            self._write(f"{prefix}⚠ run.interrupted: {reason}")


def _print_runs_table(runs: list[dict[str, Any]]) -> int:
    if not runs:
        print("(no runs)")
        return 0
    rows: list[tuple[str, str, str, str]] = []
    for r in runs:
        run_id = str(r.get("run_id") or "")
        status = str(r.get("status") or "?")
        started = _format_ts(r.get("started_at")) if r.get("started_at") else "—"
        ended = _format_ts(r.get("ended_at")) if r.get("ended_at") else "—"
        rows.append((run_id, status, started, ended))
    rid_w = max(len(r[0]) for r in rows)
    st_w = max(len(r[1]) for r in rows)
    print(_c(f"RUNS  ({len(rows)})", "bold"))
    print()
    for run_id, status, started, ended in rows:
        color = STATUS_COLORS.get(status, "dim")
        bullet = _c("●", color)
        gap = " → "
        log_path = _log_path_for(run_id)
        log_hint = ""
        if log_path.exists():
            log_hint = "  " + _c("log:", "dim") + " " + _link(str(log_path), log_path.as_uri())
        print(
            f"  {bullet}  "
            f"{_c(run_id.ljust(rid_w), 'bold')}  "
            f"{_c(status.ljust(st_w), color)}  "
            f"{_c(started + gap + ended, 'dim')}"
            f"{log_hint}"
        )
    return 0


def _spawn_log_writer(
    base_url: str,
    run_id: str,
    log_path: Path,
    swimlane_log_path: Path,
) -> None:
    """Spawn a detached child that streams SSE for run_id and writes both logs.

    Survives parent exit (start_new_session=True), no terminal I/O (DEVNULL).
    Uses sys.executable so the child runs in the same venv as the parent.
    """
    env = os.environ.copy()
    env["GRAPH_AGENT_API_URL"] = base_url
    env["NO_COLOR"] = "1"  # belt-and-braces; child also passes to_stdout=False
    cmd = [
        sys.executable,
        "-m",
        "graph_agent.cli",
        "_render",
        run_id,
        str(log_path),
        str(swimlane_log_path),
    ]
    subprocess.Popen(
        cmd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )


def _cmd_render(rest: list[str], base_url: str) -> int:
    """Internal: stream SSE for a run and write both formatted logs.

    Invoked by `_spawn_log_writer` in a detached child process.
    Not a public subcommand.
    """
    if len(rest) != 3:
        return 2
    run_id, log_path_str, swim_path_str = rest[0], rest[1], rest[2]
    log_path = Path(log_path_str)
    swim_path = Path(swim_path_str)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    swim_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as fh, open(swim_path, "a", encoding="utf-8") as swim:
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            return _stream_events(client, run_id, file=fh, swimlane_file=swim, to_stdout=False)


def _cmd_view(rest: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="agents view",
        description="Tail the log file for a run (defaults to the most recent).",
    )
    parser.add_argument(
        "run_id",
        nargs="?",
        default=None,
        help="Run id. Omit to tail the most recent log in ~/.agents/runs/.",
    )
    parser.add_argument(
        "--swimlane",
        dest="swimlane",
        action="store_true",
        help="Tail the per-agent swimlane log instead of the raw log.",
    )
    args = parser.parse_args(rest)

    path_for = _swimlane_log_path_for if args.swimlane else _log_path_for
    if args.run_id:
        log_path = path_for(args.run_id)
        if not log_path.exists():
            raise SystemExit(f"error: no log file for run '{args.run_id}' at {log_path}")
    else:
        if not LOG_DIR.exists():
            raise SystemExit(f"error: no log directory at {LOG_DIR}; launch a run first")
        suffix = ".swimlane.log" if args.swimlane else ".log"
        candidates = [
            p for p in LOG_DIR.glob(f"*{suffix}")
            if args.swimlane or not p.name.endswith(".swimlane.log")
        ]
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            raise SystemExit(f"error: no logs found in {LOG_DIR}; launch a run first")
        log_path = candidates[0]
        print(_c(f"# tailing latest: {log_path.name}", "dim"), flush=True)

    tail_bin = shutil.which("tail")
    if tail_bin is None:
        raise SystemExit("error: 'tail' not found on PATH")
    os.execvp(tail_bin, ["tail", "-f", str(log_path)])


def _cmd_kill(base_url: str) -> int:
    """Soft-cancel every active run on the backend (POST /api/runtime/stop)."""
    with httpx.Client(base_url=base_url, timeout=10.0) as client:
        try:
            resp = client.post("/api/runtime/stop")
        except httpx.HTTPError as exc:
            raise SystemExit(f"error: cannot reach backend at {base_url}: {exc}")
        if resp.status_code != 200:
            raise SystemExit(f"error: stop failed: {_detail(resp)}")
        body = resp.json() if resp.text else {}
        stopping = body.get("stopping_runs") or []
        if not stopping:
            stopping = [{"run_id": rid} for rid in (body.get("stopping_run_ids") or [])]
        zombies = body.get("reaped_zombie_runs") or []
        if not zombies:
            zombies = [{"run_id": rid} for rid in (body.get("reaped_zombie_run_ids") or [])]
    count = len(stopping)
    zombie_count = len(zombies)
    if count == 0 and zombie_count == 0:
        print(_c("(no active runs to stop)", "dim"))
        return 0
    if count:
        plural = "s" if count != 1 else ""
        print(_c(f"Stopping {count} run{plural}:", "bold", "magenta"))
        for run in stopping:
            _print_kill_row(run, bullet_color="magenta", bullet_char="●")
    if zombie_count:
        if count:
            print()
        plural = "s" if zombie_count != 1 else ""
        print(_c(f"Reaped {zombie_count} stale run{plural}:", "bold", "yellow"))
        print(_c("(zombie rows from a previous backend — heartbeat expired)", "dim"))
        for run in zombies:
            _print_kill_row(run, bullet_color="yellow", bullet_char="○")
    return 0


def _print_kill_row(run: dict[str, Any], *, bullet_color: str, bullet_char: str) -> None:
    run_id = str(run.get("run_id") or "")
    graph_label = run.get("graph_label") or run.get("graph_id") or "?"
    agent_name = run.get("agent_name") or run.get("agent_id")
    parent = run.get("parent_run_id")
    role = "agent" if agent_name else ("envelope" if parent is None and run.get("graph_id") else "run")
    head = f"  {_c(bullet_char, bullet_color)}  {_c(str(graph_label), 'bold', 'cyan')}"
    if agent_name:
        head += f"  {_c('·', 'dim')} {_c('agent:', 'dim')} {_c(str(agent_name), 'green')}"
    elif parent:
        head += f"  {_c('·', 'dim')} {_c('parent run', 'dim')}"
    else:
        head += f"  {_c('·', 'dim')} {_c(role, 'dim')}"
    print(head)
    print(f"     {_c(run_id, 'dim')}")


def _cmd_runs(rest: list[str], base_url: str) -> int:
    parser = argparse.ArgumentParser(prog="agents runs", description="List recent runs for a graph.")
    parser.add_argument("--group", dest="graph", required=True, help="graph_id or display name.")
    parser.add_argument("--Limit", dest="limit", type=int, default=20)
    args = parser.parse_args(rest)
    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        graph_id, _doc = _resolve_graph(client, args.graph)
        resp = client.get(f"/api/graphs/{graph_id}/runs", params={"limit": args.limit})
        if resp.status_code != 200:
            raise SystemExit(f"error: failed to list runs: {_detail(resp)}")
        runs = resp.json().get("runs") or []
        return _print_runs_table(runs)


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    base_url = os.environ.get("GRAPH_AGENT_API_URL", DEFAULT_API_URL).rstrip("/")

    if argv and argv[0] == "runs":
        return _cmd_runs(argv[1:], base_url)
    if argv and argv[0] == "view":
        return _cmd_view(argv[1:])
    if argv and argv[0] == "_render":
        return _cmd_render(argv[1:], base_url)

    parser = argparse.ArgumentParser(
        prog="agents",
        description="Run an agent workflow against a running agents backend.",
    )
    parser.add_argument(
        "--group",
        dest="graph",
        default=None,
        help="graph_id (slug) or display name. Omit to list available graphs and exit.",
    )
    parser.add_argument(
        "--Agent",
        dest="agent",
        default=None,
        help="agent_id or display name within a test_environment graph (rejected for single graphs)",
    )
    parser.add_argument(
        "--Input",
        dest="input",
        default=None,
        help='JSON payload for the start node, or @path/to/file.json. Omit to send null.',
    )
    parser.add_argument(
        "--Follow",
        dest="follow",
        action="store_true",
        help="Stream events to stdout until the run terminates. Default: launch in background and exit.",
    )
    parser.add_argument(
        "--kill",
        dest="kill",
        action="store_true",
        help="Soft-cancel every active run on the backend, then exit. Takes no other arguments.",
    )
    args = parser.parse_args(argv)

    if args.kill:
        if args.graph is not None or args.agent is not None or args.input is not None or args.follow:
            raise SystemExit("error: --kill takes no other arguments")
        return _cmd_kill(base_url)

    if args.graph is None:
        if args.agent is not None or args.input is not None or args.follow:
            raise SystemExit("error: --Agent, --Input, and --Follow require --group")
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            return _list_graphs(client)

    input_payload = _parse_input(args.input)

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        graph_id, graph_doc = _resolve_graph(client, args.graph)
        agent_ids = _validate_agent_flag(graph_doc, graph_id, args.agent)

        body: dict[str, Any] = {"input": input_payload}
        if agent_ids is not None:
            body["agent_ids"] = agent_ids

        run_resp = client.post(f"/api/graphs/{graph_id}/runs", json=body)
        if run_resp.status_code != 200:
            detail = _detail(run_resp)
            if "requires a listener session" in detail:
                raise SystemExit(
                    f"error: graph '{graph_id}' uses a listener-mode start node "
                    "(webhook/cron/discord) — cannot be launched with --Input"
                )
            raise SystemExit(f"error: starting run failed: {detail}")

        run_id = run_resp.json().get("run_id")
        if not run_id:
            raise SystemExit("error: backend did not return a run_id")

        graph_label = _label(graph_doc, graph_id)
        agent_label: str | None = None
        if agent_ids:
            agents_list = graph_doc.get("agents") if isinstance(graph_doc.get("agents"), list) else []
            for a in agents_list:
                if isinstance(a, dict) and str(a.get("agent_id") or "") == agent_ids[0]:
                    agent_label = _label(a, agent_ids[0])
                    break
            if agent_label is None:
                agent_label = agent_ids[0]

        log_path = _log_path_for(run_id)
        swim_path = _swimlane_log_path_for(run_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        # Truncate any previous file at these paths (run_ids are unique, so this is just hygiene).
        with open(log_path, "w", encoding="utf-8") as fh:
            _print_launch_header(graph_label, agent_label, run_id, file=fh, to_stdout=False)
        with open(swim_path, "w", encoding="utf-8") as swim_fh:
            _print_launch_header(graph_label, agent_label, run_id, file=swim_fh, to_stdout=False)

    # Header to stdout (always, matching the prior UX).
    _print_launch_header(graph_label, agent_label, run_id, file=None, to_stdout=True)

    if args.follow:
        print(flush=True)
        with open(log_path, "a", encoding="utf-8") as fh, open(swim_path, "a", encoding="utf-8") as swim_fh, httpx.Client(base_url=base_url, timeout=30.0) as client:
            return _stream_events(client, run_id, file=fh, swimlane_file=swim_fh, to_stdout=True)

    _spawn_log_writer(base_url, run_id, log_path, swim_path)
    log_link = _link(str(log_path), log_path.as_uri())
    swim_link = _link(str(swim_path), swim_path.as_uri())
    print(f"{' ' * 8}  " + _c("log     ", "dim") + f" {log_link}", flush=True)
    print(f"{' ' * 8}  " + _c("swimlane", "dim") + f" {swim_link}", flush=True)
    print(f"{' ' * 8}  " + _c(f"agents view {run_id}", "dim"), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
