"""Minimal CLI for launching graph runs against a running backend.

Usage:
    agents                                                   # list graphs + agents
    agents --Graph <graph_id> [--Agent <agent_id>] [--Input <json|@file>]

Talks to the FastAPI server at $GRAPH_AGENT_API_URL (default http://127.0.0.1:8000).
With --Graph, prints each runtime event as one compact JSON line on stdout and exits
non-zero when the run terminates in failed/cancelled/interrupted state. Without
--Graph, prints a human-readable listing of available graphs and (for
test_environments) their agent_ids, then exits.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import httpx


DEFAULT_API_URL = "http://127.0.0.1:8000"
TERMINAL_FAILURE_EVENTS = {"run.failed", "run.cancelled", "run.interrupted"}


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

    available = [str(a.get("agent_id")) for a in agents if isinstance(a, dict) and a.get("agent_id")]
    if agent is None:
        listing = ", ".join(available) if available else "(none defined)"
        raise SystemExit(
            f"error: --Agent is required for test_environment '{graph_id}'. Available agents: {listing}"
        )
    if agent not in available:
        listing = ", ".join(available) if available else "(none defined)"
        raise SystemExit(
            f"error: agent '{agent}' not found in '{graph_id}'. Available agents: {listing}"
        )
    return [agent]


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
    rows = sorted(graphs, key=lambda g: str(g.get("graph_id") or ""))
    width = max(len(str(g.get("graph_id") or "")) for g in rows)
    print("Available graphs:")
    for g in rows:
        graph_id = str(g.get("graph_id") or "")
        name = str(g.get("name") or "")
        graph_type = str(g.get("graph_type") or "graph")
        print(f"  {graph_id.ljust(width)}  ({graph_type})  {name}")
        if graph_type == "test_environment":
            agents = g.get("agents") if isinstance(g.get("agents"), list) else []
            agent_ids = [str(a.get("agent_id")) for a in agents if isinstance(a, dict) and a.get("agent_id")]
            if agent_ids:
                print(f"  {' ' * width}    agents: {', '.join(agent_ids)}")
    return 0


def _stream_events(client: httpx.Client, run_id: str) -> int:
    final_event_type: str | None = None
    with client.stream("GET", f"/api/runs/{run_id}/events", timeout=None) as response:
        if response.status_code != 200:
            response.read()
            raise SystemExit(f"error: SSE stream failed: {_detail(response)}")
        for line in response.iter_lines():
            if not line or not line.startswith("data:"):
                continue
            payload = line[5:].lstrip()
            try:
                event = json.loads(payload)
            except json.JSONDecodeError:
                continue
            print(json.dumps(event, separators=(",", ":")), flush=True)
            event_type = event.get("event_type") if isinstance(event, dict) else None
            if isinstance(event_type, str):
                final_event_type = event_type
    return 1 if final_event_type in TERMINAL_FAILURE_EVENTS else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="agents",
        description="Run an agent workflow against a running agents backend.",
    )
    parser.add_argument(
        "--Graph",
        dest="graph",
        default=None,
        help="graph_id (slug). Omit to list available graphs and exit.",
    )
    parser.add_argument(
        "--Agent",
        dest="agent",
        default=None,
        help="agent_id within a test_environment graph (rejected for single graphs)",
    )
    parser.add_argument(
        "--Input",
        dest="input",
        default=None,
        help='JSON payload for the start node, or @path/to/file.json. Omit to send null.',
    )
    args = parser.parse_args(argv)

    base_url = os.environ.get("GRAPH_AGENT_API_URL", DEFAULT_API_URL).rstrip("/")

    if args.graph is None:
        if args.agent is not None or args.input is not None:
            raise SystemExit("error: --Agent and --Input require --Graph")
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            return _list_graphs(client)

    input_payload = _parse_input(args.input)

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        try:
            graph_resp = client.get(f"/api/graphs/{args.graph}")
        except httpx.HTTPError as exc:
            raise SystemExit(f"error: cannot reach backend at {base_url}: {exc}")
        if graph_resp.status_code == 404:
            raise SystemExit(f"error: unknown graph '{args.graph}'")
        if graph_resp.status_code != 200:
            raise SystemExit(f"error: failed to load graph '{args.graph}': {_detail(graph_resp)}")

        graph_doc = graph_resp.json()
        agent_ids = _validate_agent_flag(graph_doc, args.graph, args.agent)

        body: dict[str, Any] = {"input": input_payload}
        if agent_ids is not None:
            body["agent_ids"] = agent_ids

        run_resp = client.post(f"/api/graphs/{args.graph}/runs", json=body)
        if run_resp.status_code != 200:
            detail = _detail(run_resp)
            if "requires a listener session" in detail:
                raise SystemExit(
                    f"error: graph '{args.graph}' uses a listener-mode start node "
                    "(webhook/cron/discord) — cannot be launched with --Input"
                )
            raise SystemExit(f"error: starting run failed: {detail}")

        run_id = run_resp.json().get("run_id")
        if not run_id:
            raise SystemExit("error: backend did not return a run_id")
        print(json.dumps({"event_type": "cli.run_started", "run_id": run_id}, separators=(",", ":")), flush=True)

        return _stream_events(client, run_id)


if __name__ == "__main__":
    sys.exit(main())
