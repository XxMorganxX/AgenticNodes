from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from graph_agent.runtime.core import utc_now_iso


class RunLogStore:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or Path(__file__).resolve().parents[3] / ".logs" / "runs"

    def initialize_run(self, state: Mapping[str, Any]) -> None:
        run_id = str(state["run_id"])
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            self._write_json(
                manifest_path,
                {
                    "run_id": run_id,
                    "graph_id": state.get("graph_id"),
                    "agent_id": state.get("agent_id"),
                    "agent_name": state.get("agent_name"),
                    "parent_run_id": state.get("parent_run_id"),
                    "created_at": utc_now_iso(),
                },
            )
        self.write_state(run_id, state)

    def append_event(self, run_id: str, event: Mapping[str, Any]) -> None:
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        with (run_dir / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True))
            handle.write("\n")

    def write_state(self, run_id: str, state: Mapping[str, Any]) -> None:
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        self._write_json(run_dir / "state.json", state)

    def _run_dir(self, run_id: str) -> Path:
        return self.root / run_id

    def _write_json(self, path: Path, payload: Mapping[str, Any]) -> None:
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(path)
