"""A misbehaving event listener must never crash a run.

Long-running agent runs are especially exposed: a listener can fail mid-run
after operating successfully for hours. The engine must isolate that failure
and keep delivering events to other listeners.
"""
from __future__ import annotations

import logging
import unittest
from unittest.mock import MagicMock

from graph_agent.runtime.core import RunState
from graph_agent.runtime.engine import GraphRuntime


def _make_runtime() -> tuple[GraphRuntime, RunState]:
    runtime = GraphRuntime.__new__(GraphRuntime)
    runtime.services = None
    runtime.max_steps = 0
    runtime.max_visits_per_node = 0
    runtime.event_listeners = []
    runtime.cancel_requested = lambda: False
    state = RunState(graph_id="test-graph", input_payload="hi")
    return runtime, state


class ListenerIsolationTests(unittest.TestCase):
    def test_failing_listener_does_not_propagate(self) -> None:
        runtime, state = _make_runtime()

        def boom(_event: object) -> None:
            raise RuntimeError("listener exploded")

        runtime.add_event_listener(boom)
        with self.assertLogs("graph_agent.runtime.engine", level=logging.WARNING) as captured:
            event = runtime.emit(state, "node.started", "ok", {"node_id": "start"})
        self.assertEqual(event.event_type, "node.started")
        self.assertTrue(any("listener" in line and "failed" in line for line in captured.output))

    def test_other_listeners_still_receive_event_after_failure(self) -> None:
        runtime, state = _make_runtime()
        good = MagicMock()

        def boom(_event: object) -> None:
            raise RuntimeError("boom")

        runtime.add_event_listener(boom)
        runtime.add_event_listener(good)
        with self.assertLogs("graph_agent.runtime.engine", level=logging.WARNING):
            runtime.emit(state, "run.started", "ok", {})
        good.assert_called_once()


if __name__ == "__main__":
    unittest.main()
