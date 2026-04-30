from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class TriggerService(Protocol):
    """Lifecycle contract every persistent listener follows.

    A trigger service owns whatever long-lived resources are needed to fire
    `manager._start_child_run(...)` when an external event arrives. Listening
    is now session-scoped: the manager calls `activate(graph_id)` when a
    listening session for that graph begins and `deactivate(graph_id)` when
    the session ends. The underlying transport (Discord socket, webhook
    binding, ...) only stays alive while at least one graph is active.

    `stop()` is reserved for full server shutdown.
    """

    name: str

    def activate(self, graph_id: str) -> None: ...

    def deactivate(self, graph_id: str) -> None: ...

    def stop(self) -> None: ...
