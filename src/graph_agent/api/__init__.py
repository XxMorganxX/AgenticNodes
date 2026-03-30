try:
    from graph_agent.api.app import app
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency during tests
    app = None

__all__ = ["app"]
