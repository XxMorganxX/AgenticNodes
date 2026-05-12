# tools/

Developer-facing helpers that run **alongside** the graph-agent backend, not part of it.

## `status_menubar.py` — macOS menu bar indicator

A tiny menu bar app that shows how many runs are currently executing and whether the most recent terminal run failed.

The title is drawn as an `NSAttributedString`: a real colored LED dot (`●`) followed by the run count in a monospaced-digit semibold font in the secondary label color, so the count reads as a subordinate badge rather than part of the indicator.

- green LED + `N` — `N` runs in flight, no recent failures
- red LED + `N` — `N` runs in flight, the most recent terminal run failed (clears on the next non-failing terminal run)
- grey LED + `-` — backend unreachable

If AppKit can't be imported the app falls back to emoji (`🟢` / `🔴` / `⚫`).

It polls `GET /api/runtime/menubar` every 2 seconds. macOS only — `NSStatusBar` needs to own the main thread, so this runs as a separate child process of `run.py`, never embedded in uvicorn.

### Setup

```bash
.venv/bin/pip install -e ".[menubar]"
```

`run.py` auto-launches it when:

- platform is macOS, AND
- `rumps` is importable in the backend Python, AND
- `GRAPH_AGENT_MENUBAR` is not set to `0`/`false`/`no`

Disable for a session with `GRAPH_AGENT_MENUBAR=0 python3 run.py`.

### Standalone

```bash
.venv/bin/python tools/status_menubar.py \
  --api-base-url http://127.0.0.1:8000 \
  --frontend-url http://127.0.0.1:5173
```
