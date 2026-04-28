---
name: python-script-runner
description: "Authoring contract for scripts run by the python_script_runner data node (provider id `core.python_script_runner`). Use when writing, reviewing, or debugging a `.py` file uploaded as a graph project file and executed via that node."
---

# python_script_runner script contract

The runner spawns a fresh Python subprocess with the backend's `sys.executable`,
loads the script via `importlib`, and calls a top-level `run()` function. The
node's success/failure verdict is derived from `run()`'s return value and the
subprocess exit code — see `src/graph_agent/runtime/python_script_runner.py`
and `docs/python-script-runner.md` for the full implementation and envelope
shape.

## Required script structure

Every script MUST define a module-level callable named `run` that returns a
`bool`:

```python
def run() -> bool:
    ...
    return True   # success — node reports success, exit 0
```

| Return / behavior              | Node verdict                                | Exit code |
|--------------------------------|---------------------------------------------|-----------|
| Truthy (`True`, non-empty, …)  | success                                     | 0         |
| Falsy (`False`, `None`, `0`, …)| failure — `python_script_run_returned_false`| 1         |
| Raises any exception           | failure — `python_script_unhandled_exception` (traceback on stderr) | 2 |
| No `run` symbol                | failure — `python_script_missing_run_symbol`| 3         |
| Import / syntax error          | failure — `python_script_bootstrap_error`   | 4         |
| Wall-clock > timeout (default 30 s) | failure — `python_script_timeout` (process killed) | killed |

## The "indicative bool" rule (do not skip)

`run()`'s return value is the only signal the runtime has about whether the
script actually accomplished its job. **Do not unconditionally `return True`
at the end of the function** — it makes the node always report success,
including when zero rows were written, every API call failed, or every batch
was a no-op.

Bad:

```python
def run() -> bool:
    do_phase_a()         # swallows its own errors
    do_phase_b()         # also swallows its own errors
    return True          # always success, even when nothing happened
```

Good — track real outcomes and reflect them in the return value:

```python
def run() -> bool:
    outcomes = []
    outcomes.append(do_phase_a())   # returns True only if it wrote something
    outcomes.append(do_phase_b())
    return all(outcomes)            # or `any(...)` if partial counts as success
```

If "success" is fuzzy (e.g. partial writes), pick a rule and document it in a
short comment on the `return` line. The script's stdout summaries are useful
context but they are **not** the verdict — the bool is.

## Reading upstream input

Stdin always contains valid JSON. If the node has an `input_binding`, stdin
gets the resolved upstream payload (the payload, not the full envelope). If
not, stdin gets `{}`.

```python
import json, sys

def run() -> bool:
    payload = json.load(sys.stdin)   # always JSON — never read raw text
    rows = payload.get("rows") or []
    if not rows:
        print("no rows on stdin", file=sys.stderr)
        return False
    ...
    return True
```

Defensive parsing tips:

- Accept multiple shapes if the script is reused across upstream nodes (list,
  `{"rows": [...]}`, single-row dict). See `_coerce_rows` in
  `scripts/outlook_supabase_sync.py` for one pattern.
- Treat empty input as a real condition (return `False`, or return `True` if
  no-op-on-empty is the documented contract).

## stdout vs. stderr

Both streams are captured (256 KB cap each, with a truncation marker) and
attached to the data envelope as `stdout` / `stderr`. Convention used in this
repo:

- `stdout` — human-readable summaries the operator wants to see (`print(...)`
  or a small `_summary()` helper).
- `stderr` — diagnostics, partial-failure logs, tracebacks. Print with
  `print(..., file=sys.stderr)` or a `_log()` helper.

Anything you print after the cap is dropped — large dumps belong in Supabase
or a project file, not in stdout.

## Environment, dependencies, and side effects

- The script inherits the backend process environment (`os.environ`), so any
  config it needs (`SUPABASE_*`, `OUTLOOK_*`, etc.) must already be exported
  there or read from `<repo>/.env` via `run.py`.
- It runs against the backend venv — no isolation. Only `import` packages the
  venv already has.
- Full filesystem and network privileges of the backend user. Treat scripts
  as **trusted developer code**, not as a sandbox for untrusted input.
- Side effects survive a timeout. If the subprocess is killed mid-write, the
  partial state persists; design idempotently when possible.

## Timeouts and output limits

- Default wall-clock timeout is 30 s; configurable per node via
  `timeout_seconds` (must be > 0). Long-running jobs should be split, batched,
  or moved out of the runner.
- stdout and stderr each truncate at 256 KB with a `[...output truncated]`
  marker.

## Output envelope (what the node emits)

```json
{
  "success": true,
  "exit_code": 0,
  "stdout": "...",
  "stderr": "",
  "duration_ms": 142,
  "timed_out": false,
  "script_path": "/abs/path/to/your-script.py"
}
```

On failure the envelope's `errors` array carries one entry whose `type` is one
of: `python_script_run_returned_false`, `python_script_unhandled_exception`,
`python_script_missing_run_symbol`, `python_script_bootstrap_error`,
`python_script_timeout`, `python_script_not_found`,
`python_script_missing_path`, `python_script_spawn_failed`.

## Authoring checklist

Before uploading the script as a graph project file:

- [ ] Module defines a top-level `def run() -> bool:`.
- [ ] Return value reflects whether real work happened, not just "no exception
      raised".
- [ ] Reads `json.load(sys.stdin)` if it expects upstream payload; tolerates
      `{}` otherwise.
- [ ] Errors at boundaries (HTTP, Supabase, filesystem) propagate into the
      bool — either by raising, or by being aggregated and folded into the
      return value. Don't swallow + return True.
- [ ] stdout used for summary lines; stderr used for diagnostics.
- [ ] Script is idempotent or guarded against partial-write damage on timeout.
- [ ] Imports are available in the backend venv; no expectation of
      sandboxing.
- [ ] If the work can exceed 30 s, raise `timeout_seconds` on the node or
      split the work.

## Reference implementations in this repo

- `scripts/outlook_supabase_sync.py` — dual-mode (CLI + `run()`); demonstrates
  stdin row coercion, env-var toggles (`OUTLOOK_SYNC_*`), and `_log()` /
  `_summary()` split. Note: its `run()` currently always returns `True` — a
  known instance of the anti-pattern this skill warns about.
