# Python Script Runner Node

A data node that runs a user-supplied `.py` file in a subprocess and reports
whether it succeeded. It is intended for one-off, graph-local scripts — quick
data backfills, cleanup passes, ad-hoc repairs — where writing a full reusable
node would be overkill.

**Provider id:** `core.python_script_runner`
**Category:** `data` (produces a `data_envelope`)

## How the node runs your script

1. The manager pre-resolves your selected project file (`script_file_id`) into
   an absolute `script_path` on the graph just before the run starts.
2. At execution time the node spawns a fresh Python subprocess using the same
   interpreter as the backend (`sys.executable`).
3. The subprocess is handed a tiny bootstrap harness via `python -c "..."`.
   The harness:
   - Loads your script by path with `importlib.util.spec_from_file_location`.
   - Calls `run()` on the loaded module.
   - Exits with a code that tells the runtime what happened (see below).
4. The resolved upstream payload (if the node has an `input_binding`) is
   serialized to JSON and written to the subprocess's stdin, so your script
   can read it via `json.load(sys.stdin)`. If there is no upstream binding,
   stdin receives `{}`.
5. stdout and stderr are captured in full (up to 256 KB each) and returned in
   the node's output envelope.
6. A configurable wall-clock timeout (default 30 s) is enforced. On timeout
   the subprocess is killed and the node reports failure.

## Required script structure

Your script **must** define a top-level callable named `run` that returns a
boolean:

```python
def run() -> bool:
    # ... do work ...
    return True   # success
```

| You do | Node reports | Exit code |
|---|---|---|
| `return True` (or any truthy value) | `success` | `0` |
| `return False` (or any falsy value) | `failed` — `run() returned False` | `1` |
| Raise any exception | `failed` — traceback on stderr | `2` |
| Don't define `run` at all | `failed` — missing `run` symbol | `3` |
| Import or syntax error before `run` is reached | `failed` — bootstrap error | `4` |

You can `import` anything the backend venv already has available. The script
runs with the full filesystem and network privileges of the backend process —
treat scripts as **trusted developer code**, not as a sandbox for untrusted
input.

## Reading the upstream payload

Stdin always contains valid JSON. Parse it at the top of `run()`:

```python
import json
import sys

def run() -> bool:
    payload = json.load(sys.stdin)  # {} if no upstream binding
    ...
    return True
```

If the node has an `input_binding` pointing at an upstream envelope, the
resolved payload (not the full envelope) is what you get.

## Stdout, stderr, and output envelope

`print(...)` and uncaught tracebacks land in the envelope's `stdout` /
`stderr` fields, so use them freely for debugging. The envelope payload is:

```json
{
  "success": true,
  "exit_code": 0,
  "stdout": "...captured stdout...",
  "stderr": "",
  "duration_ms": 142,
  "timed_out": false,
  "script_path": "/abs/path/to/your-script.py"
}
```

On failure the envelope's `errors` array contains one entry describing the
failure type (`python_script_run_returned_false`,
`python_script_unhandled_exception`, `python_script_missing_run_symbol`,
`python_script_bootstrap_error`, or `python_script_timeout`).

## Examples

**Minimal always-succeeds script:**

```python
def run() -> bool:
    return True
```

**Handled-failure script:**

```python
import os

def run() -> bool:
    if not os.environ.get("EXPECTED_KEY"):
        print("EXPECTED_KEY is not set", flush=True)
        return False
    return True
```

**Input-aware script:**

```python
import json
import sys

def run() -> bool:
    payload = json.load(sys.stdin)
    emails = payload.get("emails") or []
    if not emails:
        print("No emails in upstream payload", flush=True)
        return False
    print(f"Processing {len(emails)} email(s)", flush=True)
    # ... do work ...
    return True
```

## Configuration

| Field | Purpose |
|---|---|
| `script_file_id` | ID of a `.py` project file uploaded to this graph. Required. |
| `timeout_seconds` | Wall-clock limit in seconds. Default `30`. Must be > 0. |
| `input_binding` | Optional upstream binding — its resolved payload becomes stdin. |

The node resolves `script_path` and `script_file_name` automatically from
`script_file_id` each time the graph runs; you don't set these manually.

## Safety notes

- Scripts execute with the same OS user and permissions as the backend. They
  can read/write anywhere the backend can, make outbound network requests,
  and modify local Supabase state.
- There is no dependency isolation. Your script runs against whatever Python
  version and packages the backend venv has installed.
- stdout and stderr are truncated at 256 KB each. If your script prints more
  than that, the tail is dropped.
- A killed subprocess cannot be "rolled back" — if your script makes external
  side effects before timing out, those persist.
