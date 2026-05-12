"""macOS menu-bar status indicator for the graph-agent backend.

Polls `/api/runtime/menubar` and renders a colored LED dot plus a running-count
badge in the system status bar:

  * green LED + count when everything is healthy
  * red LED + count when the most recent terminal run failed
  * grey LED when the backend is unreachable

When AppKit is available the title is drawn as an NSAttributedString so the
dot is a real colored glyph and the integer uses a monospaced-digit semibold
font in the secondary label color. Falls back to emoji if AppKit cannot be
imported.

Launched by `run.py` on macOS when `rumps` is installed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import urllib.error
import urllib.request
import webbrowser

try:
    import rumps
except ImportError:
    sys.stderr.write(
        "status_menubar requires 'rumps'. Install with: .venv/bin/pip install rumps\n"
    )
    sys.exit(0)

try:
    from AppKit import (
        NSAttributedString,
        NSColor,
        NSFont,
        NSFontWeightSemibold,
        NSMutableAttributedString,
    )

    _APPKIT_OK = True
except ImportError:  # pragma: no cover — only on systems without pyobjc
    _APPKIT_OK = False


LED_GLYPH = "●"  # BLACK CIRCLE — solid, monochrome, looks like an LED when tinted

# Emoji fallbacks used only when AppKit is unavailable.
FALLBACK_OK = "\U0001F7E2"
FALLBACK_FAIL = "\U0001F534"
FALLBACK_UNKNOWN = "⚫"


class StatusApp(rumps.App):
    def __init__(self, api_base_url: str, frontend_url: str, poll_seconds: float) -> None:
        super().__init__(name="graph-agent", title=f"{FALLBACK_UNKNOWN} -", quit_button=None)
        self._api_base_url = api_base_url.rstrip("/")
        self._frontend_url = frontend_url.rstrip("/")
        self._poll_seconds = max(0.5, poll_seconds)
        self._last_failed_run_id: str | None = None
        self._detail_item = rumps.MenuItem("Starting up…")
        self._open_item = rumps.MenuItem("Open studio", callback=self._open_studio)
        self._quit_item = rumps.MenuItem("Quit", callback=self._quit)
        self.menu = [self._detail_item, None, self._open_item, None, self._quit_item]

        rumps.Timer(self._tick, self._poll_seconds).start()

    def _tick(self, _sender) -> None:
        threading.Thread(target=self._poll_once, daemon=True).start()

    def _poll_once(self) -> None:
        url = f"{self._api_base_url}/api/runtime/menubar"
        try:
            with urllib.request.urlopen(url, timeout=2.0) as response:
                payload = json.load(response)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            self._render(0, "unknown")
            self._detail_item.title = f"Backend unreachable: {error}"
            return

        running = int(payload.get("running") or 0)
        failed_recent = bool(payload.get("failed_recent"))
        last_failed_run_id = payload.get("last_failed_run_id")
        last_failed_at = payload.get("last_failed_at") or "unknown time"

        state = "fail" if failed_recent else "ok"
        self._render(running, state)
        if failed_recent:
            self._detail_item.title = (
                f"Last failure: {last_failed_run_id} at {last_failed_at}"
            )
        else:
            self._detail_item.title = f"{running} agent(s) running"
        self._last_failed_run_id = last_failed_run_id if failed_recent else None

    def _render(self, running: int, state: str) -> None:
        if _APPKIT_OK and self._set_attributed_title(running, state):
            return
        fallback = {
            "ok": FALLBACK_OK,
            "fail": FALLBACK_FAIL,
            "unknown": FALLBACK_UNKNOWN,
        }[state]
        suffix = "-" if state == "unknown" else str(running)
        self.title = f"{fallback} {suffix}"

    def _set_attributed_title(self, running: int, state: str) -> bool:
        try:
            status_item = self._nsapp.nsstatusitem
            button = status_item.button()
        except AttributeError:
            return False
        if button is None:
            return False

        led_color = {
            "ok": NSColor.systemGreenColor(),
            "fail": NSColor.systemRedColor(),
            "unknown": NSColor.systemGrayColor(),
        }[state]

        title = NSMutableAttributedString.alloc().init()
        title.appendAttributedString_(
            NSAttributedString.alloc().initWithString_attributes_(
                LED_GLYPH,
                {
                    "NSColor": led_color,
                    "NSFont": NSFont.boldSystemFontOfSize_(13.0),
                },
            )
        )
        title.appendAttributedString_(
            NSAttributedString.alloc().initWithString_attributes_(
                "  ",
                {"NSFont": NSFont.systemFontOfSize_(11.0)},
            )
        )
        count_text = "-" if state == "unknown" else str(running)
        title.appendAttributedString_(
            NSAttributedString.alloc().initWithString_attributes_(
                count_text,
                {
                    "NSColor": NSColor.secondaryLabelColor(),
                    "NSFont": NSFont.monospacedDigitSystemFontOfSize_weight_(
                        11.0, NSFontWeightSemibold
                    ),
                },
            )
        )
        button.setAttributedTitle_(title)
        return True

    def _open_studio(self, _sender) -> None:
        if self._frontend_url:
            webbrowser.open(self._frontend_url)

    def _quit(self, _sender) -> None:
        rumps.quit_application()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-base-url",
        default=os.environ.get("GRAPH_AGENT_API_BASE_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--frontend-url",
        default=os.environ.get("GRAPH_AGENT_FRONTEND_URL", ""),
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=float(os.environ.get("GRAPH_AGENT_MENUBAR_POLL_SECONDS", "2.0")),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    StatusApp(
        api_base_url=args.api_base_url,
        frontend_url=args.frontend_url,
        poll_seconds=args.poll_seconds,
    ).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
