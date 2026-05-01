from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from threading import Event, Lock, Thread
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


LOGGER = logging.getLogger(__name__)

CRON_START_PROVIDER_ID = "start.cron_schedule"
UTC = timezone.utc


@dataclass(frozen=True)
class CronSchedule:
    graph_id: str
    cron_expression: str
    timezone: str
    prompt: str
    enabled: bool = True

    @property
    def fingerprint(self) -> tuple[str, str, str, bool]:
        return (self.cron_expression, self.timezone, self.prompt, self.enabled)


def normalize_cron_schedule_payload(
    schedule: CronSchedule,
    *,
    scheduled_for: datetime,
    fired_at: datetime | None = None,
) -> dict[str, Any]:
    fired_at = fired_at or datetime.now(UTC)
    return {
        "source": "cron_schedule",
        "graph_id": schedule.graph_id,
        "prompt": schedule.prompt,
        "cron_expression": schedule.cron_expression,
        "timezone": schedule.timezone,
        "scheduled_for": scheduled_for.astimezone(UTC).isoformat(),
        "fired_at": fired_at.astimezone(UTC).isoformat(),
    }


def next_cron_fire_after(expression: str, timezone_name: str, after: datetime | None = None) -> datetime:
    """Return the next UTC fire time for a standard five-field cron expression."""
    schedule = _ParsedCron.parse(expression)
    timezone = _load_timezone(timezone_name)
    after_utc = (after or datetime.now(UTC)).astimezone(UTC)
    local = after_utc.astimezone(timezone)
    candidate = local.replace(second=0, microsecond=0) + timedelta(minutes=1)
    # Minute-by-minute is simple and sufficient for editor-driven schedules.
    # Bound the scan so invalid impossible dates fail clearly.
    for _ in range(366 * 24 * 60):
        if schedule.matches(candidate):
            return candidate.astimezone(UTC)
        candidate += timedelta(minutes=1)
    raise ValueError(f"Cron expression '{expression}' has no matching time in the next year.")


class CronTriggerService:
    name = "cron"

    def __init__(
        self,
        schedule_provider: Callable[[str], CronSchedule | None],
        fire_callback: Callable[[str, dict[str, Any]], None],
        *,
        poll_interval_seconds: float = 15.0,
    ) -> None:
        self._schedule_provider = schedule_provider
        self._fire_callback = fire_callback
        self._poll_interval_seconds = max(0.5, float(poll_interval_seconds))
        self._active_graph_ids: set[str] = set()
        self._next_fire_at: dict[str, datetime] = {}
        self._fingerprints: dict[str, tuple[str, str, str, bool]] = {}
        self._lock = Lock()
        self._stop_event = Event()
        self._thread: Thread | None = None

    def activate(self, graph_id: str) -> None:
        normalized = str(graph_id or "").strip()
        if not normalized:
            return
        schedule = self._load_schedule_or_raise(normalized)
        next_fire_at = next_cron_fire_after(schedule.cron_expression, schedule.timezone)
        with self._lock:
            self._active_graph_ids.add(normalized)
            self._next_fire_at[normalized] = next_fire_at
            self._fingerprints[normalized] = schedule.fingerprint
            self._ensure_thread_locked()

    def deactivate(self, graph_id: str) -> None:
        normalized = str(graph_id or "").strip()
        with self._lock:
            self._active_graph_ids.discard(normalized)
            self._next_fire_at.pop(normalized, None)
            self._fingerprints.pop(normalized, None)

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2)
        with self._lock:
            self._active_graph_ids.clear()
            self._next_fire_at.clear()
            self._fingerprints.clear()
            self._thread = None
            self._stop_event = Event()

    def trigger_due(self, now: datetime | None = None) -> list[str]:
        """Check due schedules once and return graph ids that fired.

        The manager uses the background loop; tests can call this directly with
        a controlled clock.
        """
        now_utc = (now or datetime.now(UTC)).astimezone(UTC)
        fired_graph_ids: list[str] = []
        with self._lock:
            graph_ids = list(self._active_graph_ids)
        for graph_id in graph_ids:
            if self._trigger_graph_if_due(graph_id, now_utc):
                fired_graph_ids.append(graph_id)
        return fired_graph_ids

    def _ensure_thread_locked(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = Thread(target=self._run_loop, name="graph-agent-cron-trigger", daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        while not self._stop_event.wait(self._poll_interval_seconds):
            self.trigger_due()

    def _trigger_graph_if_due(self, graph_id: str, now_utc: datetime) -> bool:
        schedule = self._schedule_provider(graph_id)
        if schedule is None or not schedule.enabled:
            self.deactivate(graph_id)
            return False
        try:
            next_fire_at = self._next_fire_for_current_schedule(graph_id, schedule, now_utc)
        except Exception:  # noqa: BLE001
            LOGGER.exception("Cron schedule for graph %r is invalid; skipping until the listener is restarted.", graph_id)
            return False
        if next_fire_at > now_utc:
            return False
        payload = normalize_cron_schedule_payload(schedule, scheduled_for=next_fire_at, fired_at=now_utc)
        try:
            self._fire_callback(graph_id, payload)
        except Exception:  # noqa: BLE001
            LOGGER.exception("Cron trigger callback failed for graph %r.", graph_id)
        try:
            next_fire = next_cron_fire_after(schedule.cron_expression, schedule.timezone, now_utc)
        except Exception:  # noqa: BLE001
            LOGGER.exception("Cron schedule for graph %r could not compute its next fire time.", graph_id)
            return True
        with self._lock:
            if graph_id in self._active_graph_ids:
                self._next_fire_at[graph_id] = next_fire
                self._fingerprints[graph_id] = schedule.fingerprint
        return True

    def _next_fire_for_current_schedule(self, graph_id: str, schedule: CronSchedule, now_utc: datetime) -> datetime:
        with self._lock:
            current = self._next_fire_at.get(graph_id)
            fingerprint = self._fingerprints.get(graph_id)
        if current is not None and fingerprint == schedule.fingerprint:
            return current
        next_fire = next_cron_fire_after(schedule.cron_expression, schedule.timezone, now_utc)
        with self._lock:
            if graph_id in self._active_graph_ids:
                self._next_fire_at[graph_id] = next_fire
                self._fingerprints[graph_id] = schedule.fingerprint
        return next_fire

    def _load_schedule_or_raise(self, graph_id: str) -> CronSchedule:
        schedule = self._schedule_provider(graph_id)
        if schedule is None or not schedule.enabled:
            raise RuntimeError(f"No enabled cron schedule is configured for graph '{graph_id}'.")
        next_cron_fire_after(schedule.cron_expression, schedule.timezone)
        return schedule


@dataclass(frozen=True)
class _ParsedCron:
    minutes: set[int]
    hours: set[int]
    days_of_month: set[int]
    months: set[int]
    days_of_week: set[int]
    day_of_month_restricted: bool
    day_of_week_restricted: bool

    @classmethod
    def parse(cls, expression: str) -> "_ParsedCron":
        fields = str(expression or "").strip().split()
        if len(fields) != 5:
            raise ValueError("Cron expression must have five fields: minute hour day-of-month month day-of-week.")
        minute, hour, day_of_month, month, day_of_week = fields
        days_of_month, dom_restricted = _parse_cron_field(day_of_month, minimum=1, maximum=31)
        days_of_week, dow_restricted = _parse_cron_field(day_of_week, minimum=0, maximum=7, names=_DAY_NAMES)
        return cls(
            minutes=_parse_cron_field(minute, minimum=0, maximum=59)[0],
            hours=_parse_cron_field(hour, minimum=0, maximum=23)[0],
            days_of_month=days_of_month,
            months=_parse_cron_field(month, minimum=1, maximum=12, names=_MONTH_NAMES)[0],
            days_of_week={0 if value == 7 else value for value in days_of_week},
            day_of_month_restricted=dom_restricted,
            day_of_week_restricted=dow_restricted,
        )

    def matches(self, value: datetime) -> bool:
        cron_weekday = (value.weekday() + 1) % 7
        day_of_month_matches = value.day in self.days_of_month
        day_of_week_matches = cron_weekday in self.days_of_week
        if self.day_of_month_restricted and self.day_of_week_restricted:
            day_matches = day_of_month_matches or day_of_week_matches
        else:
            day_matches = day_of_month_matches and day_of_week_matches
        return (
            value.minute in self.minutes
            and value.hour in self.hours
            and day_matches
            and value.month in self.months
        )


_MONTH_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_DAY_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}


def _parse_cron_field(
    raw: str,
    *,
    minimum: int,
    maximum: int,
    names: Mapping[str, int] | None = None,
) -> tuple[set[int], bool]:
    text = str(raw or "").strip().lower()
    if not text:
        raise ValueError("Cron field cannot be empty.")
    values: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError(f"Invalid empty cron list item in '{raw}'.")
        values.update(_expand_cron_part(part, minimum=minimum, maximum=maximum, names=names or {}))
    return values, text != "*"


def _expand_cron_part(part: str, *, minimum: int, maximum: int, names: Mapping[str, int]) -> set[int]:
    base, step_text = part, ""
    if "/" in part:
        base, step_text = part.split("/", 1)
        if not step_text.isdigit() or int(step_text) <= 0:
            raise ValueError(f"Invalid cron step in '{part}'.")
    step = int(step_text) if step_text else 1
    if base == "*":
        start, end = minimum, maximum
    elif "-" in base:
        start_text, end_text = base.split("-", 1)
        start = _parse_cron_value(start_text, names)
        end = _parse_cron_value(end_text, names)
    else:
        start = end = _parse_cron_value(base, names)
    if start < minimum or start > maximum or end < minimum or end > maximum or start > end:
        raise ValueError(f"Cron value '{part}' is outside the allowed range {minimum}-{maximum}.")
    return set(range(start, end + 1, step))


def _parse_cron_value(raw: str, names: Mapping[str, int]) -> int:
    text = str(raw or "").strip().lower()
    if text in names:
        return names[text]
    if not text.isdigit():
        raise ValueError(f"Invalid cron value '{raw}'.")
    return int(text)


def _load_timezone(timezone_name: str) -> ZoneInfo:
    name = str(timezone_name or "UTC").strip() or "UTC"
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone '{name}'.") from exc
