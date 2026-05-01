import { useEffect, useState } from "react";
import type { MouseEvent } from "react";

type CronScheduleMode = "every_15_minutes" | "hourly" | "daily" | "weekdays" | "weekly" | "monthly" | "custom";

type CronScheduleModalProps = {
  cronExpression: string;
  timezone: string;
  prompt: string;
  onChangeCronExpression: (value: string) => void;
  onChangeTimezone: (value: string) => void;
  onChangePrompt: (value: string) => void;
  onClose: () => void;
};

const TIMEZONE_SUGGESTIONS = ["UTC", "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "Europe/London"];
const WEEKDAY_OPTIONS = [
  { value: "mon", label: "Monday" },
  { value: "tue", label: "Tuesday" },
  { value: "wed", label: "Wednesday" },
  { value: "thu", label: "Thursday" },
  { value: "fri", label: "Friday" },
  { value: "sat", label: "Saturday" },
  { value: "sun", label: "Sunday" },
];

export function CronScheduleModal({
  cronExpression,
  timezone,
  prompt,
  onChangeCronExpression,
  onChangeTimezone,
  onChangePrompt,
  onClose,
}: CronScheduleModalProps) {
  const [scheduleMode, setScheduleMode] = useState<CronScheduleMode>(() => inferScheduleMode(cronExpression));
  const [timeOfDay, setTimeOfDay] = useState(() => inferTimeOfDay(cronExpression));
  const [minute, setMinute] = useState(() => inferMinute(cronExpression));
  const [weekday, setWeekday] = useState(() => inferWeekday(cronExpression));
  const [monthDay, setMonthDay] = useState(() => inferMonthDay(cronExpression));

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  function applySchedule(nextMode: CronScheduleMode, nextValues: Partial<{ timeOfDay: string; minute: string; weekday: string; monthDay: string }> = {}) {
    const values = {
      timeOfDay: nextValues.timeOfDay ?? timeOfDay,
      minute: nextValues.minute ?? minute,
      weekday: nextValues.weekday ?? weekday,
      monthDay: nextValues.monthDay ?? monthDay,
    };
    if (nextValues.timeOfDay !== undefined) {
      setTimeOfDay(nextValues.timeOfDay);
    }
    if (nextValues.minute !== undefined) {
      setMinute(nextValues.minute);
    }
    if (nextValues.weekday !== undefined) {
      setWeekday(nextValues.weekday);
    }
    if (nextValues.monthDay !== undefined) {
      setMonthDay(nextValues.monthDay);
    }
    setScheduleMode(nextMode);
    const expression = cronExpressionForMode(nextMode, values);
    if (expression !== null) {
      onChangeCronExpression(expression);
    }
  }

  const isCustom = scheduleMode === "custom";

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal supabase-auth-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="cron-schedule-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Trigger</div>
            <h3 id="cron-schedule-modal-title">Cron Schedule Trigger</h3>
            <p>Choose when this listener should fire. Each due fire starts a child run with the prompt in input_payload.prompt.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body supabase-auth-modal-body">
          <div className="supabase-auth-modal-grid">
            <label>
              Schedule Type
              <select
                value={scheduleMode}
                onChange={(event) => applySchedule(event.target.value as CronScheduleMode)}
              >
                <option value="every_15_minutes">Every 15 minutes</option>
                <option value="hourly">Hourly</option>
                <option value="daily">Daily</option>
                <option value="weekdays">Weekdays</option>
                <option value="weekly">Weekly</option>
                <option value="monthly">Monthly</option>
                <option value="custom">Custom cron expression</option>
              </select>
            </label>

            <label>
              Timezone
              <input
                list="cron-schedule-timezones"
                value={timezone}
                placeholder="UTC"
                onChange={(event) => onChangeTimezone(event.target.value)}
              />
              <datalist id="cron-schedule-timezones">
                {TIMEZONE_SUGGESTIONS.map((option) => (
                  <option key={option} value={option} />
                ))}
              </datalist>
            </label>
          </div>

          {!isCustom ? (
            <div className="supabase-auth-modal-grid">
              {scheduleMode === "hourly" ? (
                <label>
                  Minute
                  <select value={minute} onChange={(event) => applySchedule(scheduleMode, { minute: event.target.value })}>
                    {minuteOptions().map((option) => (
                      <option key={option} value={option}>
                        :{option}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}

              {["daily", "weekdays", "weekly", "monthly"].includes(scheduleMode) ? (
                <label>
                  Time
                  <input type="time" value={timeOfDay} onChange={(event) => applySchedule(scheduleMode, { timeOfDay: event.target.value })} />
                </label>
              ) : null}

              {scheduleMode === "weekly" ? (
                <label>
                  Day
                  <select value={weekday} onChange={(event) => applySchedule(scheduleMode, { weekday: event.target.value })}>
                    {WEEKDAY_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}

              {scheduleMode === "monthly" ? (
                <label>
                  Day of Month
                  <select value={monthDay} onChange={(event) => applySchedule(scheduleMode, { monthDay: event.target.value })}>
                    {Array.from({ length: 31 }, (_, index) => String(index + 1)).map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}
            </div>
          ) : null}

          <label>
            Cron Expression
            <input
              value={cronExpression}
              placeholder="0 9 * * *"
              readOnly={!isCustom}
              onChange={(event) => onChangeCronExpression(event.target.value)}
            />
            <small>Five fields: minute hour day-of-month month day-of-week.</small>
          </label>

          <label>
            Prompt
            <textarea
              value={prompt}
              placeholder="Describe the task this scheduled run should perform."
              rows={5}
              onChange={(event) => onChangePrompt(event.target.value)}
            />
          </label>
        </div>

        <div className="tool-details-modal-footer">
          <button type="button" className="primary-button" onClick={onClose}>
            Done
          </button>
        </div>
      </section>
    </div>
  );
}

function cronExpressionForMode(
  mode: CronScheduleMode,
  values: { timeOfDay: string; minute: string; weekday: string; monthDay: string },
): string | null {
  const { hour, minute } = parseTimeOfDay(values.timeOfDay);
  switch (mode) {
    case "every_15_minutes":
      return "*/15 * * * *";
    case "hourly":
      return `${normalizeMinute(values.minute)} * * * *`;
    case "daily":
      return `${minute} ${hour} * * *`;
    case "weekdays":
      return `${minute} ${hour} * * mon-fri`;
    case "weekly":
      return `${minute} ${hour} * * ${values.weekday || "mon"}`;
    case "monthly":
      return `${minute} ${hour} ${values.monthDay || "1"} * *`;
    case "custom":
      return null;
  }
}

function inferScheduleMode(expression: string): CronScheduleMode {
  const fields = splitCron(expression);
  if (!fields) {
    return "custom";
  }
  const [minute, hour, dayOfMonth, month, dayOfWeek] = fields;
  if (minute === "*/15" && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    return "every_15_minutes";
  }
  if (/^\d+$/.test(minute) && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    return "hourly";
  }
  if (/^\d+$/.test(minute) && /^\d+$/.test(hour) && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    return "daily";
  }
  if (/^\d+$/.test(minute) && /^\d+$/.test(hour) && dayOfMonth === "*" && month === "*" && dayOfWeek === "mon-fri") {
    return "weekdays";
  }
  if (/^\d+$/.test(minute) && /^\d+$/.test(hour) && dayOfMonth === "*" && month === "*" && dayOfWeek !== "*") {
    return "weekly";
  }
  if (/^\d+$/.test(minute) && /^\d+$/.test(hour) && dayOfMonth !== "*" && month === "*" && dayOfWeek === "*") {
    return "monthly";
  }
  return "custom";
}

function inferTimeOfDay(expression: string): string {
  const fields = splitCron(expression);
  if (!fields || !/^\d+$/.test(fields[0]) || !/^\d+$/.test(fields[1])) {
    return "09:00";
  }
  return `${pad2Hour(fields[1])}:${pad2Minute(fields[0])}`;
}

function inferMinute(expression: string): string {
  const fields = splitCron(expression);
  if (!fields || !/^\d+$/.test(fields[0])) {
    return "0";
  }
  return normalizeMinute(fields[0]);
}

function inferWeekday(expression: string): string {
  const fields = splitCron(expression);
  if (!fields || !WEEKDAY_OPTIONS.some((option) => option.value === fields[4])) {
    return "mon";
  }
  return fields[4];
}

function inferMonthDay(expression: string): string {
  const fields = splitCron(expression);
  if (!fields || !/^\d+$/.test(fields[2])) {
    return "1";
  }
  const parsed = Number.parseInt(fields[2], 10);
  return String(Math.min(31, Math.max(1, parsed)));
}

function splitCron(expression: string): string[] | null {
  const fields = expression.trim().split(/\s+/);
  return fields.length === 5 ? fields : null;
}

function parseTimeOfDay(value: string): { hour: string; minute: string } {
  const match = /^(\d{1,2}):(\d{2})$/.exec(value);
  if (!match) {
    return { hour: "9", minute: "0" };
  }
  return {
    hour: String(Math.min(23, Math.max(0, Number.parseInt(match[1], 10) || 0))),
    minute: String(Math.min(59, Math.max(0, Number.parseInt(match[2], 10) || 0))),
  };
}

function normalizeMinute(value: string): string {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return "0";
  }
  return String(Math.min(59, Math.max(0, parsed)));
}

function pad2Minute(value: string): string {
  return normalizeMinute(value).padStart(2, "0");
}

function pad2Hour(value: string): string {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return "09";
  }
  return String(Math.min(23, Math.max(0, parsed))).padStart(2, "0");
}

function minuteOptions(): string[] {
  return Array.from({ length: 60 }, (_, index) => String(index));
}
