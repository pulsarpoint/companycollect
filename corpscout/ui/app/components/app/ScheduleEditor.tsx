import { useMemo, useState } from "react";
import { Clock3 } from "lucide-react";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import { Switch } from "~/components/ui/switch";
import type { WorkflowScheduleSpec } from "~/types/api";

type ScheduleMode = "daily" | "weekly" | "monthly" | "advanced";

interface ScheduleEditorProps {
  spec: WorkflowScheduleSpec;
  enabled: boolean;
  onSpecChange: (spec: WorkflowScheduleSpec) => void;
  onEnabledChange: (enabled: boolean) => void;
}

const weekdays = [
  { label: "Monday", value: "1" },
  { label: "Tuesday", value: "2" },
  { label: "Wednesday", value: "3" },
  { label: "Thursday", value: "4" },
  { label: "Friday", value: "5" },
  { label: "Saturday", value: "6" },
  { label: "Sunday", value: "0" },
] as const;

export function ScheduleEditor({
  spec,
  enabled,
  onSpecChange,
  onEnabledChange,
}: ScheduleEditorProps) {
  const [mode, setMode] = useState<ScheduleMode>(() =>
    modeFromCron(spec.cron_expression),
  );
  const [time, setTime] = useState(() => timeFromCron(spec.cron_expression));
  const [weekday, setWeekday] = useState(() =>
    weekdayFromCron(spec.cron_expression),
  );
  const [monthDay, setMonthDay] = useState(() =>
    monthDayFromCron(spec.cron_expression),
  );

  const preview = useMemo(() => describeSchedule(spec), [spec]);

  function updatePreset(
    nextMode: ScheduleMode,
    nextTime = time,
    nextWeekday = weekday,
    nextMonthDay = monthDay,
  ) {
    setMode(nextMode);
    setTime(nextTime);
    setWeekday(nextWeekday);
    setMonthDay(nextMonthDay);
    if (nextMode === "advanced") return;
    onSpecChange({
      ...spec,
      cron_expression: cronFromPreset(
        nextMode,
        nextTime,
        nextWeekday,
        nextMonthDay,
      ),
    });
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between gap-4">
        <div>
          <Label className="text-sm font-medium">Enabled</Label>
          <p className="text-xs text-muted-foreground">
            Temporal runs this schedule only when it is enabled and not paused.
          </p>
        </div>
        <Switch checked={enabled} onCheckedChange={onEnabledChange} />
      </div>

      <Separator />

      <div className="grid gap-2">
        <Label>Frequency</Label>
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          {(["daily", "weekly", "monthly", "advanced"] as ScheduleMode[]).map(
            (item) => (
              <Button
                key={item}
                type="button"
                variant={mode === item ? "default" : "outline"}
                onClick={() => updatePreset(item)}
                className="justify-center capitalize"
              >
                {item}
              </Button>
            ),
          )}
        </div>
      </div>

      {mode !== "advanced" && (
        <div className="grid gap-4 sm:grid-cols-3">
          <div className="grid gap-2">
            <Label htmlFor="schedule-time">Time</Label>
            <Input
              id="schedule-time"
              type="time"
              value={time}
              onChange={(event) => updatePreset(mode, event.target.value)}
            />
          </div>
          {mode === "weekly" && (
            <div className="grid gap-2">
              <Label htmlFor="schedule-weekday">Weekday</Label>
              <select
                id="schedule-weekday"
                value={weekday}
                onChange={(event) =>
                  updatePreset(mode, time, event.target.value)
                }
                className="h-10 rounded-md border border-input bg-background px-3 text-sm"
              >
                {weekdays.map((day) => (
                  <option key={day.value} value={day.value}>
                    {day.label}
                  </option>
                ))}
              </select>
            </div>
          )}
          {mode === "monthly" && (
            <div className="grid gap-2">
              <Label htmlFor="schedule-month-day">Day of month</Label>
              <Input
                id="schedule-month-day"
                type="number"
                min={1}
                max={28}
                value={monthDay}
                onChange={(event) =>
                  updatePreset(mode, time, weekday, event.target.value)
                }
              />
              <p className="text-xs text-muted-foreground">
                Use 1-28 so every month has the selected day.
              </p>
            </div>
          )}
        </div>
      )}

      <div className="grid gap-2">
        <Label htmlFor="schedule-cron">Cron expression</Label>
        <Input
          id="schedule-cron"
          value={spec.cron_expression}
          readOnly={mode !== "advanced"}
          onChange={(event) =>
            onSpecChange({ ...spec, cron_expression: event.target.value })
          }
        />
        <p className="text-xs text-muted-foreground">
          Five-field cron expression interpreted in the configured timezone.
        </p>
      </div>

      <div className="grid gap-4 sm:grid-cols-3">
        <div className="grid gap-2">
          <Label htmlFor="schedule-timezone">Timezone</Label>
          <Input
            id="schedule-timezone"
            value={spec.timezone}
            onChange={(event) =>
              onSpecChange({ ...spec, timezone: event.target.value })
            }
          />
        </div>
        <div className="grid gap-2">
          <Label htmlFor="schedule-overlap">Overlap policy</Label>
          <select
            id="schedule-overlap"
            value={spec.overlap_policy}
            onChange={(event) =>
              onSpecChange({
                ...spec,
                overlap_policy: event.target
                  .value as WorkflowScheduleSpec["overlap_policy"],
              })
            }
            className="h-10 rounded-md border border-input bg-background px-3 text-sm"
          >
            <option value="skip">Skip when previous run is active</option>
            <option value="buffer_one">Buffer one run</option>
            <option value="allow_all">Allow overlapping runs</option>
            <option value="cancel_other">Cancel previous run</option>
            <option value="terminate_other">Terminate previous run</option>
          </select>
        </div>
        <div className="grid gap-2">
          <Label htmlFor="schedule-catchup">Catchup window seconds</Label>
          <Input
            id="schedule-catchup"
            type="number"
            min={0}
            value={spec.catchup_window_seconds}
            onChange={(event) =>
              onSpecChange({
                ...spec,
                catchup_window_seconds: Number(event.target.value) || 0,
              })
            }
          />
        </div>
      </div>

      <div className="flex items-center gap-2 rounded-md border bg-muted/30 px-3 py-2 text-sm">
        <Clock3 className="size-4 text-muted-foreground" />
        <span>{preview}</span>
      </div>
    </div>
  );
}

function modeFromCron(cron: string): ScheduleMode {
  const fields = cron.trim().split(/\s+/);
  if (fields.length !== 5) return "advanced";
  if (fields[2] !== "*" && fields[3] === "*" && fields[4] === "*") {
    return "monthly";
  }
  if (fields[2] === "*" && fields[3] === "*" && fields[4] !== "*") {
    return "weekly";
  }
  if (fields[2] === "*" && fields[3] === "*" && fields[4] === "*") {
    return "daily";
  }
  return "advanced";
}

function timeFromCron(cron: string): string {
  const fields = cron.trim().split(/\s+/);
  if (fields.length !== 5) return "03:00";
  const minute = fields[0].padStart(2, "0");
  const hour = fields[1].padStart(2, "0");
  return `${hour}:${minute}`;
}

function weekdayFromCron(cron: string): string {
  const fields = cron.trim().split(/\s+/);
  return fields.length === 5 && fields[4] !== "*" ? fields[4] : "1";
}

function monthDayFromCron(cron: string): string {
  const fields = cron.trim().split(/\s+/);
  return fields.length === 5 && fields[2] !== "*" ? fields[2] : "1";
}

function cronFromPreset(
  mode: ScheduleMode,
  time: string,
  weekday: string,
  monthDay: string,
): string {
  const [hour = "3", minute = "0"] = time.split(":");
  if (mode === "weekly") return `${Number(minute)} ${Number(hour)} * * ${weekday}`;
  if (mode === "monthly") return `${Number(minute)} ${Number(hour)} ${monthDay} * *`;
  return `${Number(minute)} ${Number(hour)} * * *`;
}

function describeSchedule(spec: WorkflowScheduleSpec): string {
  return `Runs with cron ${spec.cron_expression} in ${spec.timezone || "Europe/Belgrade"}.`;
}
