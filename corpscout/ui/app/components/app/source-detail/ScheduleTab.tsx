import { useEffect, useState } from "react";
import { Play, Save } from "lucide-react";
import type { DataSource } from "~/types/api";
import { cn, formatDate, timeAgo } from "~/lib/utils";
import { validateCronExpression, validateDuration } from "~/components/app/source-detail/sourceDetailUtils";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { RadioGroup, RadioGroupItem } from "~/components/ui/radio-group";
import { Separator } from "~/components/ui/separator";
import { Switch } from "~/components/ui/switch";

type SourcePatch = Parameters<typeof import("~/lib/api").api.patchSource>[1];

interface ScheduleTabProps {
  source: DataSource;
  saving: boolean;
  triggering: boolean;
  onPatch: (patch: SourcePatch) => Promise<void>;
  onTrigger: () => Promise<void>;
}

const DEFAULT_INTERVAL = "24h";
const DEFAULT_CRON = "0 4 * * *";

function nextRunText(source: DataSource): string {
  if (source.next_scheduled_at) return formatDate(source.next_scheduled_at);
  if (!source.enabled || !source.schedule_enabled) return "Paused";
  if (source.schedule_kind === "interval" && source.schedule_expression && !source.last_started_at) {
    return "Next scheduler check";
  }
  return "Not scheduled";
}

export function ScheduleTab({
  source,
  saving,
  triggering,
  onPatch,
  onTrigger,
}: ScheduleTabProps) {
  const [duration, setDuration] = useState(source.schedule_expression ?? "");
  const [durationError, setDurationError] = useState<string>();
  const [cronExpression, setCronExpression] = useState(source.schedule_kind === "cron" ? source.schedule_expression ?? "" : "");
  const [cronError, setCronError] = useState<string>();
  const scheduleMode = source.schedule_kind === "cron" ? "cron" : "interval";
  const manualOnlyTemporalSource =
    source.name === "ariregister" || source.name === "cvr" || source.name === "france";
  const autoSchedulingAvailable =
    source.download_workflow_registered && !manualOnlyTemporalSource;
  const manualTriggerAvailable = source.manual_trigger_available;
  const autoSchedulingEnabled =
    autoSchedulingAvailable &&
    source.enabled &&
    source.schedule_enabled &&
    (source.schedule_kind === "interval" || source.schedule_kind === "cron");
  const schedulingMessage = manualOnlyTemporalSource
    ? "Manual Temporal workflow actions are available. Automatic scheduling is not configured for this source."
    : autoSchedulingAvailable
    ? autoSchedulingEnabled
      ? "This source can be queued automatically. Manual actions remain available."
      : "Automatic scheduling is off. Manual actions remain available."
    : source.name === "brreg"
      ? "BRREG uses workflow actions instead of source scheduling."
      : "No download workflow is registered for this source.";

  useEffect(() => {
    if (source.schedule_kind === "interval") {
      setDuration(source.schedule_expression ?? "");
    }
    if (source.schedule_kind === "cron") {
      setCronExpression(source.schedule_expression ?? "");
    }
    setDurationError(undefined);
    setCronError(undefined);
  }, [source.schedule_expression, source.schedule_kind]);

  const nextRun = nextRunText(source);
  async function saveDuration() {
    const error = validateDuration(duration);
    setDurationError(error);
    if (error) return;

    await onPatch({
      enabled: true,
      schedule_enabled: true,
      schedule_kind: "interval",
      schedule_expression: duration.trim(),
    });
  }

  async function saveCronExpression() {
    const error = validateCronExpression(cronExpression);
    setCronError(error);
    if (error) return;

    await onPatch({
      enabled: true,
      schedule_enabled: true,
      schedule_kind: "cron",
      schedule_expression: cronExpression.trim(),
    });
  }

  async function setAutoScheduling(enabled: boolean) {
    if (!enabled) {
      await onPatch({ enabled: false, schedule_enabled: false });
      return;
    }

    if (source.schedule_kind === "cron") {
      const expression = cronExpression.trim() || DEFAULT_CRON;
      const error = validateCronExpression(expression);
      setCronError(error);
      if (error) return;
      await onPatch({
        enabled: true,
        schedule_enabled: true,
        schedule_kind: "cron",
        schedule_expression: expression,
      });
      return;
    }

    const expression = duration.trim() || DEFAULT_INTERVAL;
    const error = validateDuration(expression);
    setDurationError(error);
    if (error) return;
    await onPatch({
      enabled: true,
      schedule_enabled: true,
      schedule_kind: "interval",
      schedule_expression: expression,
    });
  }

  async function selectScheduleMode(mode: string) {
    if (mode === "cron") {
      const expression = cronExpression.trim() || DEFAULT_CRON;
      const error = validateCronExpression(expression);
      setCronError(error);
      if (error) return;
      await onPatch({
        enabled: true,
        schedule_enabled: true,
        schedule_kind: "cron",
        schedule_expression: expression,
      });
      return;
    }

    const expression = duration.trim() || DEFAULT_INTERVAL;
    const error = validateDuration(expression);
    setDurationError(error);
    if (error) return;
    await onPatch({
      enabled: true,
      schedule_enabled: true,
      schedule_kind: "interval",
      schedule_expression: expression,
    });
  }

  return (
    <div className="flex flex-col gap-4">
      <Alert>
        <AlertTitle>Auto scheduling</AlertTitle>
        <AlertDescription className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <span>{schedulingMessage}</span>
          <Switch
            checked={autoSchedulingEnabled}
            disabled={!autoSchedulingAvailable || saving}
            onCheckedChange={setAutoScheduling}
          />
        </AlertDescription>
      </Alert>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(280px,360px)]">
        <div className="flex flex-col gap-4">
          <RadioGroup
            className="grid gap-4 md:grid-cols-2"
            value={scheduleMode}
            onValueChange={selectScheduleMode}
          >
            <Card className={cn(scheduleMode === "interval" && "border-primary")}>
              <CardHeader>
                <div className="flex items-start gap-3">
                  <RadioGroupItem id="schedule-mode-interval" value="interval" disabled={!autoSchedulingAvailable || saving} />
                  <div className="flex flex-col gap-1">
                    <Label htmlFor="schedule-mode-interval" className="font-medium">Interval</Label>
                    <CardDescription>Run again after a fixed delay from the last start.</CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="schedule-duration">Duration</Label>
                  <div className="flex flex-col gap-2 sm:flex-row">
                    <Input
                      id="schedule-duration"
                      value={duration}
                      disabled={!autoSchedulingAvailable || !autoSchedulingEnabled || scheduleMode !== "interval" || saving}
                      onChange={(event) => {
                        setDuration(event.target.value);
                        setDurationError(undefined);
                      }}
                      placeholder={DEFAULT_INTERVAL}
                      aria-invalid={Boolean(durationError)}
                    />
                    <Button disabled={!autoSchedulingAvailable || !autoSchedulingEnabled || scheduleMode !== "interval" || saving} onClick={saveDuration}>
                      <Save data-icon="inline-start" />
                      Save
                    </Button>
                  </div>
                  {durationError && <p className="text-sm text-destructive">{durationError}</p>}
                </div>
              </CardContent>
            </Card>

            <Card className={cn(scheduleMode === "cron" && "border-primary")}>
              <CardHeader>
                <div className="flex items-start gap-3">
                  <RadioGroupItem id="schedule-mode-cron" value="cron" disabled={!autoSchedulingAvailable || saving} />
                  <div className="flex flex-col gap-1">
                    <Label htmlFor="schedule-mode-cron" className="font-medium">Cron</Label>
                    <CardDescription>Run at a fixed clock schedule.</CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="schedule-cron">Expression</Label>
                  <div className="flex flex-col gap-2 sm:flex-row">
                    <Input
                      id="schedule-cron"
                      value={cronExpression}
                      disabled={!autoSchedulingAvailable || !autoSchedulingEnabled || scheduleMode !== "cron" || saving}
                      onChange={(event) => {
                        setCronExpression(event.target.value);
                        setCronError(undefined);
                      }}
                      placeholder={DEFAULT_CRON}
                      aria-invalid={Boolean(cronError)}
                    />
                    <Button disabled={!autoSchedulingAvailable || !autoSchedulingEnabled || scheduleMode !== "cron" || saving} onClick={saveCronExpression}>
                      <Save data-icon="inline-start" />
                      Save
                    </Button>
                  </div>
                  {cronError && <p className="text-sm text-destructive">{cronError}</p>}
                </div>
              </CardContent>
            </Card>
          </RadioGroup>

          <Card>
            <CardHeader>
              <CardTitle>Manual actions</CardTitle>
              <CardDescription>Queue work without changing the automatic schedule.</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex flex-col gap-1 text-sm">
                <span className="text-muted-foreground">Next scheduled</span>
                <span className="font-medium">{nextRun}</span>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button disabled={!manualTriggerAvailable || triggering} onClick={onTrigger} variant="outline">
                  <Play data-icon="inline-start" />
                  {triggering ? "Queuing..." : manualTriggerAvailable ? "Trigger now" : "No trigger available"}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Last run</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <div className="flex flex-col gap-1">
              <p className="text-xs uppercase text-muted-foreground">Last started</p>
              <p className="text-sm">{source.last_started_at ? timeAgo(source.last_started_at) : "Never"}</p>
            </div>
            <Separator />
            <div className="flex flex-col gap-1">
              <p className="text-xs uppercase text-muted-foreground">Last success</p>
              <p className="text-sm">{source.last_success_at ? formatDate(source.last_success_at) : "-"}</p>
            </div>
            <div className="flex flex-col gap-1">
              <p className="text-xs uppercase text-muted-foreground">Last failure</p>
              <p className="text-sm">{source.last_failed_at ? formatDate(source.last_failed_at) : "-"}</p>
            </div>
            <div className="flex flex-col gap-1">
              <p className="text-xs uppercase text-muted-foreground">Consecutive failures</p>
              <Badge variant="outline">{source.consecutive_failures}</Badge>
            </div>
            {source.last_error && (
              <p className="whitespace-pre-wrap rounded-md bg-muted p-3 text-sm text-destructive">
                {source.last_error}
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
