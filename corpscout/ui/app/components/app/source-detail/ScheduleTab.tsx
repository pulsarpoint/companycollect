import { useEffect, useMemo, useState } from "react";
import { api, errorMessage } from "~/lib/api";
import type { DataSource, WorkflowSchedule } from "~/types/api";
import { formatDate } from "~/lib/utils";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

interface ScheduleTabProps {
  source: DataSource;
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function scheduleMatchesSource(schedule: WorkflowSchedule, source: DataSource): boolean {
  const actionInput = schedule.action_input ?? {};
  const metadata = schedule.metadata ?? {};
  const candidates = new Set([
    source.name,
    source.registry_key,
    source.country,
    source.source,
  ]);

  const values = [
    stringValue(metadata.source),
    stringValue(metadata.source_name),
    stringValue(metadata.data_source),
    stringValue(metadata.registry_key),
    stringValue(actionInput.source),
    stringValue(actionInput.source_name),
    stringValue(actionInput.data_source),
    stringValue(actionInput.registry_key),
    stringValue(actionInput.country),
    stringValue(actionInput.country_source),
    ...schedule.tags,
  ];

  return values.some((value) => candidates.has(value));
}

function temporalState(schedule: WorkflowSchedule): { label: string; className: string } {
  if (!schedule.temporal.exists) {
    return { label: "Missing", className: "border-red-200 bg-red-100 text-red-800" };
  }
  if (schedule.temporal.paused || !schedule.enabled) {
    return { label: "Paused", className: "border-amber-200 bg-amber-100 text-amber-800" };
  }
  return { label: "Active", className: "border-green-200 bg-green-100 text-green-800" };
}

export function ScheduleTab({ source }: ScheduleTabProps) {
  const [schedules, setSchedules] = useState<WorkflowSchedule[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let ignore = false;
    setLoading(true);
    setError(undefined);
    api.getWorkflowSchedules()
      .then((response) => {
        if (!ignore) setSchedules(response.items);
      })
      .catch((err) => {
        if (!ignore) setError(errorMessage(err, "Failed to load Temporal schedules."));
      })
      .finally(() => {
        if (!ignore) setLoading(false);
      });
    return () => {
      ignore = true;
    };
  }, [source.name]);

  const sourceSchedules = useMemo(
    () => schedules.filter((schedule) => scheduleMatchesSource(schedule, source)),
    [schedules, source],
  );

  if (loading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 3 }).map((_, index) => (
          <Skeleton key={index} className="h-16 w-full" />
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  if (sourceSchedules.length === 0) {
    return (
      <Alert>
        <AlertDescription>No Temporal schedules found for this source.</AlertDescription>
      </Alert>
    );
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Schedule</TableHead>
          <TableHead>Workflow</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Cron</TableHead>
          <TableHead>Next run</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {sourceSchedules.map((schedule) => {
          const state = temporalState(schedule);
          return (
            <TableRow key={schedule.temporal_schedule_id}>
              <TableCell className="min-w-[18rem]">
                <div className="font-medium">{schedule.display_name}</div>
                <div className="font-mono text-xs text-muted-foreground">
                  {schedule.temporal_schedule_id}
                </div>
              </TableCell>
              <TableCell>
                <div className="space-y-1 text-sm">
                  <div>{schedule.workflow_name}</div>
                  <div className="font-mono text-xs text-muted-foreground">
                    {schedule.task_queue}
                  </div>
                </div>
              </TableCell>
              <TableCell>
                <Badge className={state.className} variant="outline">
                  {state.label}
                </Badge>
              </TableCell>
              <TableCell>
                <div className="space-y-1 text-sm">
                  <div className="font-mono">{schedule.spec.cron_expression}</div>
                  <div className="text-xs text-muted-foreground">
                    {schedule.spec.timezone}
                  </div>
                </div>
              </TableCell>
              <TableCell>
                <span className="text-sm">
                  {schedule.temporal.next_run_at
                    ? formatDate(schedule.temporal.next_run_at)
                    : "-"}
                </span>
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}
