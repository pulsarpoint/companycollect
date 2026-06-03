import { useEffect, useState } from "react";
import { Link } from "react-router";
import { CalendarClock, Play, RefreshCw } from "lucide-react";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import {
  FXRateSyncForm,
  defaultFXRateSyncFormValue,
  type FXRateSyncFormValue,
} from "~/components/app/FXRateSyncForm";
import { api, errorMessage } from "~/lib/api";
import type { ExchangeRateWorkflowRun } from "~/types/api";

function workflowStatusVariant(
  status: string,
): "default" | "secondary" | "destructive" | "outline" {
  if (status === "completed") return "default";
  if (status === "running") return "secondary";
  if (["failed", "terminated", "timed_out", "canceled"].includes(status)) {
    return "destructive";
  }
  return "outline";
}

function formatTimestamp(value?: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function FXRateSyncManagement() {
  const [form, setForm] = useState<FXRateSyncFormValue>(
    defaultFXRateSyncFormValue,
  );
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [workflowRuns, setWorkflowRuns] = useState<ExchangeRateWorkflowRun[]>(
    [],
  );
  const [loadingWorkflowRuns, setLoadingWorkflowRuns] = useState(false);
  const [workflowRunsError, setWorkflowRunsError] = useState<string | null>(
    null,
  );

  async function loadWorkflowRuns() {
    setLoadingWorkflowRuns(true);
    setWorkflowRunsError(null);
    try {
      const response = await api.getExchangeRateSyncRuns(10);
      setWorkflowRuns(response.items);
    } catch (err) {
      setWorkflowRunsError(
        errorMessage(err, "Failed to load recent FX workflow runs"),
      );
    } finally {
      setLoadingWorkflowRuns(false);
    }
  }

  useEffect(() => {
    void loadWorkflowRuns();
  }, []);

  async function runNow() {
    setRunning(true);
    setError(null);
    setMessage(null);
    try {
      const response = await api.startExchangeRateSync({
        provider: form.provider,
        source_url: form.source_url.trim(),
        trigger: "manual",
        force_reprocess: form.force_reprocess,
      });
      setMessage(`Started ${response.workflow_id}`);
      void loadWorkflowRuns();
    } catch (err) {
      setError(errorMessage(err, "Failed to start FX rate sync"));
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold">FX Rates</h1>
          <p className="text-sm text-muted-foreground">
            Run the ECB exchange-rate sync workflow and keep global currency
            reference data up to date.
          </p>
        </div>
        <Button variant="outline" asChild>
          <Link to="/settings/workflow-schedules">
            <CalendarClock className="size-4" />
            Schedules
          </Link>
        </Button>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {message && (
        <Alert>
          <AlertDescription>{message}</AlertDescription>
        </Alert>
      )}

      <section className="rounded-md border p-4">
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-base font-semibold">Manual sync</h2>
              <p className="text-sm text-muted-foreground">
                Trigger the Temporal workflow immediately with these input
                values.
              </p>
            </div>
            <Button onClick={() => void runNow()} disabled={running}>
              <Play className="size-4" />
              {running ? "Starting..." : "Run now"}
            </Button>
          </div>
          <FXRateSyncForm value={form} onChange={setForm} />
        </div>
      </section>

      <section className="rounded-md border p-4">
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-base font-semibold">Recent Temporal runs</h2>
              <p className="text-sm text-muted-foreground">
                Last 10 exchange-rate sync workflows from Temporal visibility.
              </p>
            </div>
            <Button
              variant="outline"
              onClick={() => void loadWorkflowRuns()}
              disabled={loadingWorkflowRuns}
            >
              <RefreshCw className="size-4" />
              {loadingWorkflowRuns ? "Refreshing..." : "Refresh"}
            </Button>
          </div>

          {workflowRunsError && (
            <Alert variant="destructive">
              <AlertDescription>{workflowRunsError}</AlertDescription>
            </Alert>
          )}

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Status</TableHead>
                <TableHead>Workflow</TableHead>
                <TableHead>Started</TableHead>
                <TableHead>Closed</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {workflowRuns.length === 0 ? (
                <TableRow>
                  <TableCell
                    colSpan={4}
                    className="h-24 text-center text-muted-foreground"
                  >
                    {loadingWorkflowRuns
                      ? "Loading recent runs..."
                      : "No FX rate workflow runs found."}
                  </TableCell>
                </TableRow>
              ) : (
                workflowRuns.map((run) => (
                  <TableRow key={`${run.workflow_id}:${run.run_id}`}>
                    <TableCell>
                      <Badge
                        variant={workflowStatusVariant(run.status)}
                        className="capitalize"
                      >
                        {run.status.replaceAll("_", " ")}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex max-w-[28rem] flex-col gap-1">
                        <span className="truncate font-mono text-xs">
                          {run.workflow_id}
                        </span>
                        <span className="truncate font-mono text-[11px] text-muted-foreground">
                          {run.run_id}
                        </span>
                      </div>
                    </TableCell>
                    <TableCell>{formatTimestamp(run.start_time)}</TableCell>
                    <TableCell>{formatTimestamp(run.close_time)}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </section>
    </div>
  );
}
