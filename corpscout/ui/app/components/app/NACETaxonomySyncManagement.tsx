import { useEffect, useState } from "react";
import { Link } from "react-router";
import { BookOpen, CalendarClock, Play, RefreshCw } from "lucide-react";
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
  NACETaxonomySyncForm,
  defaultNACETaxonomySyncFormValue,
  type NACETaxonomySyncFormValue,
} from "~/components/app/NACETaxonomySyncForm";
import { api, errorMessage } from "~/lib/api";
import type { NACETaxonomyWorkflowRun } from "~/types/api";

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

export function NACETaxonomySyncManagement() {
  const [form, setForm] = useState<NACETaxonomySyncFormValue>(
    defaultNACETaxonomySyncFormValue,
  );
  const [running, setRunning] = useState(false);
  const [syncingClickHouse, setSyncingClickHouse] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [clickHouseError, setClickHouseError] = useState<string | null>(null);
  const [clickHouseMessage, setClickHouseMessage] = useState<string | null>(
    null,
  );
  const [workflowRuns, setWorkflowRuns] = useState<NACETaxonomyWorkflowRun[]>(
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
      const response = await api.getNACETaxonomySyncRuns(10);
      setWorkflowRuns(response.items);
    } catch (err) {
      setWorkflowRunsError(
        errorMessage(err, "Failed to load recent NACE workflow runs"),
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
      const response = await api.startNACETaxonomySync({
        revision: form.revision.trim(),
        source_url: form.source_url.trim(),
        trigger: "manual",
        force_reprocess: form.force_reprocess,
      });
      setMessage(`Started ${response.workflow_id}`);
      void loadWorkflowRuns();
    } catch (err) {
      setError(errorMessage(err, "Failed to start NACE taxonomy sync"));
    } finally {
      setRunning(false);
    }
  }

  async function syncToClickHouse() {
    setSyncingClickHouse(true);
    setClickHouseError(null);
    setClickHouseMessage(null);
    try {
      const response = await api.startNACEClickHouseSync({
        trigger: "manual",
      });
      setClickHouseMessage(`Started ${response.workflow_id}`);
    } catch (err) {
      setClickHouseError(errorMessage(err, "Failed to start NACE ClickHouse sync"));
    } finally {
      setSyncingClickHouse(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold">NACE Taxonomy</h1>
          <p className="text-sm text-muted-foreground">
            Run the NACE taxonomy sync workflow and keep the global industry
            classification table up to date.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" asChild>
            <Link to="/settings/nace-taxonomy/browser">
              <BookOpen className="size-4" />
              Browse taxonomy
            </Link>
          </Button>
          <Button variant="outline" asChild>
            <Link to="/settings/workflow-schedules">
              <CalendarClock className="size-4" />
              Schedules
            </Link>
          </Button>
          <Button
            variant="outline"
            onClick={() => void syncToClickHouse()}
            disabled={syncingClickHouse}
          >
            <RefreshCw className="size-4" />
            {syncingClickHouse ? "Starting..." : "Sync to CH"}
          </Button>
        </div>
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
      {clickHouseError && (
        <Alert variant="destructive">
          <AlertDescription>{clickHouseError}</AlertDescription>
        </Alert>
      )}
      {clickHouseMessage && (
        <Alert>
          <AlertDescription>{clickHouseMessage}</AlertDescription>
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
          <NACETaxonomySyncForm value={form} onChange={setForm} />
        </div>
      </section>

      <section className="rounded-md border p-4">
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-base font-semibold">Recent Temporal runs</h2>
              <p className="text-sm text-muted-foreground">
                Last 10 NACE taxonomy sync workflows from Temporal visibility.
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
                      : "No NACE taxonomy workflow runs found."}
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
