import { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, Database, Play, RefreshCw, Search, X } from "lucide-react";
import { Link } from "react-router";
import { toast } from "sonner";

import { api, errorMessage } from "~/lib/api";
import type {
  BrregTaskStateAction,
  BrregTaskStateResponse,
  BrregWorkflowRun,
  BrregWorkflowRunPrefix,
} from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { Input } from "~/components/ui/input";
import { Skeleton } from "~/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

function number(value: number) {
  return value.toLocaleString();
}

function formatDate(value: string | undefined) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
}

const ACTION_RUNNERS: Record<string, { buttonLabel: string; run: () => Promise<string> }> = {
  translate: {
    buttonLabel: "Translate",
    run: async () => {
      await api.translateBrregSourceCompanies({ all_records: true });
      return "BRREG company translation workflow started.";
    },
  },
};

const WORKFLOW_STATUSES = [
  ["running", "Running"],
  ["completed", "Completed"],
  ["failed", "Failed"],
  ["canceled", "Canceled"],
  ["terminated", "Terminated"],
  ["continued_as_new", "Continued as new"],
  ["timed_out", "Timed out"],
] as const;

function workflowStatusVariant(status: string) {
  if (status === "completed") return "secondary";
  if (status === "failed" || status === "terminated" || status === "timed_out") return "destructive";
  if (status === "running") return "default";
  return "outline";
}

function ActionMetrics({ action }: { action: BrregTaskStateAction }) {
  const metrics = [
    ["Eligible", action.state.task_eligible_now],
    ["Running", action.state.task_running_active],
    ["Retryable", action.state.task_failed_retryable],
    ["Terminal", action.state.task_failed_terminal],
    ["Artifacts", action.state.artifact_succeeded + action.state.artifact_skipped + action.state.artifact_failed],
    ["Missing", action.state.artifact_missing],
  ];

  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
      {metrics.map(([label, value]) => (
        <div key={label} className="rounded-md border px-3 py-2">
          <div className="text-xs text-muted-foreground">{label}</div>
          <div className="text-sm font-semibold tabular-nums">{number(Number(value))}</div>
        </div>
      ))}
    </div>
  );
}

function BrregWorkflowRunsTable({ runs }: { runs: BrregWorkflowRun[] }) {
  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Workflow</TableHead>
            <TableHead>Action</TableHead>
            <TableHead>Status</TableHead>
            <TableHead>Started</TableHead>
            <TableHead>Closed</TableHead>
            <TableHead>Run ID</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {runs.length === 0 ? (
            <TableRow>
              <TableCell colSpan={6} className="py-10 text-center text-muted-foreground">
                No BRREG workflows found.
              </TableCell>
            </TableRow>
          ) : (
            runs.map((run) => (
              <TableRow key={`${run.workflow_id}:${run.run_id}`}>
                <TableCell>
                  <div className="flex min-w-72 flex-col gap-1">
                    <span className="font-mono text-xs">{run.workflow_id}</span>
                    <span className="text-xs text-muted-foreground">{run.workflow_type}</span>
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex min-w-44 flex-col gap-1">
                    <span className="font-medium">{run.action}</span>
                    <Badge variant="outline">{run.prefix}</Badge>
                  </div>
                </TableCell>
                <TableCell>
                  <Badge variant={workflowStatusVariant(run.status)}>{run.status.replace(/_/g, " ")}</Badge>
                </TableCell>
                <TableCell className="whitespace-nowrap text-sm">{formatDate(run.start_time)}</TableCell>
                <TableCell className="whitespace-nowrap text-sm">{formatDate(run.close_time)}</TableCell>
                <TableCell>
                  <span className="font-mono text-xs text-muted-foreground">{run.run_id}</span>
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </div>
  );
}

export function BrregTaskStateTab() {
  const [taskState, setTaskState] = useState<BrregTaskStateResponse>();
  const [runs, setRuns] = useState<BrregWorkflowRun[]>([]);
  const [prefixes, setPrefixes] = useState<BrregWorkflowRunPrefix[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [runningAction, setRunningAction] = useState<string>();
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [prefix, setPrefix] = useState("");
  const [status, setStatus] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError(undefined);
    try {
      const [workflowRuns, nextTaskState] = await Promise.all([
        api.getBrregWorkflowRuns({
          limit: 50,
          q: search || undefined,
          prefix: prefix || undefined,
          status: status || undefined,
        }),
        api.getBrregTaskState(),
      ]);
      setRuns(workflowRuns.items);
      setPrefixes(workflowRuns.prefixes);
      setTaskState(nextTaskState);
    } catch (err) {
      setError(errorMessage(err, "Failed to load BRREG workflows."));
    } finally {
      setLoading(false);
    }
  }, [prefix, search, status]);

  useEffect(() => {
    void load();
  }, [load]);

  const totalFailures = useMemo(
    () =>
      taskState?.actions.reduce(
        (sum, action) => sum + action.state.task_failed_retryable + action.state.task_failed_terminal,
        0,
      ) ?? 0,
    [taskState],
  );

  const applySearch = () => setSearch(searchInput.trim());
  const clearSearch = () => {
    setSearchInput("");
    setSearch("");
  };

  async function runAction(action: BrregTaskStateAction) {
    const runner = ACTION_RUNNERS[action.key];
    if (!runner) return;
    setRunningAction(action.key);
    try {
      toast.success(await runner.run());
      await load();
    } catch (err) {
      toast.error(errorMessage(err, `Failed to run ${action.label}.`));
    } finally {
      setRunningAction(undefined);
    }
  }

  if (loading) {
    return (
      <div className="flex flex-col gap-4">
        <Skeleton className="h-10 w-full" />
        <Skeleton className="h-96 w-full" />
      </div>
    );
  }

  if (error || !taskState) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{error ?? "BRREG task state is unavailable."}</AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-base font-semibold">BRREG workflows</h2>
          <div className="text-sm text-muted-foreground">Updated {formatDate(taskState.updated_at)}</div>
        </div>
        <Button variant="outline" size="sm" onClick={load}>
          <RefreshCw data-icon="inline-start" />
          Refresh
        </Button>
      </div>

      {totalFailures > 0 && (
        <Alert>
          <AlertTriangle />
          <AlertDescription>{number(totalFailures)} BRREG tasks are failed.</AlertDescription>
        </Alert>
      )}

      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center gap-3">
          <div className="relative flex items-center">
            <Search className="absolute left-2.5 size-4 text-muted-foreground" />
            <Input
              className="h-8 w-80 pl-8 pr-8 text-sm"
              placeholder="Search workflows"
              value={searchInput}
              onChange={(event) => setSearchInput(event.target.value)}
              onKeyDown={(event) => event.key === "Enter" && applySearch()}
            />
            {searchInput && (
              <button className="absolute right-2" onClick={clearSearch} type="button">
                <X className="size-3.5 text-muted-foreground" />
              </button>
            )}
          </div>
          {searchInput !== search && (
            <Button size="sm" variant="secondary" className="h-8" onClick={applySearch}>
              Search
            </Button>
          )}
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            <span>Prefix</span>
            <select
              className="h-8 rounded-md border border-input bg-background px-2 text-sm text-foreground"
              value={prefix}
              onChange={(event) => setPrefix(event.target.value)}
            >
              <option value="">All</option>
              {prefixes.map((option) => (
                <option key={option.prefix} value={option.prefix}>
                  {option.prefix}
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            <span>Status</span>
            <select
              className="h-8 rounded-md border border-input bg-background px-2 text-sm text-foreground"
              value={status}
              onChange={(event) => setStatus(event.target.value)}
            >
              <option value="">All</option>
              {WORKFLOW_STATUSES.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </label>
          <span className="ml-auto text-sm text-muted-foreground">
            {number(runs.length)} workflow runs
          </span>
        </div>
        <BrregWorkflowRunsTable runs={runs} />
      </section>

      <section className="grid gap-3 lg:grid-cols-2">
        {taskState.actions.map((action) => {
          const runner = ACTION_RUNNERS[action.key];
          return (
            <Card key={action.key}>
              <CardHeader className="gap-2">
                <div className="flex items-start justify-between gap-3">
                  <div className="space-y-1">
                    <CardTitle className="text-base">{action.label}</CardTitle>
                    <p className="text-sm leading-5 text-muted-foreground">{action.description}</p>
                  </div>
                  {action.task_type && <Badge variant="outline">{action.task_type}</Badge>}
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                <ActionMetrics action={action} />
                <Button
                  size="sm"
                  variant={runner ? "default" : "outline"}
                  disabled={!runner || runningAction !== undefined}
                  onClick={() => runAction(action)}
                >
                  <Play data-icon="inline-start" />
                  {runningAction === action.key ? "Running..." : runner?.buttonLabel ?? "No action"}
                </Button>
              </CardContent>
            </Card>
          );
        })}
      </section>

      <section className="space-y-3">
        <div className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" />
          <h2 className="text-base font-semibold">BRREG result tables</h2>
        </div>
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Table</TableHead>
                <TableHead className="text-right">Rows</TableHead>
                <TableHead className="text-right">Open</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {taskState.result_tables.map((table) => (
                <TableRow key={table.name}>
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <span className="font-medium">{table.label}</span>
                      <span className="font-mono text-xs text-muted-foreground">{table.name}</span>
                    </div>
                  </TableCell>
                  <TableCell className="text-right font-mono text-sm">{number(table.count)}</TableCell>
                  <TableCell className="text-right">
                    {table.href ? (
                      <Button asChild size="sm" variant="outline">
                        <Link to={table.href}>Open</Link>
                      </Button>
                    ) : (
                      <Button size="sm" variant="outline" disabled>
                        Pending
                      </Button>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </section>
    </div>
  );
}
