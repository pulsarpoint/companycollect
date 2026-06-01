import { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, Database, Play, RefreshCw } from "lucide-react";
import { Link } from "react-router";
import { toast } from "sonner";

import { api, errorMessage } from "~/lib/api";
import type { BrregTaskStateAction, BrregTaskStateResponse } from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
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
      await api.translateBrreg();
      return "BRREG translation task run started.";
    },
  },
};

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

export function BrregTaskStateTab() {
  const [taskState, setTaskState] = useState<BrregTaskStateResponse>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [runningAction, setRunningAction] = useState<string>();

  const load = useCallback(async () => {
    setLoading(true);
    setError(undefined);
    try {
      setTaskState(await api.getBrregTaskState());
    } catch (err) {
      setError(errorMessage(err, "Failed to load BRREG task state."));
    } finally {
      setLoading(false);
    }
  }, []);

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
        <div className="grid gap-3 lg:grid-cols-2">
          {Array.from({ length: 4 }).map((_, index) => (
            <Skeleton key={index} className="h-52 w-full" />
          ))}
        </div>
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
          <h2 className="text-base font-semibold">BRREG task state</h2>
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
