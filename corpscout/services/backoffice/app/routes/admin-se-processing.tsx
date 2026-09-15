import { useCallback, useEffect, useState } from "react";
import { useRevalidator } from "react-router";
import { ActivityIcon, ArrowUpRightIcon, CheckIcon, CircleAlertIcon, LoaderCircleIcon, RefreshCwIcon } from "lucide-react";
import type { Route } from "./+types/admin-se-processing";
import { CompanyActionDialog } from "~/components/admin/company-action-dialog";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Field, FieldLabel } from "~/components/ui/field";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { COMPANY_ACTION_AREAS, type CompanyActionResult, type CompanyActionSelection } from "~/lib/company-actions";
import { isProcessingActive, mergeProcessingSnapshot, processingDuration, processingStatusLabel, processingStepLabel, processingTime, type ProcessingRun } from "~/lib/company-processing";
import { loadCompanyProcessing } from "~/lib/company-processing.server";
import { listLlmProfiles } from "~/lib/llm-settings.server";
import { listPeoplePrompts } from "~/lib/people-prompts.server";

import { listDomainPrompts } from "~/lib/domain-prompts.server";

export async function loader() {
  const snapshot = await loadCompanyProcessing();
  const profiles = listLlmProfiles().map(({ profileId, name, provider, model, isActive }) => ({ profileId, name, provider, model, isActive }));
  return { snapshot, profiles, prompts: listPeoplePrompts(), domainPrompts: listDomainPrompts() };
}

export function meta() {
  return [{ title: "Processing | CompanyCollect admin" }];
}

function RunStatus({ status }: { status: string }) {
  return <Badge variant={status === "FAILURE" ? "destructive" : "secondary"} className={status === "SUCCESS" ? "bg-emerald-500/10 text-emerald-700 dark:text-emerald-400" : undefined}>
    {status === "SUCCESS" ? <CheckIcon /> : status === "FAILURE" ? <CircleAlertIcon /> : isProcessingActive(status) ? <ActivityIcon /> : null}
    {processingStatusLabel(status)}
  </Badge>;
}

function RunLink({ run }: { run: ProcessingRun }) {
  return run.runUrl ? <a href={run.runUrl} target="_blank" rel="noreferrer" aria-label={`View Dagster run ${run.runId}`} className="inline-flex items-center gap-1 whitespace-nowrap text-sm underline-offset-4 hover:underline">
    View run <ArrowUpRightIcon className="size-3.5" />
  </a> : <span className="font-mono text-xs text-muted-foreground">{run.runId.slice(0, 8)}</span>;
}

export default function AdminSeProcessing({ loaderData }: Route.ComponentProps) {
  const [snapshot, setSnapshot] = useState(loaderData.snapshot);
  const [submitted, setSubmitted] = useState<ProcessingRun[]>([]);
  const [selected, setSelected] = useState<CompanyActionSelection | null>(null);
  const [workflow, setWorkflow] = useState("all");
  const [status, setStatus] = useState("all");
  const [page, setPage] = useState(0);
  const { revalidate, state } = useRevalidator();

  useEffect(() => {
    setSnapshot((previous) => mergeProcessingSnapshot(previous, loaderData.snapshot));
    setSubmitted((previous) => previous.filter((run) => !loaderData.snapshot.runs.some((fresh) => fresh.runId === run.runId)));
  }, [loaderData.snapshot]);

  const runs = [...new Map([...submitted, ...snapshot.runs].map((run) => [run.runId, run])).values()];
  const activeRuns = runs.filter((run) => isProcessingActive(run.status)).sort((a, b) => b.createdAt - a.createdAt);
  const history = runs.filter((run) => !isProcessingActive(run.status) && (workflow === "all" || run.area === workflow) && (status === "all" || run.status === status)).sort((a, b) => b.createdAt - a.createdAt);
  const lastPage = Math.max(0, Math.ceil(history.length / 20) - 1);
  const currentPage = Math.min(page, lastPage);
  const errors = COMPANY_ACTION_AREAS.filter((area) => snapshot.errors[area.value]);

  useEffect(() => {
    const refresh = () => { if (document.visibilityState === "visible" && state === "idle") void revalidate(); };
    const timer = window.setInterval(refresh, activeRuns.length ? 5_000 : 15_000);
    document.addEventListener("visibilitychange", refresh);
    return () => { window.clearInterval(timer); document.removeEventListener("visibilitychange", refresh); };
  }, [activeRuns.length, revalidate, state]);

  const onLaunched = useCallback((result: CompanyActionResult, selection: CompanyActionSelection) => {
    if (!result.runId) return;
    setSubmitted((previous) => [...previous, {
      ...selection, runId: result.runId!, status: result.status ?? "QUEUED", runUrl: result.runUrl ?? null,
      createdAt: Date.now() / 1000, startTime: null, endTime: null, activeSteps: [], operator: "backoffice",
    }]);
    setSelected(null);
    void revalidate();
  }, [revalidate]);

  return <div className="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-8 p-4 md:p-6">
    <header className="flex flex-wrap items-start justify-between gap-4">
      <div className="space-y-2">
        <h1 className="text-2xl font-semibold tracking-tight">Processing</h1>
        <p className="text-sm text-muted-foreground">Run and monitor workflows for all Swedish companies.</p>
      </div>
      <div className="flex flex-col items-end gap-2">
        <Button variant="outline" size="sm" disabled={state !== "idle"} onClick={() => void revalidate()}>
          {state !== "idle" ? <LoaderCircleIcon className="motion-safe:animate-spin" /> : <RefreshCwIcon />} Refresh
        </Button>
        <span className="text-xs text-muted-foreground">Checked {new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Stockholm", hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(snapshot.checkedAt * 1000)} · auto-refresh</span>
      </div>
    </header>

    {errors.length > 0 && <Alert variant="destructive">
      <CircleAlertIcon />
      <AlertTitle>Dagster status unavailable for {errors.map((area) => area.label).join(", ")}</AlertTitle>
      <AlertDescription>Launches are disabled for these workflows until status can be verified. Any retained runs show their last known status.
        <details className="mt-2"><summary className="cursor-pointer">Connection details</summary>
          {errors.map((area) => <p key={area.value} className="mt-2 break-words">{area.label}: {snapshot.errors[area.value]}</p>)}
        </details>
      </AlertDescription>
    </Alert>}

    {activeRuns.length > 0 && <section aria-labelledby="active-runs-title" className="space-y-3">
      <h2 id="active-runs-title" className="flex items-center gap-2 text-base font-semibold">Active runs <Badge variant="secondary">{activeRuns.length}</Badge></h2>
      <div className="divide-y rounded-lg border">
        {activeRuns.map((run) => <article key={run.runId} className="flex flex-wrap items-center justify-between gap-4 p-4">
          <div className="min-w-0 space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <h3 className="text-sm font-medium">{COMPANY_ACTION_AREAS.find((area) => area.value === run.area)?.label} · {run.operation === "sync" ? "Sync inputs" : "Full processing"}</h3>
              <RunStatus status={run.status} />
              {snapshot.errors[run.area] && <span className="text-xs text-destructive">Last known status</span>}
            </div>
            <p className="break-words text-sm text-muted-foreground">{run.activeSteps.length ? run.activeSteps.map(processingStepLabel).join(" · ") : run.status === "CANCELING" ? "Waiting for the run to stop" : run.status === "STARTED" ? "Preparing the next step" : "Waiting for Dagster to start"}</p>
            <p className="text-xs text-muted-foreground">{run.startTime ? "Started" : "Submitted"} {processingTime(run.startTime ?? run.createdAt)} · {processingDuration(run.startTime ?? run.createdAt, snapshot.checkedAt)} {run.startTime ? "elapsed" : "waiting"} · {run.operator}</p>
          </div>
          <RunLink run={run} />
        </article>)}
      </div>
    </section>}

    <section aria-labelledby="workflows-title" className="space-y-4">
      <div className="space-y-1">
        <h2 id="workflows-title" className="text-base font-semibold">Workflows</h2>
        <p className="max-w-3xl text-sm text-muted-foreground">Sync inputs updates data from ingested sources. Full processing also syncs, then processes and publishes results, reusing unchanged data where supported.</p>
      </div>
      <div className="divide-y border-y">
        {COMPANY_ACTION_AREAS.map((area) => {
          const running = activeRuns.some((run) => run.area === area.value);
          const unavailable = Boolean(snapshot.errors[area.value]);
          return <div key={area.value} role="group" aria-labelledby={`workflow-${area.value}`} className="grid gap-x-5 gap-y-3 py-5 md:grid-cols-[minmax(0,1fr)_auto] md:items-center xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]">
            <div className="space-y-1">
              <h3 id={`workflow-${area.value}`} className="text-sm font-medium">{area.label}</h3>
              <p className="text-sm text-muted-foreground">{area.description}</p>
            </div>
            <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs md:col-start-1 md:row-start-2 xl:col-start-2 xl:row-start-1">
              {(["sync", "process"] as const).map((operation) => {
                const success = snapshot.lastSuccess[area.jobs[operation]];
                return <div className="contents" key={operation}>
                  <dt className="text-muted-foreground">Last {operation === "sync" ? "sync" : "processing"}</dt>
                  <dd>{success ? <span title={`Successful run ${success.runId}`}>{processingTime(success.endTime ?? success.createdAt)}</span> : unavailable ? "Unavailable" : "No successful run"}</dd>
                </div>;
              })}
            </dl>
            <div className="flex flex-col items-start gap-2 md:col-start-2 md:row-span-2 md:row-start-1 md:items-end xl:col-start-3 xl:row-span-1">
              <div className="flex flex-wrap gap-2">
                <Button variant="outline" size="sm" disabled={running || unavailable} aria-label={`${area.label}: Sync inputs`} onClick={() => setSelected({ area: area.value, operation: "sync" })}>Sync inputs</Button>
                <Button variant="secondary" size="sm" disabled={running || unavailable} aria-label={`${area.label}: Full processing`} onClick={() => setSelected({ area: area.value, operation: "process" })}>Full processing</Button>
              </div>
              {(running || unavailable) && <span className="text-xs text-muted-foreground">{unavailable ? "Status unavailable" : "A run is already active"}</span>}
            </div>
          </div>;
        })}
      </div>
    </section>

    <section aria-labelledby="run-history-title" className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div className="space-y-1">
          <h2 id="run-history-title" className="text-base font-semibold">Recent history</h2>
          <p className="text-xs text-muted-foreground">Completed runs from the latest 20 per action. Times are in Stockholm time.</p>
        </div>
        <div className="flex flex-wrap gap-3">
          <Field className="w-auto gap-1"><FieldLabel htmlFor="processing-workflow" className="text-xs">Workflow</FieldLabel>
            <NativeSelect id="processing-workflow" value={workflow} onChange={(event) => { setWorkflow(event.target.value); setPage(0); }}>
              <NativeSelectOption value="all">All workflows</NativeSelectOption>
              {COMPANY_ACTION_AREAS.map((area) => <NativeSelectOption key={area.value} value={area.value}>{area.label}</NativeSelectOption>)}
            </NativeSelect>
          </Field>
          <Field className="w-auto gap-1"><FieldLabel htmlFor="processing-status" className="text-xs">Status</FieldLabel>
            <NativeSelect id="processing-status" value={status} onChange={(event) => { setStatus(event.target.value); setPage(0); }}>
              <NativeSelectOption value="all">All statuses</NativeSelectOption>
              <NativeSelectOption value="SUCCESS">Succeeded</NativeSelectOption>
              <NativeSelectOption value="FAILURE">Failed</NativeSelectOption>
              <NativeSelectOption value="CANCELED">Canceled</NativeSelectOption>
            </NativeSelect>
          </Field>
        </div>
      </div>
      {history.length ? <>
        <Table>
          <TableHeader><TableRow><TableHead>Workflow</TableHead><TableHead>Action</TableHead><TableHead>Status</TableHead><TableHead>Submitted</TableHead><TableHead>Duration</TableHead><TableHead>Operator</TableHead><TableHead><span className="sr-only">Dagster run</span></TableHead></TableRow></TableHeader>
          <TableBody>{history.slice(currentPage * 20, (currentPage + 1) * 20).map((run) => <TableRow key={run.runId}>
            <TableCell className="font-medium">{COMPANY_ACTION_AREAS.find((area) => area.value === run.area)?.label}</TableCell>
            <TableCell>{run.operation === "sync" ? "Sync inputs" : "Full processing"}</TableCell>
            <TableCell><RunStatus status={run.status} /></TableCell>
            <TableCell className="tabular-nums">{processingTime(run.createdAt)}</TableCell>
            <TableCell className="tabular-nums">{run.startTime !== null && run.endTime !== null ? processingDuration(run.startTime, run.endTime) : "—"}</TableCell>
            <TableCell>{run.operator}</TableCell><TableCell className="text-right"><RunLink run={run} /></TableCell>
          </TableRow>)}</TableBody>
        </Table>
        <div className="flex items-center justify-between gap-3 text-xs text-muted-foreground">
          <span>{currentPage * 20 + 1}–{Math.min((currentPage + 1) * 20, history.length)} of {history.length} runs</span>
          {lastPage > 0 && <div className="flex gap-2"><Button variant="outline" size="sm" disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>Previous</Button><Button variant="outline" size="sm" disabled={currentPage === lastPage} onClick={() => setPage(currentPage + 1)}>Next</Button></div>}
        </div>
      </> : <Empty className="border">
        <EmptyHeader><EmptyTitle>{errors.length ? "History is partially unavailable" : workflow !== "all" || status !== "all" ? "No runs match these filters" : "No completed runs yet"}</EmptyTitle>
          <EmptyDescription>{errors.length ? "Refresh after Dagster reconnects to see the latest runs." : "Completed workflow runs will appear here with their status and Dagster link."}</EmptyDescription>
        </EmptyHeader>
      </Empty>}
    </section>

    {selected && <CompanyActionDialog selected={selected} profiles={loaderData.profiles} prompts={selected.area === "domains" ? loaderData.domainPrompts : loaderData.prompts}
      disabled={Boolean(snapshot.errors[selected.area]) || activeRuns.some((run) => run.area === selected.area)}
      onClose={() => setSelected(null)} onLaunched={onLaunched} />}
  </div>;
}
