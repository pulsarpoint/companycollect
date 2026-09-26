import { QueueHistoryCompanies } from "~/components/admin/queue-history-companies";
import { braveTaskResultsPath } from "~/lib/brave-results";
import { QueueHistorySources } from "~/components/admin/queue-history-sources";
import { data, redirect, Form, Link, useNavigation, useRevalidator, useSearchParams } from "react-router";
import { PlayIcon, RefreshCwIcon } from "lucide-react";
import type { Route } from "./+types/admin-queue";
import { QueueProcessSheet } from "~/components/admin/queue-process-sheet";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import { CRAWL_QUEUES, QUEUE_PAGE_SIZE, QUEUE_TYPES, parseQueueFilters, queuePath } from "~/lib/queues";
import { QueueRequestError, loadCrawlQueueCounts, loadQueueInputs, loadQueueRuns, loadQueueHistory, startQueueProcessing } from "~/lib/queues.server";
import { DagsterRunConfigValidationError } from "~/lib/dagster.server";

export async function loader({request, params}: Route.LoaderArgs) {
  let filters;
  try { filters = parseQueueFilters(params.type, new URL(request.url).searchParams); }
  catch (error) { throw new Response(error instanceof Error ? error.message : "Invalid queue", {status: 400}); }
  const [inputs, runState, history, crawlCounts] = await Promise.allSettled([loadQueueInputs(filters), loadQueueRuns(filters.task), loadQueueHistory(filters),
    filters.type === "crawler" ? loadCrawlQueueCounts() : Promise.resolve(null)]);
  if (inputs.status === "rejected") throw inputs.reason;
  if ((filters.type === "webtech" || filters.type === "crawler" || filters.type === "brave") && (!filters.task || inputs.value.selectedTotal === 0)) {
    const currentTask = inputs.value.tasks[0]?.task_id ?? "";
    if (currentTask !== filters.task) throw redirect(queuePath(filters, {task: currentTask, page: 1, taskPage: 1}));
  }
  return { filters, inputs: inputs.value,
    crawlCounts: crawlCounts.status === "fulfilled" ? crawlCounts.value : null,
    history: history.status === "fulfilled" ? history.value : [],
    historyError: history.status === "rejected" ? "Task history is unavailable. Refresh to retry." : null,
    runState: runState.status === "fulfilled" ? runState.value : null,
    runError: runState.status === "rejected" ? "Dagster status is unavailable. Refresh before processing." : null };
}

export async function action({request, params}: Route.ActionArgs) {
  // Match the existing operator-only Backoffice actions, rejecting cross-origin form posts.
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({ok: false as const, error: "Invalid request origin."}, {status: 403});
  const form = await request.formData();
  let filters;
  try { filters = parseQueueFilters(params.type, new URLSearchParams({task: String(form.get("task") ?? ""), crawlType: String(form.get("crawlType") ?? "full")})); }
  catch (error) { return data({ok: false as const, error: error instanceof Error ? error.message : "Invalid queue."}, {status: 400}); }
  try {
    return data(await startQueueProcessing(filters, String(form.get("config") ?? ""), String(form.get("requestId") ?? ""), process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice"));
  } catch (error) {
    if (error instanceof Response) throw error;
    const invalid = error instanceof QueueRequestError || error instanceof DagsterRunConfigValidationError;
    return data({ok: false as const, error: invalid ? error.message : "Could not confirm processing submission. Refresh and check Dagster before retrying; retrying this sheet keeps the same request ID."}, {status: invalid ? 400 : 502});
  }
}

export function meta() { return [{title: "Queues | CompanyCollect"}]; }

export default function AdminQueue({loaderData}: Route.ComponentProps) {
  const {filters, inputs, runState, runError, history, historyError, crawlCounts} = loaderData;
  const [searchParams, setSearchParams] = useSearchParams();
  const identity = `${filters.type}:${filters.crawlType}:${filters.task}`;
  const busy = useNavigation().state !== "idle";
  const revalidator = useRevalidator();
  const processingBlocked = !filters.task ? ((filters.type === "webtech" || filters.type === "crawler" || filters.type === "brave") ? "The queue is empty. Add inputs to prepare the next task." : "Choose a task below to configure processing.")
    : inputs.selectedTotal === 0 ? "This task has no inputs to process."
    : !runState ? "Refresh to check Dagster status before processing."
    : runState.active ? "This task has an active Dagster run. Wait for it to finish, then refresh."
    : null;
  const canProcess = processingBlocked === null;
  const configureProcessing = (open: boolean) => {
    const params = new URLSearchParams(searchParams);
    if (open) params.set("configure", "1");
    else params.delete("configure");
    setSearchParams(params, {replace: true, preventScrollReset: true});
  };
  return <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={busy}>
    <header className="flex flex-wrap items-start justify-between gap-4">
      <div className="flex flex-col gap-2"><h1 className="text-2xl font-semibold">Queues</h1>
        <p className="max-w-3xl text-sm text-muted-foreground">Review queued inputs, configure processing, and follow previous task runs.</p></div>
      <Button variant="outline" disabled={revalidator.state !== "idle"} onClick={() => revalidator.revalidate()}><RefreshCwIcon data-icon="inline-start" />Refresh</Button>
    </header>
    <Tabs value={filters.type}><TabsList aria-label="Queue types" className="max-w-full overflow-x-auto">
      {QUEUE_TYPES.map(queue => <TabsTrigger key={queue.id} value={queue.id} nativeButton={false} render={<Link to={`/admin/queues/${queue.id}`} />}>{queue.label}</TabsTrigger>)}
    </TabsList></Tabs>
    {filters.type === "crawler" && <Tabs value={filters.crawlType}><TabsList aria-label="Crawler queues" className="max-w-full overflow-x-auto">
      {CRAWL_QUEUES.map(queue => <TabsTrigger key={queue.id} value={queue.id} nativeButton={false}
        render={<Link to={queuePath(filters, {crawlType: queue.id, task: "", search: "", page: 1, taskPage: 1})} />}>
        {queue.label}<Badge variant={crawlCounts?.[queue.id] ? "default" : "outline"} aria-label={crawlCounts ? `${crawlCounts[queue.id]} queued ${crawlCounts[queue.id] === 1 ? "entry" : "entries"}` : "Queued entries unavailable"}>{crawlCounts ? crawlCounts[queue.id].toLocaleString() : "–"}</Badge>
      </TabsTrigger>)}
    </TabsList></Tabs>}

    <section className="flex flex-col gap-4" aria-label="Queue inputs">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-col gap-1"><h2 className="text-lg font-semibold">{QUEUE_TYPES.find(queue => queue.id === filters.type)?.label} inputs</h2>
          <p className="text-sm text-muted-foreground">{inputs.totalInputs.toLocaleString()} inputs · {inputs.totalTasks.toLocaleString()} tasks</p></div>
        {filters.task && <Button disabled={!canProcess} onClick={() => configureProcessing(true)}><PlayIcon data-icon="inline-start" />Configure processing</Button>}
      </div>
      {processingBlocked && <p className="text-sm text-muted-foreground" role="status">{processingBlocked}</p>}
      <Form method="get" key={JSON.stringify(filters)}>
        <FieldGroup className="sm:flex-row sm:flex-wrap sm:items-end">
          {filters.type === "crawler" && <input type="hidden" name="crawlType" value={filters.crawlType} />}
          <input type="hidden" name="task" value={filters.task} />
          <Field className="sm:max-w-xs"><FieldLabel htmlFor="queue-search">Search inputs</FieldLabel><Input id="queue-search" name="search" defaultValue={filters.search} placeholder={filters.type === "brave" ? "Company name or country:ID" : filters.type === "ip-enrichment" ? "IP address" : "Domain or page URL"} /></Field>
          <Button type="submit" disabled={busy}>Apply</Button>
          <Button variant="ghost" nativeButton={false} render={<Link to={queuePath(filters, {task: "", search: "", page: 1, taskPage: 1})} />}>Reset</Button>
        </FieldGroup>
      </Form>
      {(((filters.type !== "webtech" && filters.type !== "crawler" && filters.type !== "brave") && !filters.task) || ((filters.type === "webtech" || filters.type === "crawler" || filters.type === "brave") && inputs.totalTasks > 1)) && <>
        <h3 className="text-sm font-medium">Queues awaiting processing</h3>
        <Table><TableHeader><TableRow><TableHead>Task</TableHead><TableHead>Inputs</TableHead><TableHead>Last submission (UTC)</TableHead><TableHead>Processing</TableHead></TableRow></TableHeader>
          <TableBody>{inputs.tasks.map(task => <TableRow key={task.task_id}><TableCell>{task.task_id ? <Link className="font-mono text-xs underline" to={queuePath(filters, {task: task.task_id, page: 1, search: ""})}>{task.task_id}</Link> : "Legacy inputs without a task"}</TableCell><TableCell>{Number(task.total).toLocaleString()}</TableCell><TableCell>{task.submitted_at || "Not recorded"}</TableCell><TableCell>
            {task.task_id && <Button size="sm" variant="outline" nativeButton={false} render={<Link to={`${queuePath(filters, {task: task.task_id, page: 1, search: ""})}&configure=1`} />}><PlayIcon data-icon="inline-start" />Configure processing</Button>}
          </TableCell></TableRow>)}
          {!inputs.tasks.length && <TableRow><TableCell colSpan={4}>No inputs have been submitted to this queue.</TableCell></TableRow>}</TableBody>
        </Table>
        {inputs.totalTasks > QUEUE_PAGE_SIZE && <div className="flex items-center justify-end gap-3"><span className="text-sm text-muted-foreground">Task page {filters.taskPage}</span>
          {filters.taskPage > 1 && <Button variant="outline" nativeButton={false} render={<Link to={queuePath(filters, {taskPage: filters.taskPage - 1})} />}>Previous tasks</Button>}
          {filters.taskPage * QUEUE_PAGE_SIZE < inputs.totalTasks && <Button variant="outline" nativeButton={false} render={<Link to={queuePath(filters, {taskPage: filters.taskPage + 1})} />}>Next tasks</Button>}
        </div>}
      </>}
      {filters.type === "crawler" && <p className="text-sm text-muted-foreground">These are task selections. <Link className="underline" to="/admin/crawls">Open saved crawler requests and browser settings</Link>.</p>}
      {filters.task && <div className="flex flex-col gap-2">
        <p className="text-sm"><strong>{inputs.selectedTotal.toLocaleString()} inputs</strong> in this task. Processing uses the whole task; search only filters the preview.</p>
        <p className="text-sm text-muted-foreground">{(filters.type === "webtech" || filters.type === "crawler" || filters.type === "brave") ? "Inputs can be appended while the task is a draft. Dagster checks submissions and freezes the task when execution begins." : "This queue uses a fixed input selection."}</p>
      </div>}
      {runError && <Alert variant="destructive"><AlertTitle>Run status unavailable</AlertTitle><AlertDescription>{runError}</AlertDescription></Alert>}
      {(filters.type !== "webtech" && filters.type !== "crawler" && filters.type !== "brave") && runState && runState.runs.length > 0 && <div className="flex flex-col gap-2"><h3 className="text-sm font-medium">Recent task runs</h3>
        {runState.runs.map(run => <div key={run.runId} className="flex flex-wrap items-center gap-3 text-sm"><Badge variant="outline">{run.status}</Badge><span>{run.job}</span>{run.runUrl && <a className="font-mono text-xs underline" href={run.runUrl} target="_blank" rel="noreferrer">{run.runId}</a>}</div>)}
      </div>}
      <p className="text-sm text-muted-foreground" role="status">{inputs.matching.toLocaleString()} matching inputs · Source: <code>{inputs.table}</code></p>
      <Table><TableHeader><TableRow><TableHead>{filters.type === "brave" ? "Company" : filters.type === "ip-enrichment" ? "IP address" : "Domain / page"}</TableHead>{(filters.type !== "webtech" && filters.type !== "crawler" && filters.type !== "brave") && <TableHead>Task</TableHead>}<TableHead>Source / record</TableHead><TableHead>Submitted (UTC)</TableHead></TableRow></TableHeader>
        <TableBody>{inputs.rows.map(row => <TableRow key={`${row.task_id}:${row.input_id}`}>
          <TableCell className="max-w-lg whitespace-normal"><span className="font-medium">{row.target}</span><p className="break-all text-xs text-muted-foreground">{row.detail}</p></TableCell>
          {(filters.type !== "webtech" && filters.type !== "crawler" && filters.type !== "brave") && <TableCell className="max-w-64 whitespace-normal">{row.task_id ? <Link className="break-all font-mono text-xs underline" to={queuePath(filters, {task: row.task_id, page: 1})}>{row.task_id}</Link> : "No task"}</TableCell>}
          <TableCell className="max-w-xs whitespace-normal">{row.source}<p className="break-all text-xs text-muted-foreground">{row.source_record_id}</p></TableCell><TableCell>{row.submitted_at || "Not recorded"}</TableCell>
        </TableRow>)}{!inputs.rows.length && <TableRow><TableCell colSpan={(filters.type === "webtech" || filters.type === "crawler" || filters.type === "brave") ? 3 : 4} className="h-24 text-center text-muted-foreground">No inputs match this selection.</TableCell></TableRow>}</TableBody>
      </Table>
      <div className="flex items-center justify-between gap-3"><span className="text-sm text-muted-foreground">Page {filters.page} of {Math.max(1, Math.ceil(inputs.matching / QUEUE_PAGE_SIZE)).toLocaleString()}</span><div className="flex gap-2">
        {filters.page > 1 && <Button variant="outline" nativeButton={false} render={<Link to={queuePath(filters, {page: filters.page - 1})} />}>Previous</Button>}
        {filters.page * QUEUE_PAGE_SIZE < inputs.matching && <Button variant="outline" nativeButton={false} render={<Link to={queuePath(filters, {page: filters.page + 1})} />}>Next</Button>}
      </div></div>
    </section>
    <section className="flex flex-col gap-3" aria-label="Task history">
      <h2 className="text-lg font-semibold">Recent task history</h2>
      <p className="text-sm text-muted-foreground">Latest processing run for each task{filters.type === "crawler" ? " across all crawl types" : ""}. Completed draft inputs are removed from the queues; results and history remain available.</p>
      {historyError && <Alert variant="destructive"><AlertDescription>{historyError}</AlertDescription></Alert>}
      <Table><TableHeader><TableRow><TableHead>Started (UTC)</TableHead>{(filters.type === "crawler" || filters.type === "webtech" || filters.type === "brave") && <TableHead>{filters.type === "brave" ? "Source companies" : "Source domains / websites"}</TableHead>}<TableHead>Task</TableHead>{filters.type === "crawler" && <TableHead>Crawl type</TableHead>}<TableHead>Processing status</TableHead><TableHead>Details</TableHead></TableRow></TableHeader>
        <TableBody>{history.map(task => <TableRow key={`${task.crawlType}:${task.taskId}`}>
          <TableCell>{task.startedAt ? task.startedAt.replace("T", " ").replace(/\.\d+Z$/, "") : "Not started"}</TableCell>
          {filters.type === "brave" && <TableCell className="align-top"><QueueHistoryCompanies taskId={task.taskId} sources={task.sources} error={task.sourcesError} retryFailed={(task.failedPages ?? 0) > 0} /></TableCell>}
          {(filters.type === "crawler" || filters.type === "webtech") && <TableCell className="align-top"><QueueHistorySources type={filters.type} taskId={task.taskId} crawlType={task.crawlType} sources={task.sources} error={task.sourcesError} /></TableCell>}
          <TableCell className="font-mono text-xs">{task.taskId}</TableCell>{filters.type === "crawler" && <TableCell>{CRAWL_QUEUES.find(queue => queue.id === task.crawlType)?.label}</TableCell>}<TableCell><Badge variant="outline">{task.outcome === "completed_with_errors" ? "Completed with errors" : task.outcome === "completed" ? "Completed" : task.status}</Badge>{task.failedPages != null && task.failedPages > 0 && <p className="text-xs text-muted-foreground">{task.failedPages} {filters.type === "crawler" ? "crawl errors" : filters.type === "brave" ? "search errors" : "page errors"} · results saved</p>}{task.skippedPages != null && task.skippedPages > 0 && <p className="text-xs text-muted-foreground">{task.skippedPages} {task.skippedPages === 1 ? "input skipped" : "inputs skipped"}</p>}</TableCell>
          <TableCell><div className="flex flex-col items-start gap-2">
            {filters.type === "brave" && <Button variant="outline" size="sm" nativeButton={false} render={<Link to={braveTaskResultsPath(task.taskId)} />}>View results</Button>}
            {task.runUrl && <a className="underline" href={task.runUrl} target="_blank" rel="noreferrer">View in Dagster</a>}
          </div></TableCell>
        </TableRow>)}{!history.length && !historyError && <TableRow><TableCell colSpan={filters.type === "crawler" ? 6 : (filters.type === "webtech" || filters.type === "brave") ? 5 : 4}>No processing runs yet.</TableCell></TableRow>}</TableBody>
      </Table>
    </section>
    {filters.task && searchParams.get("configure") === "1" && <QueueProcessSheet key={identity} filters={filters} total={inputs.selectedTotal} asset={inputs.asset} blockedReason={processingBlocked} onClose={() => configureProcessing(false)} />}
  </div>;
}
