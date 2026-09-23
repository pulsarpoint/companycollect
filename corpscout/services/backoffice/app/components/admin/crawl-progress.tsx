import { useFetcher } from "react-router";
import { RotateCcwIcon } from "lucide-react";
import { crawlRunStatus, type CrawlProgressSnapshot, type CrawlTaskProgress } from "~/lib/crawl-progress";
import type { DomainCrawlType } from "~/lib/se-domain-selection";
import { Button } from "~/components/ui/button";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Progress } from "~/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export function CrawlProgress({snapshot, error, type}: {snapshot?: CrawlProgressSnapshot | null; error?: string | null; type?: DomainCrawlType}) {
  return <section className="flex flex-col gap-3" aria-labelledby="crawl-progress-heading">
    {type && snapshot && snapshot.tasks.length > 0 && <CrawlTasks tasks={snapshot.tasks} type={type} />}
    <div><h2 id="crawl-progress-heading" className="text-base font-semibold">Processing status</h2>
      <p className="text-sm text-muted-foreground">Latest 10 batches for this crawl type · Refreshes every 5 seconds.</p></div>
    {(error || snapshot?.warning) && <Alert><AlertDescription>{error || snapshot?.warning}</AlertDescription></Alert>}
    {snapshot?.runs.length === 0 && <Empty className="border py-6"><EmptyHeader><EmptyTitle>No batches yet</EmptyTitle><EmptyDescription>Activate saved inputs to start processing. Progress will appear here.</EmptyDescription></EmptyHeader></Empty>}
    {snapshot && snapshot.runs.length > 0 && <Table>
      <TableHeader><TableRow><TableHead>Batch</TableHead><TableHead>Status</TableHead><TableHead>Progress</TableHead><TableHead>Successful</TableHead><TableHead>Unsuccessful</TableHead><TableHead>Skipped · fresh</TableHead><TableHead>Started</TableHead></TableRow></TableHeader>
      <TableBody>{snapshot.runs.map(run => {
        const processed = (run.successful ?? 0) + (run.unsuccessful ?? 0) + (run.skipped ?? 0);
        const hasCounts = run.successful !== null && run.unsuccessful !== null;
        return <TableRow key={run.runId}>
          <TableCell>{run.runUrl ? <a className="underline underline-offset-4" href={run.runUrl} target="_blank" rel="noreferrer">{run.runId.slice(0, 8)}</a> : run.runId.slice(0, 8)}</TableCell>
          <TableCell><Badge variant={run.status === "FAILURE" ? "destructive" : "secondary"}>{crawlRunStatus(run.status)}</Badge></TableCell>
          <TableCell className="min-w-44"><div className="flex flex-col gap-2">
            <span>{hasCounts ? `${processed} processed${run.selected === null ? "" : ` / ${run.selected} selected`}` : "Counts unavailable"}</span>
            {hasCounts && run.selected !== null && <Progress aria-label={`Batch ${run.runId.slice(0, 8)} progress`} value={Math.min(100, processed / run.selected * 100)} />}
          </div></TableCell>
          <TableCell>{run.successful ?? "—"}</TableCell><TableCell>{run.unsuccessful ?? "—"}</TableCell><TableCell>{run.skipped ?? "—"}</TableCell>
          <TableCell className="whitespace-nowrap text-xs">{run.startTime ? new Date(run.startTime * 1000).toLocaleString() : "Waiting to start"}</TableCell>
        </TableRow>;
      })}</TableBody>
    </Table>}
    {snapshot && snapshot.runs.length > 0 && <p className="text-xs text-muted-foreground">Counts update when responses are saved. Skipped inputs are counted when the batch finishes. A finished batch can contain unsuccessful crawls; see individual attempts below.</p>}
  </section>;
}

function CrawlTasks({tasks, type}: {tasks: CrawlTaskProgress[]; type: DomainCrawlType}) {
  return <div className="flex flex-col gap-3">
    <div><h2 className="text-base font-semibold">Crawl tasks</h2>
      <p className="text-sm text-muted-foreground">Selections sent from the domains page. Each task crawls every enabled domain it froze; resumes continue the same task.</p></div>
    <Table>
      <TableHeader><TableRow><TableHead>Task</TableHead><TableHead>Status</TableHead><TableHead>Progress</TableHead><TableHead>Successful</TableHead><TableHead>Unsuccessful</TableHead><TableHead>Skipped · fresh</TableHead><TableHead>Started</TableHead><TableHead>Action</TableHead></TableRow></TableHeader>
      <TableBody>{tasks.map((task) => {
        const hasCounts = task.successful !== null && task.unsuccessful !== null;
        const processed = (task.successful ?? 0) + (task.unsuccessful ?? 0) + (task.skipped ?? 0);
        return <TableRow key={task.executionId}>
          <TableCell><div className="flex flex-col">
            {task.runUrl ? <a className="underline underline-offset-4" href={task.runUrl} target="_blank" rel="noreferrer">{task.taskId.slice(0, 8)}</a> : task.taskId.slice(0, 8)}
            {task.runs > 1 && <span className="text-xs text-muted-foreground">{task.runs} runs</span>}
          </div></TableCell>
          <TableCell><Badge variant={task.status === "FAILURE" ? "destructive" : "secondary"}>{crawlRunStatus(task.status)}</Badge></TableCell>
          <TableCell className="min-w-44"><div className="flex flex-col gap-2">
            <span>{hasCounts ? `${processed} processed${task.selected === null ? "" : ` / ${task.selected} selected`}` : "Counts unavailable"}</span>
            {hasCounts && task.selected ? <Progress aria-label={`Task ${task.taskId.slice(0, 8)} progress`} value={Math.min(100, processed / task.selected * 100)} /> : null}
          </div></TableCell>
          <TableCell>{task.successful ?? "—"}</TableCell><TableCell>{task.unsuccessful ?? "—"}</TableCell><TableCell>{task.skipped ?? "—"}</TableCell>
          <TableCell className="whitespace-nowrap text-xs">{task.startTime ? new Date(task.startTime * 1000).toLocaleString() : "Waiting to start"}</TableCell>
          <TableCell>{task.resumable ? <ResumeTask type={type} executionId={task.executionId} taskId={task.taskId} /> : null}</TableCell>
        </TableRow>;
      })}</TableBody>
    </Table>
  </div>;
}

function ResumeTask({type, executionId, taskId}: {type: DomainCrawlType; executionId: string; taskId: string}) {
  const fetcher = useFetcher<{error: string | null; resumed?: {runUrl: string | null}}>();
  const busy = fetcher.state !== "idle";
  return <fetcher.Form method="post" className="flex flex-col gap-1">
    <input type="hidden" name="intent" value="resume-task" />
    <input type="hidden" name="crawl_type" value={type} />
    <input type="hidden" name="execution_id" value={executionId} />
    <Button type="submit" size="sm" variant="outline" disabled={busy}><RotateCcwIcon data-icon="inline-start" />{busy ? "Resuming…" : "Resume"}<span className="sr-only"> task {taskId.slice(0, 8)}</span></Button>
    {fetcher.data?.error && <span className="text-xs text-destructive">{fetcher.data.error}</span>}
  </fetcher.Form>;
}
