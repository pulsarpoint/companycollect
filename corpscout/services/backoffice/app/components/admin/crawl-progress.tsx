import { crawlRunStatus, type CrawlProgressSnapshot } from "~/lib/crawl-progress";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Progress } from "~/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export function CrawlProgress({snapshot, error}: {snapshot?: CrawlProgressSnapshot | null; error?: string | null}) {
  return <section className="flex flex-col gap-3" aria-labelledby="crawl-progress-heading">
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
