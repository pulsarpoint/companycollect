import { useState } from "react";
import { Link, useFetcher } from "react-router";
import { QueueImportStatus, useQueueSubmission } from "~/components/admin/queue-import-status";
import type { action } from "~/routes/admin-brave-retry-failed";
import { Button } from "~/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "~/components/ui/dialog";
import type { QueueSourceSummary } from "~/lib/queue-history.server";
import type { loader } from "~/routes/admin-brave-company-sources";

function Company({name, country, id}: {name: string; country: string; id: string}) {
  return <div><Link className="font-medium underline" to={`/admin/${country.toLowerCase()}/company/${encodeURIComponent(id)}`}>{name}</Link><p className="text-xs text-muted-foreground">{country}:{id}</p></div>;
}

export function QueueHistoryCompanies({taskId, sources, error, retryFailed}: {taskId: string; sources: QueueSourceSummary | null; error: boolean; retryFailed: boolean}) {
  const [open, setOpen] = useState(false);
  const fetcher = useFetcher<typeof loader>();
  const retry = useFetcher<typeof action>();
  const [submissionId] = useState(() => crypto.randomUUID());
  const receipt = retry.data?.ok ? retry.data : null;
  const {state} = useQueueSubmission(receipt, "brave");
  const total = Number(sources?.total ?? 0);
  const load = (page: number) => void fetcher.load(`/admin/brave/company-sources?${new URLSearchParams({task: taskId, page: String(page)})}`);
  if (error) return <p className="text-xs text-muted-foreground">Source companies unavailable. Refresh to retry.</p>;
  if (!sources || !total) return <p className="text-xs text-muted-foreground">No retained companies for this task.</p>;
  return <div className="flex min-w-52 max-w-sm flex-col gap-2 whitespace-normal">
    {sources.preview.map(([name, key]) => {const [country, id] = key.split(":"); return <Company key={key} name={name} country={country} id={id} />;})}
    {!sources.complete && <p className="text-xs text-muted-foreground">From saved results; skipped companies may be missing.</p>}
    <Button size="sm" variant="ghost" className="self-start" onClick={() => {setOpen(true); load(1);}}>View {total.toLocaleString()} companies</Button>
    {retryFailed && <Button size="sm" variant="outline" disabled={retry.state !== "idle" || Boolean(receipt && state?.status !== "FAILURE" && state?.status !== "CANCELED")} onClick={() => retry.submit({task: taskId, submissionId}, {method: "post", action: "/admin/brave/retry-failed"})}>Queue failed companies again</Button>}
    {receipt && <QueueImportStatus receipt={receipt} state={state} type="brave" />}
    {retry.data && !retry.data.ok && <p role="alert">{retry.data.error}</p>}
    <Dialog open={open} onOpenChange={setOpen}><DialogContent className="sm:max-w-2xl">
      <DialogHeader><DialogTitle>Source companies</DialogTitle><DialogDescription>Task {taskId} · {total.toLocaleString()} companies</DialogDescription></DialogHeader>
      <div className="flex max-h-[60vh] flex-col gap-4 overflow-y-auto" aria-busy={fetcher.state !== "idle"}>
        {fetcher.data?.error ? <p role="alert">{fetcher.data.error}</p> : fetcher.data?.rows.map(row => <div key={`${row.country_code}:${row.company_id}`}><Company name={row.company_name} country={row.country_code} id={row.company_id} /><p className="text-xs text-muted-foreground">{row.sources.join(", ")}</p></div>)}
        {fetcher.state !== "idle" && <p role="status">Loading companies…</p>}
      </div>
      <div className="flex items-center justify-between gap-3"><span className="text-xs text-muted-foreground">Page {fetcher.data?.page ?? 1} of {Math.max(1, Math.ceil(total / 50)).toLocaleString()}</span>
        <div className="flex gap-2">
          {fetcher.data?.error ? <Button variant="outline" onClick={() => load(fetcher.data?.page ?? 1)}>Retry</Button> : <>
            <Button variant="outline" disabled={fetcher.state !== "idle" || (fetcher.data?.page ?? 1) <= 1} onClick={() => load((fetcher.data?.page ?? 1) - 1)}>Previous</Button>
            <Button variant="outline" disabled={fetcher.state !== "idle" || !fetcher.data || fetcher.data.page * 50 >= total} onClick={() => load((fetcher.data?.page ?? 1) + 1)}>Next</Button>
          </>}
        </div>
      </div>
    </DialogContent></Dialog>
  </div>;
}
