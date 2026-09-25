import { useState } from "react";
import { useFetcher } from "react-router";
import { Button } from "~/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "~/components/ui/dialog";
import type { CrawlQueueType } from "~/lib/queues";
import type { QueueSourceSummary } from "~/lib/queue-history.server";
import type { loader } from "~/routes/admin-queue-sources";

function Website({domain, url}: {domain: string; url: string}) {
  const safe = /^https?:\/\//i.test(url);
  return <div className="min-w-0"><div className="font-medium">{domain}</div>
    {safe ? <a className="break-all text-xs text-muted-foreground underline" href={url} target="_blank" rel="noreferrer">{url}</a> : <p className="break-all text-xs text-muted-foreground">{url}</p>}
  </div>;
}

export function QueueHistorySources({type, taskId, crawlType, sources, error}: {
  type: "webtech" | "crawler"; taskId: string; crawlType: CrawlQueueType | null; sources: QueueSourceSummary | null; error: boolean;
}) {
  const [open, setOpen] = useState(false);
  const fetcher = useFetcher<typeof loader>();
  const total = Number(sources?.total ?? 0);
  const load = (page: number) => void fetcher.load(`/admin/queues/${type}/sources?${new URLSearchParams({task: taskId, crawlType: crawlType ?? "full", page: String(page)})}`);
  if (error) return <p className="text-xs text-muted-foreground">Source websites unavailable. Refresh to retry.</p>;
  if (!sources || !total) return <p className="text-xs text-muted-foreground">Original websites were not retained for this task.</p>;
  return <div className="flex min-w-52 max-w-sm flex-col gap-2 whitespace-normal">
    {sources.preview.map(([domain, url]) => <Website key={`${domain}:${url}`} domain={domain} url={url} />)}
    {!sources.complete && <p className="text-xs text-muted-foreground">From saved results; skipped websites may be missing.</p>}
    <Button size="sm" variant="ghost" className="self-start" onClick={() => {setOpen(true); load(1);}}>{`View ${total.toLocaleString()} ${total === 1 ? "website" : "websites"}`}</Button>
    <Dialog open={open} onOpenChange={setOpen}><DialogContent className="sm:max-w-2xl">
      <DialogHeader><DialogTitle>Source websites</DialogTitle><DialogDescription>Task {taskId} · {total.toLocaleString()} websites{!sources.complete ? " · Available saved results only" : ""}</DialogDescription></DialogHeader>
      <div className="flex max-h-[60vh] flex-col gap-4 overflow-y-auto" aria-busy={fetcher.state !== "idle"}>
        {fetcher.data?.error ? <p role="alert">{fetcher.data.error}</p> : fetcher.data?.rows.map(row => <div key={`${row.domain}:${row.website_url}`} className="flex flex-col gap-1"><Website domain={row.domain} url={row.website_url} />{row.sources.length > 0 && <p className="break-all text-xs text-muted-foreground">Source: {row.sources.join(", ")}</p>}</div>)}
        {fetcher.state !== "idle" && <p role="status">Loading websites…</p>}
      </div>
      <div className="flex items-center justify-between gap-3"><span className="text-xs text-muted-foreground">Page {fetcher.data?.page ?? 1} of {Math.max(1, Math.ceil(total / 50)).toLocaleString()}</span>
        <div className="flex gap-2">{fetcher.data?.error ? <Button variant="outline" onClick={() => load(fetcher.data?.page ?? 1)}>Retry</Button> : <>
          <Button variant="outline" disabled={fetcher.state !== "idle" || (fetcher.data?.page ?? 1) <= 1} onClick={() => load((fetcher.data?.page ?? 1) - 1)}>Previous</Button>
          <Button variant="outline" disabled={fetcher.state !== "idle" || !fetcher.data || fetcher.data.page * 50 >= total} onClick={() => load((fetcher.data?.page ?? 1) + 1)}>Next</Button>
        </>}</div></div>
    </DialogContent></Dialog>
  </div>;
}
