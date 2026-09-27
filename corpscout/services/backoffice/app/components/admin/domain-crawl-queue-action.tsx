import { useEffect, useRef, useState } from "react";
import { useFetcher } from "react-router";
import { ChevronDownIcon } from "lucide-react";
import { QueueImportStatus, useQueueSubmission, type QueueImportReceipt } from "~/components/admin/queue-import-status";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuGroup, DropdownMenuItem } from "~/components/ui/dropdown-menu";
import type { DomainSelection } from "~/lib/domain-selection";
import { DOMAIN_CRAWL_TYPES } from "~/lib/se-domain-selection";

type Reply = ({ ok: true; crawlType: string } & QueueImportReceipt) | { ok: false; error: string };

/** Shared import control: save selected domains in a draft before choosing models. */
export function DomainCrawlQueueAction<S extends DomainSelection<unknown>>({ selection, selectedCount, action, disabled = false, onImported, onBusyChange }: {
  selection: S; selectedCount: number; action: string; disabled?: boolean;
  onImported: (selection: S) => void; onBusyChange?: (busy: boolean) => void;
}) {
  const fetcher = useFetcher<Reply>();
  const submitted = useRef<{ id: string; selection: S; crawlType: string } | null>(null);
  const completed = useRef<string | null>(null);
  const receipt = fetcher.data?.ok ? fetcher.data : null;
  const { state, error } = useQueueSubmission(receipt, "crawler");
  const [retry, setRetry] = useState(false);
  const busy = fetcher.state !== "idle" || Boolean(receipt && !state?.finished);
  const overLimit = selectedCount > 1_000_000;
  useEffect(() => { onBusyChange?.(busy); }, [busy, onBusyChange]);
  useEffect(() => {
    if (state?.status === "SUCCESS" && submitted.current && completed.current !== state.runId) {
      completed.current = state.runId;
      onImported(submitted.current.selection);
      setRetry(false);
    } else if (state?.status === "FAILURE" || state?.status === "CANCELED" || fetcher.data?.ok === false) setRetry(true);
  }, [state, fetcher.data, onImported]);
  function submit(crawlType: string, value: S, retrying = false) {
    const id = retrying && submitted.current ? submitted.current.id : crypto.randomUUID();
    submitted.current = { id, selection: value, crawlType };
    setRetry(false);
    fetcher.submit(JSON.stringify({ action: "add_crawl_inputs", crawlType, selection: value, submissionId: id }), {
      method: "post", encType: "application/json", action,
    });
  }
  return <div className="flex flex-col gap-2">
    <div className="flex flex-wrap gap-2">
      <DropdownMenu>
        <DropdownMenuTrigger render={<Button size="sm" variant="outline" disabled={disabled || busy || overLimit || selectedCount === 0} />}>
          {busy ? "Queuing…" : "Add to crawl queue"}<ChevronDownIcon data-icon="inline-end" />
        </DropdownMenuTrigger>
        <DropdownMenuContent><DropdownMenuGroup>
          {DOMAIN_CRAWL_TYPES.map(type => <DropdownMenuItem key={type.value} disabled={disabled || busy || overLimit || selectedCount === 0}
            onClick={() => { if (!disabled && !busy && !overLimit && selectedCount) submit(type.value, selection); }}>{type.label}</DropdownMenuItem>)}
        </DropdownMenuGroup></DropdownMenuContent>
      </DropdownMenu>
      {retry && submitted.current && <Button size="sm" variant="outline" disabled={disabled || busy}
        onClick={() => submit(submitted.current!.crawlType, submitted.current!.selection, true)}>Retry crawl import</Button>}
    </div>
    {overLimit && <p className="text-sm text-muted-foreground">A crawl draft supports up to 1,000,000 domains. Narrow the filters before adding this selection.</p>}
    {receipt && <QueueImportStatus receipt={receipt} state={state} crawlType={receipt.crawlType} />}
    {(fetcher.data?.ok === false || error) && <Alert variant="destructive"><AlertDescription>{fetcher.data?.ok === false ? fetcher.data.error : error}</AlertDescription></Alert>}
  </div>;
}
