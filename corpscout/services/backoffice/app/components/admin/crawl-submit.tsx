import { useEffect, useRef, useState } from "react";
import { useFetcher } from "react-router";
import { SendIcon } from "lucide-react";
import type { action } from "~/routes/admin-crawls";
import type { CrawlPublishReceipt } from "~/lib/crawler";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from "~/components/ui/dialog";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Textarea } from "~/components/ui/textarea";

export function CrawlSubmit({enabled, onPublished}: {enabled: boolean; onPublished: (receipt: CrawlPublishReceipt) => void}) {
  const fetcher = useFetcher<typeof action>();
  const [open, setOpen] = useState(false);
  const [body, setBody] = useState("");
  const handled = useRef<unknown>(null);
  const busy = fetcher.state !== "idle";

  useEffect(() => {
    if (!fetcher.data || handled.current === fetcher.data) return;
    handled.current = fetcher.data;
    if ("receipt" in fetcher.data && fetcher.data.receipt) {
      onPublished(fetcher.data.receipt);
      setOpen(false);
      setBody("");
    }
  }, [fetcher.data, onPublished]);

  function changeOpen(next: boolean) {
    if (busy) return;
    if (next && !body) setBody(JSON.stringify({
      request_id: `backoffice-${crypto.randomUUID()}`,
      url: "https://www.novelic.com/",
      crawl: "full",
      save_artifacts: true,
      challenge_agent_max_runs: 3,
      challenge_agent_model: "deepseek-flash",
    }, null, 2));
    setOpen(next);
  }

  return <Dialog open={open} onOpenChange={changeOpen}>
    <DialogTrigger render={<Button disabled={!enabled} title={enabled ? undefined : "Test submissions are disabled on Backoffice."} />}>
      <SendIcon data-icon="inline-start" />New test crawl
    </DialogTrigger>
    <DialogContent className="sm:max-w-2xl">
      <DialogHeader><DialogTitle>Send a test crawl</DialogTitle>
        <DialogDescription>Full crawl discovers company, contact, jobs and financial source pages. Collected content and artifacts appear in the crawl results.</DialogDescription>
      </DialogHeader>
      <fetcher.Form method="post" className="flex flex-col gap-4">
        <input type="hidden" name="intent" value="submit" />
        <FieldGroup><Field data-invalid={Boolean(fetcher.data?.error)}>
          <FieldLabel htmlFor="crawl-request-json">Crawl request JSON</FieldLabel>
          <Textarea id="crawl-request-json" name="body" value={body} onChange={event => setBody(event.target.value)}
            rows={12} className="max-h-[50vh] font-mono" required disabled={busy} aria-invalid={Boolean(fetcher.data?.error)} spellCheck={false} />
          <FieldDescription>Edit the website and any request options. For a page limit, add <code>"config": {"{"}"max_pages": 5{"}"}</code>. To use pages or custom instructions, remove <code>"crawl": "full"</code>.</FieldDescription>
          <FieldDescription>CAPTCHA agent budget: 3–1000 runs. To test GLM, set <code>challenge_agent_model</code> to <code>z-ai/glm-5.3-flash</code>. Retries double the budget after exhaustion unless you supply an override.</FieldDescription>
        </Field></FieldGroup>
        {fetcher.data?.error && <Alert variant="destructive"><AlertTitle>Request not confirmed</AlertTitle><AlertDescription>{fetcher.data.error}</AlertDescription></Alert>}
        <p className="text-xs text-muted-foreground">Sent to the crawler REST API for testing. Keep the same request ID when retrying an uncertain submission; use a new ID for a new crawl.</p>
        <DialogFooter>
          <Button type="button" variant="outline" disabled={busy} onClick={() => setOpen(false)}>Close</Button>
          <Button type="submit" disabled={busy}><SendIcon data-icon="inline-start" />{busy ? "Submitting…" : "Send to crawler"}</Button>
        </DialogFooter>
      </fetcher.Form>
    </DialogContent>
  </Dialog>;
}
