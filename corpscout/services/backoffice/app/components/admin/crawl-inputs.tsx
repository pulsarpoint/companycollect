import { useEffect, useRef, useState, type ReactNode } from "react";
import { Form, useFetcher, useSearchParams } from "react-router";
import { PlayIcon } from "lucide-react";
import type { action } from "~/routes/admin-crawls";
import type { CrawlInputsSnapshot } from "~/lib/crawl-inputs";
import { DOMAIN_CRAWL_TYPES, type DomainCrawlType } from "~/lib/se-domain-selection";
import { CrawlSettingsFields } from "~/components/admin/crawl-settings-fields";
import { Badge } from "~/components/ui/badge";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription, SheetFooter } from "~/components/ui/sheet";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "~/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export function CrawlInputs({snapshot, children}: {snapshot: CrawlInputsSnapshot; children?: ReactNode}) {
  const [search, setSearch] = useSearchParams();
  const [selection, setSelection] = useState<string[]>([]);
  const [activation, setActivation] = useState<string[] | null>(null);
  const enabledDomains = snapshot.rows.filter(row => row.enabled).map(row => row.domain);
  const selected = selection.filter(domain => enabledDomains.includes(domain));
  const stats = snapshot.stats.find(item => item.type === snapshot.type);
  const label = DOMAIN_CRAWL_TYPES.find(item => item.value === snapshot.type)!.label;
  function navigate(values: Record<string, string>) {
    const next = new URLSearchParams(search);
    for (const [key, value] of Object.entries(values)) next.set(key, value);
    setSelection([]);
    setSearch(next);
  }
  return <Tabs value={snapshot.type} onValueChange={value => navigate({input_type: String(value), input_offset: "0"})}>
    <TabsList variant="line" aria-label="Crawl type">
      {(["jobs", "site_info", "full"] as const).map(type => <TabsTrigger key={type} value={type}>
        {DOMAIN_CRAWL_TYPES.find(item => item.value === type)!.label}
        <Badge variant="secondary">{snapshot.stats.find(item => item.type === type)?.total.toLocaleString() ?? "—"}</Badge>
      </TabsTrigger>)}
    </TabsList>
    <TabsContent value={snapshot.type} className="flex flex-col gap-6 pt-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div><h2 className="text-lg font-semibold">{label} inputs</h2>
          <p className="text-sm text-muted-foreground">{stats?.total.toLocaleString() ?? "—"} saved domains · {stats?.enabled.toLocaleString() ?? "—"} enabled · {stats ? stats.total - stats.enabled : "—"} disabled</p></div>
        <Button disabled={enabledDomains.length === 0} onClick={() => setActivation(selected.length > 0 ? selected : enabledDomains)}>
          <PlayIcon data-icon="inline-start" />Activate {selected.length > 0 ? `${selected.length} selected` : `${enabledDomains.length} shown`}
        </Button>
      </div>
      <CrawlActivationSheet type={snapshot.type} domains={activation} onClose={() => setActivation(null)} />
      {children}
      <section className="flex flex-col gap-3" aria-labelledby="crawl-inputs-heading">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div><h2 id="crawl-inputs-heading" className="text-base font-semibold">Saved domains</h2><p className="text-sm text-muted-foreground">Select individual inputs or activate the enabled domains on this page.</p></div>
          <Form method="get" className="w-full sm:w-80" key={`${snapshot.type}:${snapshot.domain}`}>
            {[...search.entries()].filter(([key]) => !key.startsWith("input_")).map(([key,value]) => <input key={key} type="hidden" name={key} value={value} />)}
            <input type="hidden" name="input_type" value={snapshot.type} />
            <FieldGroup className="flex-row items-end gap-2">
              <Field className="min-w-0 flex-1"><FieldLabel htmlFor="input-domain" className="sr-only">Saved domain</FieldLabel><Input id="input-domain" name="input_domain" defaultValue={snapshot.domain} placeholder="Filter saved domains" /></Field>
              <Button type="submit" variant="outline" className="shrink-0">Filter inputs</Button>
            </FieldGroup>
          </Form>
        </div>
        <Table><TableHeader><TableRow>
          <TableHead className="w-10"><Checkbox aria-label="Select enabled inputs on this page" disabled={enabledDomains.length === 0} checked={enabledDomains.length > 0 && selected.length === enabledDomains.length} indeterminate={selected.length > 0 && selected.length < enabledDomains.length} onCheckedChange={checked => setSelection(checked ? enabledDomains : [])} /></TableHead>
          <TableHead>Saved domain</TableHead><TableHead>Status</TableHead><TableHead>Priority</TableHead><TableHead>Pages</TableHead><TableHead>Browser</TableHead><TableHead>Action</TableHead>
        </TableRow></TableHeader>
          <TableBody>{snapshot.rows.map(row => <TableRow key={row.domain} data-state={selected.includes(row.domain) ? "selected" : undefined}>
            <TableCell><Checkbox aria-label={`Select ${row.domain}`} disabled={!row.enabled} checked={selected.includes(row.domain)} onCheckedChange={checked => setSelection(checked ? [...selected, row.domain] : selected.filter(domain => domain !== row.domain))} /></TableCell>
            <TableCell><a className="font-medium underline-offset-4 hover:underline" href={row.website_url} target="_blank" rel="noreferrer">{row.domain}</a></TableCell>
            <TableCell><Badge variant={row.enabled ? "secondary" : "outline"}>{row.enabled ? "Enabled" : "Disabled"}</Badge></TableCell>
            <TableCell>{row.priority}</TableCell><TableCell>{snapshot.type === "site_info" ? "Basic info only" : row.page_mode === "discover" ? "Discover pages" : `${row.pages.length} explicit pages`}</TableCell>
            <TableCell>{row.headless ? "Headless" : "Visible"} · {row.proxy_route}</TableCell>
            <TableCell><Button size="sm" variant="outline" disabled={!row.enabled} onClick={() => setActivation([row.domain])}>Activate<span className="sr-only"> {row.domain}</span></Button></TableCell>
          </TableRow>)}{snapshot.rows.length === 0 && <TableRow><TableCell colSpan={7} className="py-8 text-center text-muted-foreground">No saved domains match this filter. Add inputs from the company domains page.</TableCell></TableRow>}</TableBody>
        </Table>
        <div className="flex flex-wrap items-center justify-between gap-3"><p className="text-sm text-muted-foreground">{snapshot.total.toLocaleString()} matching inputs · Highest priority first</p>
          <div className="flex gap-2"><Button variant="outline" disabled={snapshot.offset === 0} onClick={() => navigate({input_offset: String(Math.max(0, snapshot.offset - snapshot.limit))})}>Previous inputs</Button>
            <Button variant="outline" disabled={snapshot.offset + snapshot.limit >= snapshot.total} onClick={() => navigate({input_offset: String(snapshot.offset + snapshot.limit)})}>Next inputs</Button></div>
        </div>
      </section>
    </TabsContent>
  </Tabs>;
}

function CrawlActivationSheet({type, domains, onClose}: {type: DomainCrawlType; domains: string[] | null; onClose: () => void}) {
  const fetcher = useFetcher<typeof action>();
  const batch = useRef<{key: string; id: string} | null>(null);
  const handled = useRef<unknown>(null);
  const busy = fetcher.state !== "idle";
  const receipt = fetcher.data && "batch" in fetcher.data ? fetcher.data.batch : null;
  const label = DOMAIN_CRAWL_TYPES.find(item => item.value === type)!.label;
  useEffect(() => {
    if (receipt && fetcher.state === "idle" && handled.current !== receipt) {
      handled.current = receipt;
      batch.current = null;
      onClose();
    }
  }, [receipt, fetcher.state, onClose]);
  return <>
    {receipt && <Alert><AlertTitle>{label} batch queued</AlertTitle><AlertDescription>
      <p>{receipt.count} {receipt.count === 1 ? "input" : "inputs"} submitted. Follow processing status below.</p>
      {receipt.runUrl && <a className="underline" href={receipt.runUrl} target="_blank" rel="noreferrer">View Dagster run</a>}
    </AlertDescription></Alert>}
    <Sheet open={domains !== null} onOpenChange={open => {if (!open && !busy) onClose();}}>
      <SheetContent className="data-[side=right]:w-full data-[side=right]:sm:max-w-xl" showCloseButton={!busy}>
        <SheetHeader><SheetTitle>Activate {label.toLowerCase()}</SheetTitle>
          <SheetDescription>Configure this batch, then start processing {domains?.length ?? 0} saved {domains?.length === 1 ? "domain" : "domains"}.</SheetDescription>
        </SheetHeader>
        <form className="flex min-h-0 flex-1 flex-col" onSubmit={event => {
          event.preventDefault();
          if (busy || !domains?.length) return;
          const form = new FormData(event.currentTarget);
          const key = JSON.stringify([type, [...domains].sort(), [...form.entries()]]);
          if (!batch.current || batch.current.key !== key) batch.current = {key, id: crypto.randomUUID()};
          form.set("intent", "start-inputs"); form.set("crawl_type", type);
          form.set("domains", JSON.stringify(domains)); form.set("batch_id", batch.current.id);
          fetcher.submit(form, {method: "post"});
        }}>
          <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-4 pb-4">
            <details className="text-sm"><summary className="cursor-pointer">Selected domains ({domains?.length ?? 0})</summary><p className="mt-2 break-words text-muted-foreground">{domains?.join(", ")}</p></details>
            <CrawlSettingsFields type={type} idPrefix="input" />
            <p className="text-xs text-muted-foreground">Browser mode, proxy route, and artifact storage use each input’s saved settings. Model identifiers must be supported by the selected API.</p>
            {fetcher.data?.error && <Alert variant="destructive"><AlertTitle>Could not start the batch</AlertTitle><AlertDescription>{fetcher.data.error}</AlertDescription></Alert>}
          </div>
          <SheetFooter className="border-t">
            <Button type="submit" disabled={busy || !domains?.length}><PlayIcon data-icon="inline-start" />{busy ? "Starting…" : `Start ${domains?.length ?? 0} ${domains?.length === 1 ? "crawl" : "crawls"}`}</Button>
            <Button type="button" variant="outline" disabled={busy} onClick={onClose}>Cancel</Button>
          </SheetFooter>
        </form>
      </SheetContent>
    </Sheet>
  </>;
}
