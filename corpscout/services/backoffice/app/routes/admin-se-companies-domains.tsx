import type { Route } from "./+types/admin-se-companies-domains";
import { useEffect, useRef, useState } from "react";
import { data, useFetcher } from "react-router";
import { PlusIcon, ChevronDownIcon } from "lucide-react";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuGroup, DropdownMenuItem } from "~/components/ui/dropdown-menu";
import { DOMAIN_CRAWL_TYPES, NO_DOMAINS_SELECTED, selectionForSeDomainFilters, type SeDomainSelection } from "~/lib/se-domain-selection";
import {addSeDomainsToCrawlQueue} from "~/lib/crawl-queue.server";
import { addSeDomainsToWebtechQueue } from "~/lib/webtech-queue.server";
import { useQueueSubmission, QueueImportStatus } from "~/components/admin/queue-import-status";
import { SeDomainsTable } from "~/components/admin/se-domains-table";
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";
import { parseSeDomainsFilters } from "~/lib/se-domains-filters";
import { listSeDomainsPage, loadSeDomainsCounts } from "~/lib/se-domains-list.server";

// Keep server calls inside loader/action so React Router removes them from
// the client bundle (see CLAUDE.md, "Route modules and .server files").

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseSeDomainsFilters(url.searchParams);
  const page = clampPage(Number.parseInt(url.searchParams.get("page") ?? "1", 10));
  const pageSize = clampPageSize(
    Number.parseInt(url.searchParams.get("pageSize") ?? String(DEFAULT_PAGE_SIZE), 10),
  );

  // The counts strip and the page share one WHERE, so the pager total (`counts.rows`)
  // is honest under every filter.
  const [{ rows }, counts] = await Promise.all([
    listSeDomainsPage({ ...filters, page, pageSize }),
    loadSeDomainsCounts(filters),
  ]);

  return { rows, counts, page, pageSize, filters };
}

export function meta() {
  return [{ title: "Companies · Domains | CompanyCollect" }];
}

export async function action({ request }: Route.ActionArgs) {
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({ok: false as const, error: "Invalid request origin."}, {status: 403});
  try {
    const body = await request.json();
    if (body?.action === "add_webtech_inputs") {
      const run = await addSeDomainsToWebtechQueue(body.selection, String(body.submissionId ?? ""), process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice");
      return data({...run, kind: "webtech" as const});
    }
    if (body?.action !== "add_crawl_inputs") throw new Error("Choose a supported domain action.");
    return data({...await addSeDomainsToCrawlQueue(body.selection, body.crawlType, String(body.submissionId ?? ""), process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice"), kind: "crawl" as const});
  } catch (error) {
    return data({ ok: false as const, error: error instanceof Error ? error.message : "Could not start the domain action." }, { status: 400 });
  }
}

export default function AdminSeCompaniesDomains({ loaderData }: Route.ComponentProps) {
  const { rows, counts, page, pageSize, filters } = loaderData;
  const [selection, setSelection] = useState<SeDomainSelection>(NO_DOMAINS_SELECTED);
  const currentSelection = selectionForSeDomainFilters(selection, filters);
  if (currentSelection !== selection) setSelection(currentSelection);
  const fetcher = useFetcher<typeof action>();
  const webtech = useFetcher<typeof action>();
  const webtechRequest = useRef<{id: string; selection: SeDomainSelection; fingerprint: string} | null>(null);
  const receipt = webtech.data?.ok && webtech.data.kind === "webtech" ? webtech.data : null;
  const {state: webtechState, error: webtechStatusError} = useQueueSubmission(receipt);
  const webtechWaiting = webtech.state !== "idle" || Boolean(receipt && !webtechState?.finished);
  const [webtechRetry, setWebtechRetry] = useState(false);
  const webtechFailed = webtechState?.status === "FAILURE" || webtechState?.status === "CANCELED";
  useEffect(() => {
    if (webtechState?.status === "SUCCESS") {
      setSelection(current => current === webtechRequest.current?.selection ? NO_DOMAINS_SELECTED : current);
      setWebtechRetry(false);
    } else if (webtechFailed || webtech.data?.ok === false) setWebtechRetry(true);
  }, [webtechState?.status, webtech.data]);
  const submitWebtech = (value: SeDomainSelection, retry = false) => {
    const fingerprint = JSON.stringify(value);
    const previous = webtechRequest.current;
    const id = retry && previous ? previous.id : previous?.fingerprint === fingerprint && webtechRetry ? previous.id : crypto.randomUUID();
    webtechRequest.current = {id, selection: value, fingerprint};
    setWebtechRetry(false);
    webtech.submit(JSON.stringify({action: "add_webtech_inputs", selection: value, submissionId: id}), {method: "post", encType: "application/json", action: "/admin/se/companies/domains"});
  };
  const crawlRequest = useRef<{id: string; selection: SeDomainSelection; crawlType: string} | null>(null);
  const crawlReceipt = fetcher.data?.ok && fetcher.data.kind === "crawl" ? fetcher.data : null;
  const {state: crawlState, error: crawlStatusError} = useQueueSubmission(crawlReceipt, "crawler");
  const [crawlRetry, setCrawlRetry] = useState(false);
  const busy = fetcher.state !== "idle" || Boolean(crawlReceipt && !crawlState?.finished);
  const selectedCount = currentSelection.mode === "ids" ? currentSelection.domains.length : Math.max(0, counts.domains - currentSelection.excludedDomains.length);
  useEffect(() => {
    if (crawlState?.status === "SUCCESS") {
      setSelection(current => current === crawlRequest.current?.selection ? NO_DOMAINS_SELECTED : current);
      setCrawlRetry(false);
    } else if (crawlState?.status === "FAILURE" || crawlState?.status === "CANCELED" || fetcher.data?.ok === false) setCrawlRetry(true);
  }, [crawlState?.status, fetcher.data]);
  const submitCrawl = (crawlType: string, value: SeDomainSelection, retry = false) => {
    const id = retry && crawlRequest.current ? crawlRequest.current.id : crypto.randomUUID();
    crawlRequest.current = {id, selection: value, crawlType};
    setCrawlRetry(false);
    fetcher.submit(JSON.stringify({action: "add_crawl_inputs", crawlType, selection: value, submissionId: id}), {method: "post", encType: "application/json", action: "/admin/se/companies/domains"});
  };
  // The layout owns the page header (title + tab bar); this tab renders only
  // its own body.
  return (
    <div className="flex flex-col gap-4">
      {crawlReceipt && <QueueImportStatus receipt={crawlReceipt} state={crawlState} crawlType={crawlReceipt.crawlType} />}
      {(fetcher.data?.ok === false || crawlStatusError) && <Alert variant="destructive"><AlertDescription>{fetcher.data?.ok === false ? fetcher.data.error : crawlStatusError}</AlertDescription></Alert>}
      {crawlRetry && crawlRequest.current && <Button variant="outline" disabled={busy} onClick={() => submitCrawl(crawlRequest.current!.crawlType, crawlRequest.current!.selection, true)}>Retry crawl import</Button>}
      {receipt && <QueueImportStatus receipt={receipt} state={webtechState} />}
      {(webtech.data?.ok === false || webtechStatusError) && <Alert variant="destructive"><AlertDescription>{webtech.data?.ok === false ? webtech.data.error : webtechStatusError}</AlertDescription></Alert>}
      {webtechRetry && webtechRequest.current && <Button variant="outline" disabled={webtechWaiting} onClick={() => submitWebtech(webtechRequest.current!.selection, true)}>Retry Webtech import</Button>}
      <p className="text-muted-foreground text-sm">
        Every domain of the SE domain entity, one row per company that claims it.
        A domain claimed by several companies shows how many; open it to see them
        all and how the domain links to other domains in the Common Crawl graph.
      </p>
      <p className="text-muted-foreground text-sm">
        Add selected domains to a Webtech or crawl draft. Combine selections, then configure and start processing from Queues.
      </p>
      <SeDomainsTable rows={rows} counts={counts} page={page} pageSize={pageSize} filters={filters} selection={{
        value: currentSelection, onChange: setSelection, actions: <div className="flex flex-wrap gap-2">
          <Button size="sm" disabled={busy || webtechWaiting || selectedCount === 0} onClick={() => submitWebtech(currentSelection)}><PlusIcon data-icon="inline-start" />{webtechWaiting ? "Adding to Webtech queue…" : "Add to Webtech queue"}</Button>
          <DropdownMenu>
          <DropdownMenuTrigger render={<Button size="sm" variant="outline" disabled={busy || selectedCount === 0} />}>
            {busy ? "Queuing…" : "Add to crawl queue"}<ChevronDownIcon data-icon="inline-end" />
          </DropdownMenuTrigger>
          <DropdownMenuContent><DropdownMenuGroup>
            {DOMAIN_CRAWL_TYPES.map((type) => <DropdownMenuItem key={type.value} disabled={busy || selectedCount === 0} onClick={() => {
              if (busy || selectedCount === 0) return;
              submitCrawl(type.value, currentSelection);
            }}>{type.label}</DropdownMenuItem>)}
          </DropdownMenuGroup></DropdownMenuContent>
          </DropdownMenu>
        </div>,
      }} />
    </div>
  );
}
