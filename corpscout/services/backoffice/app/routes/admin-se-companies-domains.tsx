import type { Route } from "./+types/admin-se-companies-domains";
import { useEffect, useRef, useState } from "react";
import { data, useFetcher } from "react-router";
import { ChevronDownIcon } from "lucide-react";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuGroup, DropdownMenuItem } from "~/components/ui/dropdown-menu";
import { DOMAIN_CRAWL_TYPES, NO_DOMAINS_SELECTED, selectionForSeDomainFilters, type SeDomainSelection } from "~/lib/se-domain-selection";
import { launchSeDomainCrawlWorkflow, saveSeDomainCrawlInputs } from "~/lib/se-domain-crawl.server";
import { SeDomainCrawlSheet } from "~/components/admin/se-domain-crawl-sheet";
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
  try {
    const body = await request.json();
    if (body?.action === "send_for_crawl") {
      const run = await launchSeDomainCrawlWorkflow(
        body.selection, body.crawlType, body.settings,
        process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
      );
      return data({ ...run, workflow: true as const });
    }
    if (body?.action !== "save_crawl_inputs") throw new Error("Choose a supported domain action.");
    return data({ ...await saveSeDomainCrawlInputs(body.selection, body.crawlType), workflow: false as const });
  } catch (error) {
    return data({ ok: false as const, error: error instanceof Error ? error.message : "Could not start the domain action." }, { status: 400 });
  }
}

export default function AdminSeCompaniesDomains({ loaderData }: Route.ComponentProps) {
  const { rows, counts, page, pageSize, filters } = loaderData;
  const [selection, setSelection] = useState<SeDomainSelection>(NO_DOMAINS_SELECTED);
  const [crawlSheetOpen, setCrawlSheetOpen] = useState(false);
  const currentSelection = selectionForSeDomainFilters(selection, filters);
  if (currentSelection !== selection) setSelection(currentSelection);
  const fetcher = useFetcher<typeof action>();
  const submittedSelection = useRef<SeDomainSelection | null>(null);
  const handledRequest = useRef<string | null>(null);
  const busy = fetcher.state !== "idle";
  const selectedCount = currentSelection.mode === "ids" ? currentSelection.domains.length : Math.max(0, counts.domains - currentSelection.excludedDomains.length);
  useEffect(() => {
    if (fetcher.data?.ok && handledRequest.current !== fetcher.data.requestId) {
      handledRequest.current = fetcher.data.requestId;
      setSelection((current) => current === submittedSelection.current ? NO_DOMAINS_SELECTED : current);
      setCrawlSheetOpen(false);
    }
  }, [fetcher.data]);
  // The layout owns the page header (title + tab bar); this tab renders only
  // its own body.
  return (
    <div className="flex flex-col gap-4">
      {fetcher.data && !busy && <Alert variant={fetcher.data.ok ? "default" : "destructive"}>
        <AlertDescription>{fetcher.data.ok
          ? fetcher.data.workflow
            ? <>Crawl queued in Dagster as task {fetcher.data.requestId}. The workflow saves the domains, freezes the selection and crawls every enabled domain of it. {fetcher.data.runUrl && <a className="underline" href={fetcher.data.runUrl} target="_blank" rel="noreferrer">View crawl run</a>}</>
            : <>Input selection queued in Dagster. The input asset will save the domains and preserve existing settings. No crawl was started. {fetcher.data.runUrl && <a className="underline" href={fetcher.data.runUrl} target="_blank" rel="noreferrer">View input run</a>}</>
          : fetcher.data.error}</AlertDescription>
      </Alert>}
      <p className="text-muted-foreground text-sm">
        Every domain of the SE domain entity, one row per company that claims it.
        A domain claimed by several companies shows how many; open it to see them
        all and how the domain links to other domains in the Common Crawl graph.
      </p>
      <p className="text-muted-foreground text-sm">
        Send selected domains for crawling, or only add them to the full crawl, jobs or basic info input tables. Adding inputs does not start crawling.
      </p>
      <SeDomainsTable rows={rows} counts={counts} page={page} pageSize={pageSize} filters={filters} selection={{
        value: currentSelection, onChange: setSelection, actions: <div className="flex flex-wrap gap-2">
          <Button size="sm" disabled={busy || selectedCount === 0} onClick={() => setCrawlSheetOpen(true)}>Send for crawl</Button>
          <DropdownMenu>
          <DropdownMenuTrigger render={<Button size="sm" variant="outline" disabled={busy || selectedCount === 0} />}>
            {busy ? "Queuing…" : "Add to crawl inputs"}<ChevronDownIcon data-icon="inline-end" />
          </DropdownMenuTrigger>
          <DropdownMenuContent><DropdownMenuGroup>
            {DOMAIN_CRAWL_TYPES.map((type) => <DropdownMenuItem key={type.value} disabled={busy || selectedCount === 0} onClick={() => {
              if (busy || selectedCount === 0) return;
              submittedSelection.current = currentSelection;
              fetcher.submit(JSON.stringify({ action: "save_crawl_inputs", crawlType: type.value, selection: currentSelection }), { method: "post", encType: "application/json", action: "/admin/se/companies/domains" });
            }}>{type.label}</DropdownMenuItem>)}
          </DropdownMenuGroup></DropdownMenuContent>
          </DropdownMenu>
        </div>,
      }} />
      <SeDomainCrawlSheet open={crawlSheetOpen} selectedCount={selectedCount} busy={busy}
        error={crawlSheetOpen && fetcher.data && !fetcher.data.ok ? fetcher.data.error : null}
        onClose={() => setCrawlSheetOpen(false)}
        onSubmit={(crawlType, settings) => {
          if (busy || selectedCount === 0) return;
          submittedSelection.current = currentSelection;
          fetcher.submit(JSON.stringify({ action: "send_for_crawl", crawlType, selection: currentSelection, settings }), { method: "post", encType: "application/json", action: "/admin/se/companies/domains" });
        }} />
    </div>
  );
}
