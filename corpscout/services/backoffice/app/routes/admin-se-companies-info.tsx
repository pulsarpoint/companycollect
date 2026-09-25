import { useEffect, useRef, useState } from "react";
import { data, Link, useFetcher } from "react-router";
import type { Route } from "./+types/admin-se-companies-info";
import { SeCompanyInfoTable } from "~/components/admin/se-company-info-table";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { launchSeCompanyBraveAnalysis } from "~/lib/se-company-brave.server";
import {
  NO_COMPANIES_SELECTED,
  selectionForSeCompanyFilters,
  type SeCompanySelection,
} from "~/lib/se-company-selection";
import { parseInfoFilters, parseListView } from "~/lib/se-company-info-filters";
import {
  listSeCompanyInfoPage,
  loadSeCompanyInfoCounts,
  loadSeCompanyInfoFilterOptions,
  resolveInfoSort,
} from "~/lib/se-company-info-lists.server";

// Only route exports and the component live here -- any other export that
// touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md). Parsing lives in the client-safe
// `se-company-info-filters` module, shared with the ledger route and directly
// testable: it returns the filters as APPLIED (unknown and "Any" values
// dropped), so the chips and the Filters count can never claim a filter the
// query does not have.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseInfoFilters(url);
  const view = parseListView(url);
  // Whitelisted here as well as inside the query builder, so the component
  // renders the sort that was actually applied -- an unknown ?sort= shows the
  // default column as active rather than an indicator on nothing.
  const sort = resolveInfoSort(view.sort, view.dir);

  const [listPage, counts, options] = await Promise.all([
    listSeCompanyInfoPage({
      ...filters,
      page: view.page,
      pageSize: view.pageSize,
      ...sort,
    }),
    loadSeCompanyInfoCounts(filters),
    // Cached for ten minutes server-side (see FILTER_OPTIONS_TTL_MS): the
    // filter sheet's discrete option lists must not cost a FINAL scan per load.
    loadSeCompanyInfoFilterOptions(),
  ]);
  // listSeCompanyInfoPage runs no count() of its own -- the table's pagination
  // total is the counts strip's `total`, which shares this exact WHERE and is
  // already loaded above for the strip.
  return { listPage, counts, options, total: counts.total, filters, view, sort };
}

export function meta() {
  return [{ title: "Companies · Info | CompanyCollect" }];
}

export async function action({ request }: Route.ActionArgs) {
  try {
    const body = await request.json();
    if (body?.action !== "brave_analysis") throw new Error("Choose a supported company action.");
    return data(await launchSeCompanyBraveAnalysis(
      body.selection,
      process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
    ));
  } catch (error) {
    return data({ ok: false as const, error: error instanceof Error ? error.message : "Could not prepare the Brave queue." }, { status: 400 });
  }
}

export default function AdminSeCompanyInfoTable({ loaderData }: Route.ComponentProps) {
  const { listPage, counts, options, total, filters, view, sort } = loaderData;
  // Route-owned state survives pagination and sorting. Submit filters directly;
  // Dagster freezes their matching companies in ClickHouse.
  const [selection, setSelection] = useState<SeCompanySelection>(NO_COMPANIES_SELECTED);
  const currentSelection = selectionForSeCompanyFilters(selection, filters);
  if (currentSelection !== selection) setSelection(currentSelection);
  const fetcher = useFetcher<typeof action>();
  const submittedSelection = useRef<SeCompanySelection | null>(null);
  const handledRun = useRef<string | null>(null);
  const busy = fetcher.state !== "idle";
  const selectedCount = currentSelection.mode === "ids"
    ? currentSelection.companyIds.length
    : Math.max(0, total - currentSelection.excludedCompanyIds.length);
  useEffect(() => {
    if (fetcher.data?.ok && handledRun.current !== fetcher.data.runId) {
      handledRun.current = fetcher.data.runId;
      setSelection((current) => current === submittedSelection.current ? NO_COMPANIES_SELECTED : current);
    }
  }, [fetcher.data]);
  // The layout owns the page header now (title + tab bar), so this tab renders
  // only its own body.
  return (
    <div className="flex flex-col gap-4">
    {fetcher.data && !busy && <Alert variant={fetcher.data.ok ? "default" : "destructive"}>
      <AlertDescription>
        {fetcher.data.ok
          ? <>Preparing the Brave queue. <Link to={fetcher.data.queueUrl}>Choose an assistant model and configure processing</Link>. {fetcher.data.runUrl && <a href={fetcher.data.runUrl} target="_blank" rel="noreferrer">View preparation in Dagster</a>}.</>
          : fetcher.data.error}
      </AlertDescription>
    </Alert>}
    <SeCompanyInfoTable
      rows={listPage.rows}
      total={total}
      page={view.page}
      pageSize={view.pageSize}
      sort={sort.sort}
      dir={sort.dir}
      counts={counts}
      options={options}
      filters={filters}
      selection={currentSelection}
      onSelectionChange={setSelection}
      selectionActions={(selectedCount > 0 || busy) && <Button
        size="sm"
        disabled={busy || selectedCount === 0}
        onClick={() => {
          if (busy) return;
          submittedSelection.current = currentSelection;
          fetcher.submit(JSON.stringify({ action: "brave_analysis", selection: currentSelection }), { method: "post", encType: "application/json", action: "/admin/se/companies?index" });
        }}
      >{busy ? "Preparing Brave queue…" : "Add to Brave queue"}</Button>}
    />
    </div>
  );
}
