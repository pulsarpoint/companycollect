import type { Route } from "./+types/admin-common-crawl";
import { CommonCrawlTable } from "~/components/admin/common-crawl-table";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import {
  commonCrawlFilterError,
  hasCommonCrawlFilters,
  parseCommonCrawlFilters,
  parseCommonCrawlListView,
} from "~/lib/common-crawl";
import { searchCommonCrawlDomains } from "~/lib/common-crawl.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseCommonCrawlFilters(url);
  const view = parseCommonCrawlListView(url);
  const searched = hasCommonCrawlFilters(filters);
  const filterError = commonCrawlFilterError(filters);
  const result =
    searched && !filterError
      ? await searchCommonCrawlDomains(filters, view.page, view.pageSize)
      : { rows: [], total: 0 };
  return { ...result, filters, view, searched, filterError };
}

export function meta() {
  return [{ title: "Common Crawl | CompanyCollect" }];
}

export default function AdminCommonCrawl({ loaderData }: Route.ComponentProps) {
  const { rows, total, filters, view, searched, filterError } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">Common Crawl</h1>
        <p className="max-w-4xl text-sm text-muted-foreground">
          Search website evidence extracted into ClickHouse, then inspect the
          archived claims, contacts, identifiers, industries, metadata, and
          crawl history retained for each domain.
        </p>
      </header>
      {filterError ? (
        <Alert variant="destructive">
          <AlertTitle>Search term is too short</AlertTitle>
          <AlertDescription>{filterError}</AlertDescription>
        </Alert>
      ) : null}
      <CommonCrawlTable
        rows={rows}
        total={total}
        filters={filters}
        view={view}
        searched={searched && !filterError}
      />
    </div>
  );
}
