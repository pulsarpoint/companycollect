import type { Route } from "./+types/admin-se-companies-domains";
import { SeDomainsTable } from "~/components/admin/se-domains-table";
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";
import { parseSeDomainsFilters } from "~/lib/se-domains-filters";
import { listSeDomainsPage, loadSeDomainsCounts } from "~/lib/se-domains-list.server";

// Only `loader`, `meta` and the component live here (see CLAUDE.md, "Route
// modules and .server files"): any other export touching
// `~/lib/se-domains-list.server` would keep that module in the client bundle
// and break the production build.

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

export default function AdminSeCompaniesDomains({ loaderData }: Route.ComponentProps) {
  const { rows, counts, page, pageSize, filters } = loaderData;
  // The layout owns the page header (title + tab bar); this tab renders only
  // its own body.
  return (
    <div className="flex flex-col gap-4">
      <p className="text-muted-foreground text-sm">
        Every domain of the SE domain entity, one row per company that claims it.
        A domain claimed by several companies shows how many; open it to see them
        all and how the domain links to other domains in the Common Crawl graph.
      </p>
      <SeDomainsTable rows={rows} counts={counts} page={page} pageSize={pageSize} filters={filters} />
    </div>
  );
}
