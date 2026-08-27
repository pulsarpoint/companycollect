import { Link } from "react-router";
import type { Route } from "./+types/admin-se-people";
import { SePeopleSourcesTable } from "~/components/admin/se-people-sources-table";
import {
  parseSePeopleSourceFilters,
  parseSePeopleSourceTab,
  parseSePeopleSourceView,
} from "~/lib/se-people-sources";
import { loadSePeopleSourcePage } from "~/lib/se-people-sources.server";

// The old Draft 1/Draft 2 SQLite/DuckDB/Temporal curation workspace this
// route used to host is retired, superseded by the ClickHouse company-person
// model (se/people/pipeline, se/people/person/:id, se/people/stale-
// corrections). The route stays -- it is still the sidebar's "People" entry
// and the breadcrumb "Admin"/"People" link every other admin page points at
// (see admin-layout.tsx's AdminBreadcrumbs) -- but its own content is now a
// tabbed browser over the three SE person SOURCE views plus the resolved
// se_company_person table, so a reviewer can see what each source actually
// published before/after a pipeline run without leaving this page.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const tab = parseSePeopleSourceTab(url);
  const filters = parseSePeopleSourceFilters(url);
  const view = parseSePeopleSourceView(url);
  const page = await loadSePeopleSourcePage(tab, filters, view.page, view.pageSize);
  return { page, filters, view };
}

export function meta() {
  return [{ title: "Sweden people | CompanyCollect" }];
}

export default function AdminSwedenPeople({ loaderData }: Route.ComponentProps) {
  const { page, filters, view } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold tracking-tight">Sweden people</h1>
        <p className="text-sm text-muted-foreground">
          The three raw person source reads (Bolagsverket, ESEF, Wikidata) and
          the resolved <code>se_company_person</code> table they feed. To run
          normalization, resolution or merge suggestions, use{" "}
          <Link to="/admin/se/people/pipeline" className="underline underline-offset-2">
            the pipeline page
          </Link>
          .
        </p>
      </header>
      <SePeopleSourcesTable page={page} filters={filters} view={view} />
    </div>
  );
}
