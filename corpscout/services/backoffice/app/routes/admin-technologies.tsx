import type { Route } from "./+types/admin-technologies";
import { TechnologiesTable } from "~/components/admin/technologies-table";
import {
  parseTechnologyListFilters,
  parseTechnologyListView,
} from "~/lib/technologies";
import {
  countTechnologies,
  listTechnologiesPage,
  loadTechnologyCategoryOptions,
} from "~/lib/technologies.server";

// The technology catalog browser: every detector the catalog knows, as one
// server-paged list (search, category filter, adoption count when the weekly
// rollup has rows). Each name opens /admin/technologies/:slug.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filters = parseTechnologyListFilters(url);
  const view = parseTechnologyListView(url);
  const [rows, total, categories] = await Promise.all([
    listTechnologiesPage(filters, view.page, view.pageSize),
    countTechnologies(filters),
    loadTechnologyCategoryOptions(),
  ]);
  return { rows, total, categories, filters, view };
}

export function meta() {
  return [{ title: "Technologies | CompanyCollect" }];
}

export default function AdminTechnologies({ loaderData }: Route.ComponentProps) {
  const { rows, total, categories, filters, view } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">Technologies</h1>
        <p className="text-sm text-muted-foreground">
          The technology catalog behind every detection — icons, descriptions
          and categories per detector, plus each technology&apos;s adopting
          domain count once the weekly rollup has run.
        </p>
      </header>
      <TechnologiesTable
        rows={rows}
        total={total}
        filters={filters}
        categories={categories}
        view={view}
      />
    </div>
  );
}
