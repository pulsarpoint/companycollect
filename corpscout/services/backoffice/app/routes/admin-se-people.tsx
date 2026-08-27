// Deliberately empty. This route used to host the Draft 1/Draft 2 SQLite/
// DuckDB/Temporal curation workspace, retired in favor of the ClickHouse
// company-person model (see se/people/pipeline, se/people/person/:id, and
// se/people/stale-corrections). The route itself stays: it is still the
// sidebar's "People" entry and the breadcrumb/back-link target for every
// other admin page (see admin-layout.tsx's AdminBreadcrumbs), so removing it
// would strand navigation. No loader, no workspace UI -- just the shell.
export function meta() {
  return [{ title: "Sweden people | CompanyCollect" }];
}

export default function AdminSwedenPeople() {
  return null;
}
