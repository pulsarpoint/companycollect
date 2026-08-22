import type { Route } from "./+types/admin-se-people-stale-corrections";
import { SeStaleCorrectionsTable } from "~/components/admin/se-stale-corrections-table";
import { listStaleSeCompanyPersonCorrections } from "~/lib/se-company-person.server";

export async function loader() {
  return { rows: await listStaleSeCompanyPersonCorrections() };
}

export function meta() {
  return [{ title: "Stale person corrections | CompanyCollect" }];
}

export default function AdminSwedenStaleCorrections({
  loaderData,
}: Route.ComponentProps) {
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">
          Stale person corrections
        </h1>
        <p className="text-sm text-muted-foreground">
          Live ledger rows Dagster skips because the evidence they were decided
          on has moved. They are never applied and never deleted: open the
          person and decide again, or undo the row.
        </p>
      </header>
      <SeStaleCorrectionsTable rows={loaderData.rows} />
    </div>
  );
}
