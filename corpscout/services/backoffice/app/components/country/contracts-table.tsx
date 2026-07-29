import type { CountryContractsPage } from "~/lib/contracts.server";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { buildContractColumns } from "~/components/data-table/contract-columns";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Empty, EmptyDescription, EmptyTitle } from "~/components/ui/empty";

/**
 * Server-driven contracts table: pagination, sorting, and the total count all
 * come from the loader's query rather than being computed client-side, the
 * same wiring `/countries/:country/companies` uses for `searchCompanies`.
 * Needed because a country's contracts can run to six figures (Brazil alone:
 * 112,943) — a client-side table would mean shipping all of them.
 */
export function CountryContractsTable({
  countryCode,
  page,
}: {
  countryCode: string;
  page: CountryContractsPage;
}) {
  if (page.total === 0) {
    return (
      <Empty>
        <EmptyTitle>No contracts yet</EmptyTitle>
        <EmptyDescription>
          This country has no ingested award notices. For countries with no
          national register loaded, that means TED has not been backfilled.
        </EmptyDescription>
      </Empty>
    );
  }

  const columns = buildContractColumns(countryCode, page.sort, page.dir);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Government contracts</CardTitle>
        <CardDescription>
          Award notices, one row per contract rather than per winner. A
          contract with several winners shows the largest and a "+N" count —
          open it for every winner and source document.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <DataTable
          columns={columns}
          data={page.rows}
          minWidthClassName="min-w-[72rem]"
          emptyText="No contracts match this page."
        />
        <DataTablePagination
          total={page.total}
          page={page.page}
          pageSize={page.pageSize}
          itemsLabel="contracts"
        />
      </CardContent>
    </Card>
  );
}
