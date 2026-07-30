import type { CountryContractsPage } from "~/lib/contracts.server";
import { DataTable } from "~/components/data-table/data-table";
import {
  ContractFilterSheet,
  type AgreementFacetOption,
  type CpvFacetOption,
} from "~/components/data-table/contract-filter-sheet";
import { ContractColumnPicker } from "~/components/data-table/contract-column-picker";
import {
  CONTRACT_COLUMNS,
  type ContractColumnId,
} from "~/lib/contract-columns";
import { brContractType } from "~/components/detail/countries/br-contract";
import {
  contractFilterCount,
  EMPTY_CONTRACT_FILTERS,
  type ContractFilters,
} from "~/lib/contract-filters";
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
  agreementOptions = [],
  cpvOptions = [],
  filters = EMPTY_CONTRACT_FILTERS,
  columns: visibleColumns,
  availableColumns,
}: {
  countryCode: string;
  page: CountryContractsPage;
  agreementOptions?: AgreementFacetOption[];
  cpvOptions?: CpvFacetOption[];
  filters?: ContractFilters;
  columns?: ContractColumnId[];
  availableColumns?: ContractColumnId[];
}) {
  const allIds = CONTRACT_COLUMNS.map((c) => c.id);
  const available = availableColumns ?? allIds;
  const visible = visibleColumns ?? available;
  const filtered = contractFilterCount(filters) > 0;

  // An empty result with filters ON is a filter that matched nothing, not a
  // country with no contracts -- saying "no contracts yet" there would be a lie
  // and would hide the way out.
  if (page.total === 0 && !filtered) {
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

  const columns = buildContractColumns(countryCode, page.sort, page.dir, visible);

  return (
    <Card>
      <CardHeader className="flex flex-row items-start justify-between gap-4">
        <div className="flex flex-col gap-1.5">
        <CardTitle>Government contracts</CardTitle>
        <CardDescription>
          Award notices, one row per contract rather than per winner. A
          contract with several winners shows the largest and a "+N" count —
          open it for every winner and source document.
        </CardDescription>
        </div>
        <div className="flex items-center gap-1.5">
          <ContractColumnPicker
            countryCode={countryCode}
            visible={visible}
            available={available}
          />
          <ContractFilterSheet
            countryCode={countryCode}
            filters={filters}
            agreementOptions={agreementOptions}
            cpvOptions={cpvOptions}
            agreementLabel={
              countryCode === "br"
                ? (value) => brContractType(value) ?? value
                : undefined
            }
          />
        </div>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <DataTable
          columns={columns}
          data={page.rows}
          minWidthClassName="min-w-[72rem]"
          emptyText={
            filtered
              ? "No contracts match these filters."
              : "No contracts match this page."
          }
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
