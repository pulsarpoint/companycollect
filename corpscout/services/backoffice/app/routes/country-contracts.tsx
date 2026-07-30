import type { Route } from "./+types/country-contracts";
import { getCountry } from "~/lib/countries";
import {
  contractColumnAvailability,
  getAgreementTypeFacet,
  getContractHeadlineStats,
  getCountryContractsPage,
  getCpvDivisionFacet,
} from "~/lib/contracts.server";
import { parseContractFilters } from "~/lib/contract-filters";
import { parseContractColumns } from "~/lib/contract-columns";
import { CountryContractsTable } from "~/components/country/contracts-table";
import { ContractStatsBanner } from "~/components/country/contract-stats-banner";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const url = new URL(request.url);
  const filters = parseContractFilters(url.searchParams);
  // The facet is independent of the current filters, so the sheet keeps offering
  // every value rather than narrowing to what the active selection already shows
  // -- otherwise unticking is the only way back and a reader can paint
  // themselves into a corner.
  const [page, agreementOptions, cpvOptions, stats] = await Promise.all([
    getCountryContractsPage(country, {
      page: Number(url.searchParams.get("page") ?? "1") || 1,
      pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
      sort: url.searchParams.get("sort"),
      dir: url.searchParams.get("dir"),
      filters,
    }),
    getAgreementTypeFacet(country),
    getCpvDivisionFacet(country),
    // Unfiltered on purpose: the banner describes the COUNTRY's contracts, not
    // the current selection, so it stays a stable reference point while filtering.
    getContractHeadlineStats(country),
  ]);

  // Which columns exist for this country is answered by the facets that were
  // loaded anyway, so offering CPV to Estonia and Agreement type to Brazil
  // costs no extra query and needs no list of which countries are "EU".
  const availableColumns = contractColumnAvailability({
    agreement: agreementOptions,
    cpv: cpvOptions,
  });
  const columns = parseContractColumns(url.searchParams, availableColumns);

  return {
    page,
    agreementOptions,
    cpvOptions,
    filters,
    stats,
    columns,
    availableColumns,
  };
}

export default function CountryContracts({ loaderData, params }: Route.ComponentProps) {
  const country = getCountry(params.country)!;
  return (
    <div className="flex flex-col gap-4">
      {loaderData.stats ? (
        <ContractStatsBanner stats={loaderData.stats} />
      ) : null}
      <CountryContractsTable
        countryCode={country.code}
        page={loaderData.page}
        agreementOptions={loaderData.agreementOptions}
        cpvOptions={loaderData.cpvOptions}
        filters={loaderData.filters}
        columns={loaderData.columns}
        availableColumns={loaderData.availableColumns}
      />
    </div>
  );
}
