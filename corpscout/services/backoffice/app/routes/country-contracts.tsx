import type { Route } from "./+types/country-contracts";
import { getCountry } from "~/lib/countries";
import {
  getAgreementTypeFacet,
  getCountryContractsPage,
} from "~/lib/contracts.server";
import { parseContractFilters } from "~/lib/contract-filters";
import { CountryContractsTable } from "~/components/country/contracts-table";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Country not found", { status: 404 });

  const url = new URL(request.url);
  const filters = parseContractFilters(url.searchParams);
  // The facet is independent of the current filters, so the sheet keeps offering
  // every value rather than narrowing to what the active selection already shows
  // -- otherwise unticking is the only way back and a reader can paint
  // themselves into a corner.
  const [page, agreementOptions] = await Promise.all([
    getCountryContractsPage(country, {
      page: Number(url.searchParams.get("page") ?? "1") || 1,
      pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
      sort: url.searchParams.get("sort"),
      dir: url.searchParams.get("dir"),
      filters,
    }),
    getAgreementTypeFacet(country),
  ]);
  return { page, agreementOptions, filters };
}

export default function CountryContracts({ loaderData, params }: Route.ComponentProps) {
  const country = getCountry(params.country)!;
  return (
    <CountryContractsTable
      countryCode={country.code}
      page={loaderData.page}
      agreementOptions={loaderData.agreementOptions}
      filters={loaderData.filters}
    />
  );
}
