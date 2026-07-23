import type { Route } from "./+types/facet-options";
import { UNIFIED_FACET_KEYS } from "~/lib/filters";
import { getCountry } from "~/lib/countries";
import { searchFacetOptions } from "~/lib/facets.server";
import { searchUnifiedFacetOptions } from "~/lib/unified.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const column = url.searchParams.get("column") ?? "";
  const q = url.searchParams.get("q") ?? "";
  const countryCode = url.searchParams.get("country");
  if (!UNIFIED_FACET_KEYS.includes(column)) {
    throw new Response(`Unknown facet column: ${column}`, { status: 400 });
  }
  if (countryCode) {
    const country = getCountry(countryCode);
    if (!country || column === "country" || column === "has_financials") {
      throw new Response("Invalid country-scoped facet", { status: 400 });
    }
    return { options: await searchFacetOptions(country, column, q) };
  }
  return { options: await searchUnifiedFacetOptions(column, q) };
}
