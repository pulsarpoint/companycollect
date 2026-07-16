import type { Route } from "./+types/country-facet-options";
import { getCountry } from "~/lib/countries";
import { searchFacetOptions } from "~/lib/facets.server";
import { filterableFacetKeys } from "~/lib/filters";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });

  const url = new URL(request.url);
  const column = url.searchParams.get("column") ?? "";
  const q = url.searchParams.get("q") ?? "";

  if (!filterableFacetKeys(country).includes(column)) {
    throw new Response(`Unknown facet column: ${column}`, { status: 400 });
  }

  return { options: await searchFacetOptions(country, column, q) };
}
