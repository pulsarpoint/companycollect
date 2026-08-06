import type { Route } from "./+types/country-person-targets";
import { getCountry } from "~/lib/countries";
import { searchCountryPersonTargets } from "~/lib/people.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const search = new URL(request.url).searchParams;
  const rows = await searchCountryPersonTargets(
    country.code,
    search.get("q") ?? "",
    search.get("source") ?? "",
  );
  return { rows, error: null };
}
