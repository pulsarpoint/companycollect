import type { Route } from "./+types/country-contracts-cpv";
import { getCountry } from "~/lib/countries";
import { getCpvChildren } from "~/lib/contracts.server";

/**
 * One level of the CPV tree, for the filter's drill-down.
 *
 * A resource route rather than loader data because the whole tree is 1,404
 * nodes for Norway and 3,353 for Sweden — too much to ship on every contracts
 * page for a panel most readers never open. The divisions arrive with the
 * page; each expansion fetches one more level.
 */
export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });

  const url = new URL(request.url);
  // Digits only. This reaches a query as a bound parameter either way, but a
  // code is all it can ever legitimately be.
  const parent = (url.searchParams.get("parent") ?? "").replace(/\D/g, "").slice(0, 8);

  return { nodes: await getCpvChildren(country, parent) };
}
