import type { Route } from "./+types/company-same-address";
import { getCountry } from "~/lib/countries";
import { getSwedenCompaniesAtSameBuilding } from "~/lib/address-companies.server";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  if (country.code !== "se") {
    throw new Response("Same-address search is not available", { status: 404 });
  }
  return getSwedenCompaniesAtSameBuilding(params.id);
}
