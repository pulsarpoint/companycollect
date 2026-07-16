import type { Route } from "./+types/country-geocode";
import { getCountry } from "~/lib/countries";
import { geocodeAddress } from "~/lib/geocode.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const address = (new URL(request.url).searchParams.get("address") ?? "").trim();
  if (address === "" || address.length > 300) {
    throw new Response("Invalid address", { status: 400 });
  }
  return { coords: await geocodeAddress(address) };
}
