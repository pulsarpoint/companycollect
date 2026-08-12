import type { Route } from "./+types/country-geocode";
import { getCountry } from "~/lib/countries";
import { geocodeAddressWithStreetFallback } from "~/lib/geocode.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  const searchParams = new URL(request.url).searchParams;
  const address = (searchParams.get("address") ?? "").trim();
  if (address === "") {
    throw new Response("Invalid address", { status: 400 });
  }
  if (address.length > 300) {
    // Data-driven length problem, not a client error — never error-boundary the page.
    return { coords: null };
  }
  const hasAddressCountry = searchParams.has("countryCode");
  const addressCountry = (searchParams.get("countryCode") ?? "")
    .trim()
    .toLowerCase();
  if (addressCountry !== "" && !/^[a-z]{2}$/.test(addressCountry)) {
    throw new Response("Invalid address country", { status: 400 });
  }
  const fallbackStreet = (searchParams.get("fallbackStreet") ?? "").trim();
  const fallbackPostalCode = (
    searchParams.get("fallbackPostalCode") ?? ""
  ).trim();
  if (fallbackStreet.length > 200 || fallbackPostalCode.length > 32) {
    throw new Response("Invalid fallback address", { status: 400 });
  }
  const match = await geocodeAddressWithStreetFallback(
    address,
    fallbackStreet === ""
      ? null
      : { street: fallbackStreet, postalCode: fallbackPostalCode },
    {
      countryCode: hasAddressCountry
        ? addressCountry || undefined
        : country.code,
    },
  );
  return {
    coords: match?.coords ?? null,
    precision: match?.precision ?? null,
  };
}
