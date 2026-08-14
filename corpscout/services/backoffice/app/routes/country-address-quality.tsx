import type { Route } from "./+types/country-address-quality";
import { AddressQualityReview } from "~/components/address-quality/address-quality-review";
import {
  parseAddressQualityFilter,
  searchAddressQualityQueue,
} from "~/lib/address-quality.server";
import { getCountry } from "~/lib/countries";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || country.code !== "se") {
    throw new Response("Address quality review is not available", {
      status: 404,
    });
  }

  const searchParams = new URL(request.url).searchParams;
  const quality = parseAddressQualityFilter(searchParams.get("quality"));
  const query = (searchParams.get("q") ?? "").trim().slice(0, 200);
  const result = await searchAddressQualityQueue({
    filter: quality,
    query,
    page: Number(searchParams.get("page") ?? "1") || 1,
    pageSize: Number(searchParams.get("pageSize") ?? "50") || 50,
  });

  return { query, quality, result };
}

export function meta() {
  return [{ title: "Sweden address quality – CompanyCollect Backoffice" }];
}

export default function CountryAddressQuality({
  loaderData,
}: Route.ComponentProps) {
  return <AddressQualityReview {...loaderData} />;
}
