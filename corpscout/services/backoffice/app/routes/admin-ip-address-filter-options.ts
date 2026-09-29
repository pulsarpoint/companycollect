import { data } from "react-router";
import type { Route } from "./+types/admin-ip-address-filter-options";
import {
  searchIpFilterOptions,
  type IpFilterOptionKind,
} from "~/lib/ip-filter-options.server";

const KINDS: readonly IpFilterOptionKind[] = ["asn", "country", "region", "city"];

/** `?kind=asn|country|region|city&q=…` (region and city also `country=SE`,
 * city optionally repeated `region=AB`) → `{ options }` with counts. */
export async function loader({ request }: Route.LoaderArgs) {
  const params = new URL(request.url).searchParams;
  const kind = params.get("kind") ?? "";
  if (!(KINDS as readonly string[]).includes(kind)) {
    throw new Response(`Unknown filter: ${kind}`, { status: 400 });
  }
  try {
    return {
      options: await searchIpFilterOptions(
        kind as IpFilterOptionKind,
        (params.get("q") ?? "").slice(0, 100),
        {
          country: (params.get("country") ?? "").toUpperCase(),
          regions: params.getAll("region").map((value) => value.toUpperCase()),
        },
      ),
    };
  } catch (error) {
    console.error("Unable to load IP filter options", error);
    return data({ options: [], error: true }, { status: 503 });
  }
}
