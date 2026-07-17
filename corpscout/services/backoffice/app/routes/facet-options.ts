import type { Route } from "./+types/facet-options";
import { UNIFIED_FACET_KEYS } from "~/lib/filters";
import { searchUnifiedFacetOptions } from "~/lib/unified.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const column = url.searchParams.get("column") ?? "";
  const q = url.searchParams.get("q") ?? "";
  if (!UNIFIED_FACET_KEYS.includes(column)) {
    throw new Response(`Unknown facet column: ${column}`, { status: 400 });
  }
  return { options: await searchUnifiedFacetOptions(column, q) };
}
