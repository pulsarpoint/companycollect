import type { Route } from "./+types/admin-domain-sites";
import { data } from "react-router";
import { graphDomainError } from "~/lib/domain-graph";
import { listDomainSites } from "~/lib/workspace-domains.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const domain = params.domain.trim().toLowerCase();
  if (!domain || graphDomainError(domain))
    throw new Response("Not found", { status: 404 });
  try {
    return await listDomainSites(
      domain,
      new URL(request.url).searchParams.get("after") ?? "",
    );
  } catch {
    return data(
      {
        sites: [],
        hasMore: false,
        next: "",
        error: "Sites could not be loaded. Please retry.",
      },
      { status: 503 },
    );
  }
}
