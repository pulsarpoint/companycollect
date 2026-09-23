import type { Route } from "./+types/admin-domain-web-technologies";
import { WebtechSection } from "~/components/detail/webtech-section";
import { graphDomainError } from "~/lib/domain-graph";
import { getDomainWebtech } from "~/lib/webtech.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const domain = params.domain.trim().toLowerCase();
  const site = new URL(request.url).searchParams
    .get("site")
    ?.trim()
    .toLowerCase();
  if (!domain || graphDomainError(domain) || (site && graphDomainError(site)))
    throw new Response("Not found", { status: 404 });
  return { ...(await getDomainWebtech(domain, site)), site };
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [{ title: `${loaderData?.site || loaderData?.domain || "Domain"} · Web technologies | CompanyCollect` }];
}

export default function WorkspaceDomainWebtech({
  loaderData,
}: Route.ComponentProps) {
  return (
    <div className="flex flex-col gap-4">
      {loaderData.site ? (
        <p className="text-muted-foreground text-sm">
          Showing detections for site <span className="font-mono">{loaderData.site}</span>
        </p>
      ) : null}
      <WebtechSection data={loaderData} site={loaderData.site} linkTechnologies />
    </div>
  );
}
