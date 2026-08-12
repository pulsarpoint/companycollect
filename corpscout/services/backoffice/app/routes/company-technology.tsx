import type { Route } from "./+types/company-technology";
import { TechnologyDomainsSection } from "~/components/detail/technology-domains-section";
import { WebTechnologyHistorySection } from "~/components/detail/web-technology-history-section";
import { technologyTabSupported } from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyDetail } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || !technologyTabSupported(country.code)) {
    throw new Response("Not found", { status: 404 });
  }
  const technology = await getCompanyTechnologyDetail(
    country,
    params.id,
    new URL(request.url).searchParams.get("domain") ?? undefined,
  );
  if (technology.domains.length === 0) {
    throw new Response("Technology information not found", { status: 404 });
  }
  return technology;
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `Technology · ${params.id} – CompanyCollect Backoffice` }];
}

export default function CompanyTechnology({
  loaderData,
}: Route.ComponentProps) {
  return (
    <div className="flex flex-col gap-5">
      <TechnologyDomainsSection
        domains={loaderData.domains}
        selectedDomain={loaderData.selectedDomain}
      />
      {loaderData.webTechnologyHistory?.technologies.length ? (
        <WebTechnologyHistorySection
          history={loaderData.webTechnologyHistory}
        />
      ) : null}
    </div>
  );
}
