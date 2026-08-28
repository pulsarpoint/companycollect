import type { Route } from "./+types/admin-se-company-technology";
import { SeCompanyTechnologyNoDomains } from "~/components/admin/se-company-technology-empty";
import { TechnologyDomainsSection } from "~/components/detail/technology-domains-section";
import { WebTechnologyHistorySection } from "~/components/detail/web-technology-history-section";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyDetail } from "~/lib/queries.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The overview of the admin technology area: the same query and shared
// sections as the public /company/se/:id/technology index. The domain
// selector and the section tabs live in the parent
// admin-se-company-technology-layout.tsx. Unlike the public page it never
// 404s on an empty result -- an admin reviewing a company needs the tab to
// say "nothing yet", not vanish.

export async function loader({ params, request }: Route.LoaderArgs) {
  return getCompanyTechnologyDetail(
    getCountry("se")!,
    params.companyId,
    new URL(request.url).searchParams.get("domain") ?? undefined,
  );
}

export default function AdminSwedenCompanyTechnology({
  loaderData,
}: Route.ComponentProps) {
  const { domains, selectedDomain, webTechnologyHistory } = loaderData;

  if (domains.length === 0) {
    return <SeCompanyTechnologyNoDomains />;
  }

  return (
    <div className="flex flex-col gap-5">
      <TechnologyDomainsSection
        domains={domains}
        selectedDomain={selectedDomain}
      />
      {webTechnologyHistory?.technologies.length ? (
        <WebTechnologyHistorySection history={webTechnologyHistory} />
      ) : null}
    </div>
  );
}
