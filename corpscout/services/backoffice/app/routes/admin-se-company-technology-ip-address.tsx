import type { Route } from "./+types/admin-se-company-technology-ip-address";
import { SeCompanyTechnologyEmpty } from "~/components/admin/se-company-technology-empty";
import { TechnologyIpAddressDetail } from "~/components/detail/technology-ip-address-detail";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyIpDetail } from "~/lib/queries.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The admin twin of company-technology-ip-address.tsx: same query, same
// shared detail component with the same relative back link, but no 404 when
// the address (or any domain at all) is unknown for this company.

export async function loader({ params, request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  return getCompanyTechnologyIpDetail(
    getCountry("se")!,
    params.companyId,
    params.address,
    {
      domain: url.searchParams.get("domain") ?? undefined,
      exactPage: Number(url.searchParams.get("exactPage") ?? "1") || 1,
      segmentPage: Number(url.searchParams.get("segmentPage") ?? "1") || 1,
    },
  );
}

export default function AdminSwedenCompanyTechnologyIpAddress({
  loaderData,
  params,
}: Route.ComponentProps) {
  if (!loaderData) {
    return (
      <SeCompanyTechnologyEmpty
        title="No evidence connects this IP address to the company"
        description={`No historical A or AAAA record under this company's
          domains resolves to ${params.address}, or no source has suggested a
          domain for this company at all.`}
      />
    );
  }
  return (
    <TechnologyIpAddressDetail
      detail={loaderData}
      companyContext={{
        domain: loaderData.companyDomain,
        hostnames: loaderData.companyHostnames,
      }}
      backLink={{
        label: "All IP addresses",
        to: `..?domain=${encodeURIComponent(loaderData.companyDomain)}`,
        relative: "path",
      }}
    />
  );
}
