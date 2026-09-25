import type { Route } from "./+types/admin-se-company-technology-ip-address";
import { DomainTechnologyIpView } from "~/components/detail/domain-technology-ip-view";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyIpDetail } from "~/lib/queries.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// The selected domain uses the same IP evidence view as its domain page.

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
  return (
    <DomainTechnologyIpView
      detail={loaderData}
      address={params.address}
      backLink={{
        label: "All IP addresses",
        to: `/admin/se/company/${params.companyId}/technology/ip-addresses${loaderData ? `?domain=${encodeURIComponent(loaderData.companyDomain)}` : ""}`,
      }}
    />
  );
}
