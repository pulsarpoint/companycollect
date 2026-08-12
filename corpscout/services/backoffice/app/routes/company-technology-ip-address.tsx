import type { Route } from "./+types/company-technology-ip-address";
import { TechnologyIpAddressDetail } from "~/components/detail/technology-ip-address-detail";
import { technologyTabSupported } from "~/lib/company-tabs";
import { getCountry } from "~/lib/countries";
import { getCompanyTechnologyIpDetail } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country || !technologyTabSupported(country.code)) {
    throw new Response("Not found", { status: 404 });
  }

  const url = new URL(request.url);
  const detail = await getCompanyTechnologyIpDetail(
    country,
    params.id,
    params.address,
    {
      domain: url.searchParams.get("domain") ?? undefined,
      exactPage: Number(url.searchParams.get("exactPage") ?? "1") || 1,
      segmentPage: Number(url.searchParams.get("segmentPage") ?? "1") || 1,
    },
  );
  if (!detail) {
    throw new Response("IP address not found for this company", {
      status: 404,
    });
  }
  return detail;
}

export function meta({ params }: Route.MetaArgs) {
  return [
    {
      title: `${params.address} · ${params.id} – CompanyCollect Backoffice`,
    },
  ];
}

export default function CompanyTechnologyIpAddress({
  loaderData,
}: Route.ComponentProps) {
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
