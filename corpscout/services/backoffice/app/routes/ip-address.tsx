import type { Route } from "./+types/ip-address";
import { TechnologyIpAddressDetail } from "~/components/detail/technology-ip-address-detail";
import { getTechnologyIpDetail } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const detail = await getTechnologyIpDetail(params.address, {
    exactPage: Number(url.searchParams.get("exactPage") ?? "1") || 1,
    segmentPage: Number(url.searchParams.get("segmentPage") ?? "1") || 1,
  });
  if (!detail) {
    throw new Response("IP address not found", { status: 404 });
  }
  return detail;
}

export function meta({ params }: Route.MetaArgs) {
  return [
    {
      title: `${params.address} – CompanyCollect Backoffice`,
    },
  ];
}

export default function IpAddress({ loaderData }: Route.ComponentProps) {
  return (
    <div className="flex flex-col gap-5">
      <header>
        <p className="text-muted-foreground text-sm">
          Technical infrastructure
        </p>
        <h1 className="mt-1 text-2xl font-semibold tracking-tight">
          IP address
        </h1>
      </header>
      <TechnologyIpAddressDetail detail={loaderData} />
    </div>
  );
}
