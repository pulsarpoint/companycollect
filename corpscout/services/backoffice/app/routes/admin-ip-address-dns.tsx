import type { Route } from "./+types/admin-ip-address-dns";
import { IpDnsRecordsView } from "~/components/admin/ip-address-detail";
import { parsePage } from "~/lib/ip-address-detail";
import { getIpAddressDnsRecords, resolveIpAddress } from "~/lib/ip-address-detail.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const address = await resolveIpAddress(params.address);
  if (!address) throw new Response("Not found", { status: 404 });
  return getIpAddressDnsRecords(address, {
    page: parsePage(new URL(request.url).searchParams.get("page")),
  });
}

export default function IpAddressDnsPage({ loaderData }: Route.ComponentProps) {
  return <IpDnsRecordsView data={loaderData} />;
}
