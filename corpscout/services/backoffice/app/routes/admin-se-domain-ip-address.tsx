import type { Route } from "./+types/admin-se-domain-ip-address";
import { DomainTechnologyIpView } from "~/components/detail/domain-technology-ip-view";
import { getDomainTechnologyIpDetail } from "~/lib/queries.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const search = new URL(request.url).searchParams;
  return getDomainTechnologyIpDetail(
    params.domain.trim().toLowerCase(),
    params.address,
    {
      exactPage: Number(search.get("exactPage") ?? "1"),
      segmentPage: Number(search.get("segmentPage") ?? "1"),
    },
  );
}

export default function DomainIpAddress({
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <DomainTechnologyIpView
      detail={loaderData}
      address={params.address}
      backLink={{
        label: "All IP addresses",
        to: "..",
        relative: "path",
      }}
    />
  );
}
