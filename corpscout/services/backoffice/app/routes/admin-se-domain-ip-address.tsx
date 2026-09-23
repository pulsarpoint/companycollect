import type { Route } from "./+types/admin-se-domain-ip-address";
import { TechnologyIpAddressDetail } from "~/components/detail/technology-ip-address-detail";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
} from "~/components/ui/empty";
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
  if (!loaderData) {
    return (
      <Empty className="min-h-40 border">
        <EmptyHeader>
          <EmptyTitle>No DNS evidence for this IP address</EmptyTitle>
          <EmptyDescription>
            No historical A or AAAA record for {params.domain} resolves to{" "}
            {params.address}.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <TechnologyIpAddressDetail
      detail={loaderData}
      backLink={{
        label: "All IP addresses",
        to: "../ip-addresses",
      }}
    />
  );
}
