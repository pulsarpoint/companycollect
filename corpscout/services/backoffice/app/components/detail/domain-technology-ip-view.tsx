import type { ComponentProps } from "react";
import { TechnologyIpAddressDetail } from "~/components/detail/technology-ip-address-detail";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import type { CompanyTechnologyIpDetail } from "~/lib/queries.server";

export function DomainTechnologyIpView({ detail, address, backLink }: {
  detail: CompanyTechnologyIpDetail | null;
  address: string;
  backLink: ComponentProps<typeof TechnologyIpAddressDetail>["backLink"];
}) {
  if (!detail) {
    return (
      <Empty className="min-h-40 border">
        <EmptyHeader>
          <EmptyTitle>No DNS evidence for this IP address</EmptyTitle>
          <EmptyDescription>
            No historical A or AAAA record for the selected domain resolves to {address}.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <TechnologyIpAddressDetail
      detail={detail}
      domainContext={{ domain: detail.companyDomain, hostnames: detail.companyHostnames }}
      backLink={backLink}
    />
  );
}
