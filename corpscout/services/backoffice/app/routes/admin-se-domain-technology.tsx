import type { Route } from "./+types/admin-se-domain-technology";
import { WebTechnologyHistorySection } from "~/components/detail/web-technology-history-section";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
} from "~/components/ui/empty";
import { getDomainTechnologyDetail } from "~/lib/queries.server";

export async function loader({ params }: Route.LoaderArgs) {
  return getDomainTechnologyDetail(params.domain.trim().toLowerCase());
}

export default function DomainTechnology({
  loaderData,
  params,
}: Route.ComponentProps) {
  if (!loaderData.webTechnologyHistory?.technologies.length) {
    return (
      <Empty className="min-h-40 border">
        <EmptyHeader>
          <EmptyTitle>No technologies discovered yet</EmptyTitle>
          <EmptyDescription>
            No web technology detections are available for {params.domain}. DNS
            and other observations are available in the adjacent tabs.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <WebTechnologyHistorySection
      history={loaderData.webTechnologyHistory}
      catalog={loaderData.technologyCatalog}
      linkTechnologies
    />
  );
}
