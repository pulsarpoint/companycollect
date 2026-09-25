import { WebTechnologyHistorySection } from "~/components/detail/web-technology-history-section";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import type { getDomainTechnologyDetail } from "~/lib/queries.server";

export function TechnologyOverview({ domain, data }: {
  domain: string;
  data: Awaited<ReturnType<typeof getDomainTechnologyDetail>>;
}) {
  if (!data.webTechnologyHistory?.technologies.length) {
    return (
      <Empty className="min-h-40 border">
        <EmptyHeader>
          <EmptyTitle>No technologies discovered yet</EmptyTitle>
          <EmptyDescription>
            No web technology detections are available for {domain}. DNS
            and other observations are available in the adjacent tabs.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <WebTechnologyHistorySection
      history={data.webTechnologyHistory}
      catalog={data.technologyCatalog}
      linkTechnologies
    />
  );
}
