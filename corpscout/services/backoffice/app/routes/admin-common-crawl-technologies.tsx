import type { Route } from "./+types/admin-common-crawl-technologies";
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

export default function CommonCrawlTechnologies({
  loaderData,
  params,
}: Route.ComponentProps) {
  if (!loaderData.webTechnologyHistory?.technologies.length) {
    return (
      <Empty className="min-h-40 border">
        <EmptyHeader>
          <EmptyTitle>No Common Crawl technologies discovered yet</EmptyTitle>
          <EmptyDescription>
            No technologies have been detected on archived Common Crawl pages for{" "}
            {params.domain}.
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
