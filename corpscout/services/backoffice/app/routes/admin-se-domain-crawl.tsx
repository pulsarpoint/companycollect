import type { Route } from "./+types/admin-se-domain-crawl";
import { DomainCrawlView } from "~/components/admin/domain-crawl-view";
import { loadDomainCrawlPage } from "~/lib/domain-crawls.server";
export { DomainCrawlErrorBoundary as ErrorBoundary } from "~/components/admin/domain-crawl-view";

export async function loader({params, request}: Route.LoaderArgs) {
  return loadDomainCrawlPage(params.domain.trim().toLowerCase(), request);
}

export default function DomainCrawl({loaderData}: Route.ComponentProps) {
  return <DomainCrawlView {...loaderData} />;
}
