import { ArrowLeftIcon } from "lucide-react";
import { Link } from "react-router";
import type { Route } from "./+types/admin-common-crawl-domain";
import { WebIntelligenceSection } from "~/components/detail/web-intelligence-section";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { normalizeCommonCrawlDomain } from "~/lib/common-crawl";
import { getDomainWebIntelligence } from "~/lib/web-intelligence.server";

export async function loader({ params }: Route.LoaderArgs) {
  const domain = normalizeCommonCrawlDomain(params.domain);
  if (!domain) throw new Response("Common Crawl domain not found", { status: 404 });
  return {
    domain,
    intelligence: await getDomainWebIntelligence(domain),
  };
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.domain ?? params.domain} · Common Crawl | CompanyCollect`,
    },
  ];
}

export default function AdminCommonCrawlDomain({
  loaderData,
}: Route.ComponentProps) {
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-4">
        <Button
          variant="ghost"
          className="w-fit"
          render={<Link to="/admin/common-crawl" />}
          nativeButton={false}
        >
          <ArrowLeftIcon data-icon="inline-start" />
          Back to Common Crawl search
        </Button>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="font-mono text-2xl font-semibold tracking-tight">
            {loaderData.domain}
          </h1>
          <Badge variant="outline">Common Crawl evidence</Badge>
        </div>
        <p className="max-w-4xl text-sm text-muted-foreground">
          Source-linked website observations stored in ClickHouse. Claims stay
          separated by crawl so changes and stale values remain visible.
        </p>
      </header>
      <WebIntelligenceSection intelligence={loaderData.intelligence} />
    </div>
  );
}
