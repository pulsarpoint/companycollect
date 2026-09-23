import { ArrowLeftIcon } from "lucide-react";
import { Link, Outlet, redirect, useLocation, useNavigation } from "react-router";
import type { Route } from "./+types/admin-common-crawl-domain";
import { TechnologySectionTabs } from "~/components/detail/technology-section-tabs";
import { Badge } from "~/components/ui/badge";
import { buttonVariants } from "~/components/ui/button";
import {
  commonCrawlDomainPath,
  normalizeCommonCrawlDomain,
} from "~/lib/common-crawl";

export function loader({ params, request }: Route.LoaderArgs) {
  const domain = normalizeCommonCrawlDomain(params.domain);
  if (!domain) throw new Response("Common Crawl domain not found", { status: 404 });
  if (params.domain !== domain) {
    const url = new URL(request.url);
    const suffix = url.pathname.split("/").slice(4).join("/");
    throw redirect(
      `${commonCrawlDomainPath(domain)}${suffix ? `/${suffix}` : ""}${url.search}`,
    );
  }
  return { domain };
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
  const location = useLocation();
  const navigation = useNavigation();
  const suffix = location.pathname.split("/")[4] ?? "";
  const section =
    suffix === "technologies" ||
    suffix === "web-technologies" ||
    suffix === "infrastructure" ||
    suffix === "ip-addresses" ||
    suffix === "mail-security"
      ? suffix
      : "overview";

  return (
    <div
      className="flex flex-1 flex-col gap-6 p-4 md:p-6"
      aria-busy={navigation.state !== "idle"}
    >
      <header className="flex flex-col gap-4">
        <Link
          className={buttonVariants({ variant: "ghost", className: "w-fit" })}
          to="/admin/common-crawl"
        >
          <ArrowLeftIcon data-icon="inline-start" />
          Back to Common Crawl search
        </Link>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="font-mono text-2xl font-semibold tracking-tight">
            {loaderData.domain}
          </h1>
          <Badge variant="outline">Domain evidence</Badge>
        </div>
        <p className="max-w-4xl text-sm text-muted-foreground">
          Parsed Common Crawl evidence, discovered technologies, DNS records,
          and associated IP addresses. Each source keeps its observation history.
        </p>
      </header>
      <TechnologySectionTabs
        basePath={commonCrawlDomainPath(loaderData.domain)}
        section={section}
        websiteEvidenceOverview
        mailSecurity
      />
      <Outlet />
    </div>
  );
}
