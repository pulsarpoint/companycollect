import { Link, Outlet, useLocation, useNavigation } from "react-router";
import type { Route } from "./+types/admin-domain";
import { TechnologySectionTabs } from "~/components/detail/technology-section-tabs";
import { buttonVariants } from "~/components/ui/button";
import type { TechnologySection } from "~/lib/company-tabs";
import { commonCrawlDomainPath } from "~/lib/common-crawl";
import { domainGraphPath, graphDomainError } from "~/lib/domain-graph";
import { workspaceDomainHref } from "~/lib/workspace-domains";

export function loader({ params }: Route.LoaderArgs) {
  const domain = params.domain.trim().toLowerCase();
  if (!domain || graphDomainError(domain)) {
    throw new Response("Not found", { status: 404 });
  }
  return { domain };
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [
    { title: `${loaderData?.domain ?? "Domain"} · Domains | CompanyCollect` },
  ];
}

export default function WorkspaceDomain({ loaderData }: Route.ComponentProps) {
  const { domain } = loaderData;
  const location = useLocation();
  const navigation = useNavigation();
  const basePath = workspaceDomainHref(domain);
  const suffix = location.pathname.split("/")[4] ?? "";
  const section: TechnologySection =
    suffix === "web-technologies" ||
    suffix === "web-intelligence" ||
    suffix === "infrastructure" ||
    suffix === "ip-addresses" ||
    suffix === "mail-security"
      ? suffix
      : "overview";

  return (
    <div
      className="flex flex-col gap-6 p-4 md:p-6"
      aria-busy={navigation.state !== "idle"}
    >
      <header className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h1 className="break-all font-mono text-2xl font-semibold">
            {domain}
          </h1>
          <p className="text-muted-foreground text-sm">Domain details</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Link
            className={buttonVariants({ variant: "outline", size: "sm" })}
            to={domainGraphPath({ domain, release: "" })}
          >
            Open in Graph
          </Link>
          <Link
            className={buttonVariants({ variant: "outline", size: "sm" })}
            to={commonCrawlDomainPath(domain)}
          >
            Inspect website evidence
          </Link>
          <Link
            className={buttonVariants({ variant: "ghost", size: "sm" })}
            to="/admin/domains"
          >
            All domains
          </Link>
        </div>
      </header>
      <TechnologySectionTabs basePath={basePath} section={section} mailSecurity />
      <Outlet />
    </div>
  );
}
