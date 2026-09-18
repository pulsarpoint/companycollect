import type { Route } from "./+types/admin-se-companies-domain";
import { Building2Icon, ExternalLinkIcon, NetworkIcon } from "lucide-react";
import { Link, useNavigation, useRevalidator } from "react-router";
import { DomainConnections } from "~/components/admin/domain-connections";
import {
  seCompanyDomainsHref,
  seDomainCompanyColumns,
} from "~/components/admin/se-domains-table";
import { DataTable } from "~/components/data-table/data-table";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { commonCrawlDomainPath } from "~/lib/common-crawl";
import {
  domainGraphPath,
  graphDomainError,
  parseDomainGraphSearch,
} from "~/lib/domain-graph";
import {
  getDomainGraphReleases,
  searchDomainGraph,
  type DomainGraphRelease,
  type DomainGraphResult,
} from "~/lib/domain-graph.server";
import { seDomainHref, seDomainsHref, EMPTY_SE_DOMAINS_FILTERS } from "~/lib/se-domains-filters";
import {
  loadSeDomainCompanies,
  loadSeDomainCompanyCounts,
} from "~/lib/se-domains-list.server";
import { DEFAULT_PAGE_SIZE } from "~/lib/paging";

// Only `loader`, `meta` and the component live here (see CLAUDE.md, "Route
// modules and .server files").

const nf = new Intl.NumberFormat("en-US");

/**
 * One domain of the SE domain entity: every company claiming it (the reason
 * this page exists -- a domain shared by several companies is what a reviewer
 * needs to see side by side), then the domain's neighbours in the newest
 * published Common Crawl graph release, exactly as the Graph page shows them.
 *
 * The companies are the page; the graph is an annex. A graph failure or a
 * missing release therefore never fails the page -- it becomes a message under
 * the companies, with a retry.
 */
export async function loader({ params, request }: Route.LoaderArgs) {
  const domain = decodeURIComponent(params.domain).trim().toLowerCase();
  if (domain === "" || graphDomainError(domain)) {
    throw new Response("Not found", { status: 404 });
  }
  const url = new URL(request.url);
  const { direction, page, pageSize, release: requestedRelease } = parseDomainGraphSearch(url);

  let release: DomainGraphRelease | null = null;
  let result: DomainGraphResult | null = null;
  let companyCounts: Record<string, number> = {};
  let error: string | null = null;

  const [companies, graph] = await Promise.all([
    loadSeDomainCompanies(domain),
    (async () => {
      const releases = await getDomainGraphReleases();
      const chosen =
        releases.find((item) => item.graph_release === requestedRelease) ?? releases[0] ?? null;
      if (chosen === null) return { release: null, result: null };
      const found = await searchDomainGraph({
        domain,
        release: chosen.graph_release,
        direction,
        page,
        pageSize,
      });
      return { release: chosen, result: found };
    })().catch(() => {
      error =
        "The graph could not be loaded. Please retry. Large domains may take longer to query.";
      return { release: null, result: null };
    }),
  ]);
  release = graph.release;
  result = graph.result;
  if (result !== null && result.rows.length > 0) {
    const counts = await loadSeDomainCompanyCounts(result.rows.map((row) => row.connected_domain));
    companyCounts = Object.fromEntries(counts);
  }

  return {
    domain,
    companies,
    search: { direction, page, pageSize },
    release,
    result,
    companyCounts,
    error,
  };
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [{ title: `${loaderData?.domain ?? "Domain"} · Domains | CompanyCollect` }];
}

export default function AdminSeCompaniesDomain({ loaderData }: Route.ComponentProps) {
  const { domain, companies, search, release, result, companyCounts, error } = loaderData;
  const navigation = useNavigation();
  const revalidator = useRevalidator();
  const loading = navigation.state !== "idle" || revalidator.state !== "idle";
  const graphHref = domainGraphPath({ domain, release: release?.graph_release ?? "" });

  return (
    <div className="flex flex-col gap-6" aria-busy={loading}>
      <header className="flex flex-col gap-2">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex flex-col gap-1">
            <h2 className="break-all font-mono text-xl font-semibold">{domain}</h2>
            <p className="text-muted-foreground text-sm">
              {companies.length === 0
                ? "Not claimed by any SE company"
                : companies.length === 1
                  ? "Claimed by 1 company"
                  : `Claimed by ${nf.format(companies.length)} companies`}
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button
              variant="outline"
              size="sm"
              nativeButton={false}
              render={<Link to={graphHref} />}
            >
              <NetworkIcon />
              Open in Graph
            </Button>
            <Button
              variant="outline"
              size="sm"
              nativeButton={false}
              render={<Link to={commonCrawlDomainPath(domain)} />}
            >
              <ExternalLinkIcon />
              Inspect website evidence
            </Button>
            <Button
              variant="ghost"
              size="sm"
              nativeButton={false}
              render={<Link to={seDomainsHref(EMPTY_SE_DOMAINS_FILTERS, 1, DEFAULT_PAGE_SIZE)} />}
            >
              All domains
            </Button>
          </div>
        </div>
      </header>

      <section className="flex flex-col gap-3" aria-label="Companies">
        <h3 className="text-lg font-semibold">Companies</h3>
        {companies.length === 0 ? (
          <Empty className="min-h-40 border">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <Building2Icon />
              </EmptyMedia>
              <EmptyTitle>No SE company claims this domain</EmptyTitle>
              <EmptyDescription>
                The SE domain entity has no row for {domain}. Its graph connections below
                may still lead to domains that companies do claim.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <DataTable
            columns={seDomainCompanyColumns()}
            data={companies}
            minWidthClassName="min-w-[56rem]"
            rowHref={(row) => seCompanyDomainsHref(row.company_id)}
          />
        )}
      </section>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Graph unavailable</AlertTitle>
          <AlertDescription>
            <p>{error}</p>
            <Button
              variant="outline"
              size="sm"
              onClick={() => revalidator.revalidate()}
              disabled={loading}
            >
              Retry
            </Button>
          </AlertDescription>
        </Alert>
      ) : release === null || result === null ? (
        <Empty className="min-h-48 border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <NetworkIcon />
            </EmptyMedia>
            <EmptyTitle>No graph release is ready yet</EmptyTitle>
            <EmptyDescription>
              Connections appear once a Common Crawl graph release has finished importing.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : !result.found ? (
        <section className="flex flex-col gap-3" aria-label="Domain connections">
          <h3 className="text-lg font-semibold">Connections</h3>
          <Empty className="min-h-48 border">
            <EmptyHeader>
              <EmptyTitle>Domain not found in this release</EmptyTitle>
              <EmptyDescription>
                {domain} is absent from {release.graph_release}. It may appear in another
                release on the Graph page.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        </section>
      ) : (
        <DomainConnections
          direction={search.direction}
          result={result}
          title={<h3 className="text-lg font-semibold">Connections</h3>}
          directionHref={(direction) =>
            seDomainHref(domain, { direction, pageSize: search.pageSize })
          }
          connectedDomainHref={(other) => seDomainHref(other, { pageSize: search.pageSize })}
          companyCounts={companyCounts}
        />
      )}
      {release ? (
        <p className="text-muted-foreground text-xs">
          {release.graph_release} · {nf.format(Number(release.node_count))} domains ·{" "}
          {nf.format(Number(release.edge_count))} directed links
        </p>
      ) : null}
    </div>
  );
}
