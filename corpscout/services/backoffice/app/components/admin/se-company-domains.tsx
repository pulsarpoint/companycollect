import { DomainSourceSupport } from "~/components/domain-suggestions/domain-source-support";
import { CompanySourceStrip } from "~/components/admin/company-source-strip";
import { SeDomainRelationships } from "~/components/admin/se-domain-relationships";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "~/components/ui/collapsible";
import { GlobeIcon } from "lucide-react";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { DefinitionList, text } from "~/components/admin/definition-list";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import type { SeCompanyDomainRow, SeDomainRelationship } from "~/lib/se-company-domains.server";

/** The unreviewed-domain queue, pre-filtered to this company. The queue reads
 * its filter from `?q=`, which matches on company id as well as name. */
function reviewQueueHref(companyId: string): string {
  return `/countries/se/domain-suggestions?q=${encodeURIComponent(companyId)}`;
}

function confidencePercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

function DomainCard({ row }: { row: SeCompanyDomainRow }) {
  const unverified = !row.is_active && row.review_status !== "rejected" && row.inactive_reason === "unverified";
  // The four source arrays are parallel by construction (one entry per source
  // that evidenced this domain), so they are zipped rather than listed apart.
  const sources = row.source_names.map((name, index) => ({
    name,
    confidence: row.source_confidences[index],
    url: row.source_urls[index] ?? "",
    basis: row.confidence_bases[index] ?? "",
  }));
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle className="text-base font-mono">
            {row.root_domain}
          </CardTitle>
          {row.suggested_primary ? <Badge>primary</Badge> : null}
          <Badge
            variant={
              row.review_status === "unreviewed" ? "outline" : "secondary"
            }
          >
            {row.review_status}
          </Badge>
          {unverified ? <Badge variant="outline">Unverified</Badge> : row.is_active ? null : <Badge variant="outline">inactive</Badge>}
          <Badge variant="outline">
            {row.association === "not_connected" ? "rejection confidence" : row.verification_status === "success" ? "assessment confidence" : "source score"}{" "}
            {confidencePercent(row.suggested_confidence)}
          </Badge>
        </div>
        <CardDescription>
          {row.website_url === "" ? (
            "No website URL recorded."
          ) : (
            <a
              className="underline underline-offset-2 break-all"
              href={row.website_url}
              target="_blank"
              rel="noreferrer"
            >
              {row.website_url}
            </a>
          )}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <DomainSourceSupport sources={row.supporting_sources} />
        <div className="flex flex-col gap-1">
          <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Evidence
          </span>
          {sources.length === 0 ? (
            <span className="text-sm text-muted-foreground">
              No source recorded.
            </span>
          ) : (
            <ul className="flex flex-col gap-1 text-sm">
              {sources.map((source, index) => (
                <li key={`${source.name}-${index}`}>
                  <Badge variant="outline">{source.name}</Badge>{" "}
                  <span className="tabular-nums">
                    {confidencePercent(source.confidence ?? 0)}
                  </span>
                  {source.basis === "" ? null : (
                    <span className="ml-1 text-muted-foreground text-xs">
                      {source.basis}
                    </span>
                  )}
                  {source.url === "" ? null : (
                    <a
                      className="ml-2 underline underline-offset-2 break-all text-xs"
                      href={source.url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {source.url}
                    </a>
                  )}
                </li>
              ))}
            </ul>
          )}
        </div>
        <DefinitionList
          valueClassName="break-all"
          entries={[
            ["Website host", text(row.website_host)],
            ["Website assessment", text(row.association === "not_connected" ? "Not a company website" : row.association === "connected" ? "Connected company website" : "Unverified company domain")],
            ["Verification processing", text(row.verification_status === "success" ? "Completed" : row.verification_status?.replaceAll("_", " ") ?? "")],
            ["Verification reason", text(row.verification_reason ?? "")],
            ["Inactive reason", text(row.is_active || unverified ? "" : (row.inactive_reason?.replaceAll("_", " ") ?? ""))],
            ["Reviewed by", text(row.reviewed_by)],
            ["Reviewed at", text(row.reviewed_at)],
            ["Review note", text(row.review_note)],
            ["First seen", text(row.first_seen_at)],
            ["Last seen", text(row.last_seen_at)],
            ["Resolved at", text(row.resolved_at)],
          ]}
        />
      </CardContent>
    </Card>
  );
}

/**
 * Every domain the domain entity associates with this
 * company -- rejected and inactive ones included, because "we already decided
 * against this one" is the answer a reviewer most often needs.
 */
export function SeCompanyDomainsTab({
  companyId,
  domains,
  relationships = [],
}: {
  companyId: string;
  domains: SeCompanyDomainRow[];
  relationships?: SeDomainRelationship[];
}) {
  const currentDomains = domains.filter((row) => row.review_status !== "rejected" && (row.is_active || row.inactive_reason === "unverified"));
  const historyDomains = domains.filter((row) => !currentDomains.includes(row));
  if (domains.length === 0 && relationships.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <GlobeIcon />
          </EmptyMedia>
          <EmptyTitle>No domains recorded</EmptyTitle>
          <EmptyDescription>
            No source has suggested a domain for this company yet.
          </EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          <Link
            className="underline underline-offset-2 text-sm"
            to={reviewQueueHref(companyId)}
          >
            Open the domain review queue
          </Link>
        </EmptyContent>
      </Empty>
    );
  }
  return (
    <section className="flex flex-col gap-4">
      {/* company_domains names its own suggesters ('common_crawl_identity',
          'wikidata', 'esef_filing'), which is a different vocabulary from the
          register letters on the list page -- the strip maps the two that ARE
          registers onto their catalog names and shows the rest as they are. */}
      <CompanySourceStrip
        sources={domains.flatMap((row) => row.source_names)}
      />
      <p className="text-muted-foreground text-sm">Domains reported by sources such as Brave appear here before verification. Unverified domains still count as company domains. Primary marks the preferred website.</p>
      <div className="text-sm">
        <Link
          className="underline underline-offset-2"
          to={reviewQueueHref(companyId)}
        >
          Review these in the domain queue
        </Link>
      </div>
      <h2 className="text-lg font-medium">Company domains</h2>
      {currentDomains.map((row) => (
        <DomainCard key={row.root_domain} row={row} />
      ))}
      {currentDomains.length === 0 ? <p className="text-sm text-muted-foreground">No current company domains recorded.</p> : null}
      <SeDomainRelationships relationships={relationships} />
      {historyDomains.length > 0 ? <Collapsible>
        <CollapsibleTrigger className="text-sm underline underline-offset-4">
          Website candidate review history ({historyDomains.length})
        </CollapsibleTrigger>
        <CollapsibleContent className="flex flex-col gap-4 pt-4">
          <p className="text-sm text-muted-foreground">These domains were considered as company websites. Their mentions may still describe useful connections.</p>
          {historyDomains.map((row) => <DomainCard key={row.root_domain} row={row} />)}
        </CollapsibleContent>
      </Collapsible> : null}
    </section>
  );
}
