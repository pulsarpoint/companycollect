import { CompanySourceStrip } from "~/components/admin/company-source-strip";
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
import type { SeCompanyDomainRow } from "~/lib/se-company-domains.server";

/** The unreviewed-domain queue, pre-filtered to this company. The queue reads
 * its filter from `?q=`, which matches on company id as well as name. */
function reviewQueueHref(companyId: string): string {
  return `/countries/se/domain-suggestions?q=${encodeURIComponent(companyId)}`;
}

function confidencePercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

function DomainCard({ row }: { row: SeCompanyDomainRow }) {
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
          {row.is_active ? null : <Badge variant="outline">inactive</Badge>}
          <Badge variant="outline">
            confidence {confidencePercent(row.suggested_confidence)}
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
 * Every domain the unified `company_domains` register associates with this
 * company -- rejected and inactive ones included, because "we already decided
 * against this one" is the answer a reviewer most often needs.
 */
export function SeCompanyDomainsTab({
  companyId,
  domains,
}: {
  companyId: string;
  domains: SeCompanyDomainRow[];
}) {
  if (domains.length === 0) {
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
      <div className="text-sm">
        <Link
          className="underline underline-offset-2"
          to={reviewQueueHref(companyId)}
        >
          Review these in the domain queue
        </Link>
      </div>
      {domains.map((row) => (
        <DomainCard key={row.root_domain} row={row} />
      ))}
    </section>
  );
}
