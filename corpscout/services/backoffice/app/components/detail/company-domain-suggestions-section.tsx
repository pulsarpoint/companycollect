import {
  Archive,
  CalendarClock,
  CheckCircle2,
  ExternalLink,
  FileSearch,
  Link2,
  LoaderCircle,
  RotateCcw,
  SearchX,
  XCircle,
} from "lucide-react";
import { Link, useFetcher } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type {
  CommonCrawlDomainEvidence,
  CompanyDomain,
  CompanyDomainReviewStatus,
  CompanyDomainSource,
  WikidataDomainEvidence,
} from "~/lib/company-domains.server";

type ReviewActionData = { ok: true } | { ok: false; error: string };

const percent = new Intl.NumberFormat("en-US", {
  style: "percent",
  maximumFractionDigits: 0,
});

const sourceLabels: Record<string, string> = {
  wikidata: "Wikidata",
  esef_filing: "ESEF filing",
  common_crawl_identity: "Common Crawl identity",
};

const reviewLabels: Record<CompanyDomainReviewStatus, string> = {
  unreviewed: "Unreviewed",
  confirmed_primary: "Confirmed primary",
  confirmed_related: "Confirmed related",
  rejected: "Rejected",
};

const dateTime = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeStyle: "short",
  timeZone: "UTC",
});

function safeUrl(value: string): string | null {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:"
      ? url.toString()
      : null;
  } catch {
    return null;
  }
}

function reviewBadgeVariant(
  status: CompanyDomainReviewStatus,
): "default" | "secondary" | "destructive" | "outline" {
  if (status === "confirmed_primary") return "default";
  if (status === "confirmed_related") return "secondary";
  if (status === "rejected") return "destructive";
  return "outline";
}

function sourceBasisDescription(source: CompanyDomainSource): string {
  if (source.name === "common_crawl_identity") {
    const matches = source.evidence.filter(
      (evidence): evidence is CommonCrawlDomainEvidence =>
        evidence.type === "common_crawl_match",
    );
    if (matches.length > 0) {
      const signals = matches.map((evidence) =>
        evidence.sourceField === "vat"
          ? "VAT"
          : evidence.sourceField === "lei"
            ? "LEI"
            : evidence.signalType,
      );
      return `Exact ${signals.join(" + ")} match from archived website evidence.`;
    }
    return "Deterministic identity signals found in Common Crawl matched the company record.";
  }
  if (source.name === "wikidata") {
    const match = source.evidence.find(
      (evidence): evidence is WikidataDomainEvidence =>
        evidence.type === "wikidata_match",
    );
    if (match?.matchMethod === "wikidata_registry_identifier") {
      return "Wikidata lists this official website, and its item matches the exact Swedish organisation number.";
    }
    if (match?.matchMethod === "wikidata_verified_lei") {
      return "Wikidata lists this official website, and its item matches the company's verified LEI.";
    }
    return "Wikidata lists this URL as the entity's official website.";
  }
  if (source.confidenceBasis === "explicit_company_website") {
    return "The company website was explicitly identified in an ESEF filing.";
  }
  if (source.confidenceBasis === "repeated_filing_website") {
    return "The same website appeared in multiple pieces of ESEF filing evidence.";
  }
  if (source.confidenceBasis === "filing_website_mention") {
    return "The website was mentioned in an ESEF filing.";
  }
  return "This source proposed the domain for the company.";
}

function identifierLabel(value: string): string {
  if (value === "vat") return "VAT number";
  if (value === "lei") return "LEI";
  if (value === "se_orgnr") return "Swedish organisation number";
  return value.replaceAll("_", " ");
}

function extractionMethodLabel(value: string): string {
  if (value === "text") return "visible page text";
  if (value === "jsonld") return "JSON-LD structured data";
  if (value === "microdata") return "HTML microdata";
  return value ? value.replaceAll("_", " ") : "archived page content";
}

function parseWarcTimestamp(value: string): Date | null {
  if (!/^\d{14}$/.test(value)) return null;
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(4, 6)) - 1;
  const day = Number(value.slice(6, 8));
  const hour = Number(value.slice(8, 10));
  const minute = Number(value.slice(10, 12));
  const second = Number(value.slice(12, 14));
  const date = new Date(Date.UTC(year, month, day, hour, minute, second));
  return Number.isNaN(date.getTime()) ? null : date;
}

function warcCaptureWindow(filename: string): string | null {
  const match = filename.match(/CC-MAIN-(\d{14})-(\d{14})-\d+\.warc\.gz$/);
  if (!match) return null;
  const from = parseWarcTimestamp(match[1]);
  const to = parseWarcTimestamp(match[2]);
  if (!from || !to) return null;
  const sameDay =
    from.getUTCFullYear() === to.getUTCFullYear() &&
    from.getUTCMonth() === to.getUTCMonth() &&
    from.getUTCDate() === to.getUTCDate();
  if (!sameDay) return `${dateTime.format(from)}–${dateTime.format(to)} UTC`;
  const day = new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeZone: "UTC",
  }).format(from);
  const time = new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC",
  });
  return `${day}, ${time.format(from)}–${time.format(to)} UTC`;
}

function CommonCrawlEvidenceDetails({
  evidence,
}: {
  evidence: CommonCrawlDomainEvidence[];
}) {
  if (evidence.length === 0) return null;
  return (
    <div className="flex flex-col gap-4">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Match</TableHead>
            <TableHead>Company record</TableHead>
            <TableHead>Extracted from website</TableHead>
            <TableHead>Contribution</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {evidence.map((item) => (
            <TableRow
              key={`${item.signalType}:${item.sourceField}:${item.sourceUrl}`}
            >
              <TableCell>
                <div className="font-medium">
                  {identifierLabel(item.sourceField)}
                </div>
                <div className="text-muted-foreground text-xs">
                  Exact normalized value match
                </div>
              </TableCell>
              <TableCell className="font-mono text-xs">
                {item.companyValue}
              </TableCell>
              <TableCell className="font-mono text-xs">
                {item.domainValue}
              </TableCell>
              <TableCell className="font-medium tabular-nums">
                +{item.scoreContribution} points
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {evidence.map((item) => {
        const sourceUrl = safeUrl(item.sourceUrl);
        const warcUrl = safeUrl(
          item.warcFilename
            ? `https://data.commoncrawl.org/${item.warcFilename}`
            : "",
        );
        const captureWindow = warcCaptureWindow(item.warcFilename);
        return (
          <div
            key={`provenance:${item.signalType}:${item.sourceField}:${item.sourceUrl}`}
            className="grid gap-3 text-sm md:grid-cols-2"
          >
            <div className="flex items-start gap-2">
              <FileSearch className="text-muted-foreground mt-0.5 size-4 shrink-0" />
              <div>
                <div className="font-medium">Verification page</div>
                <div className="text-muted-foreground">
                  Extracted from {extractionMethodLabel(item.extractionMethod)}.
                </div>
                {sourceUrl ? (
                  <a
                    href={sourceUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-1 underline underline-offset-2"
                  >
                    Open evidence page
                    <ExternalLink className="size-3" />
                  </a>
                ) : null}
              </div>
            </div>
            <div className="flex items-start gap-2">
              <CalendarClock className="text-muted-foreground mt-0.5 size-4 shrink-0" />
              <div>
                <div className="font-medium">WARC capture</div>
                <div className="text-muted-foreground">
                  {captureWindow ?? "Capture date unavailable"} · {item.crawlId}
                </div>
                {warcUrl ? (
                  <a
                    href={warcUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-1 underline underline-offset-2"
                  >
                    Open WARC file
                    <Archive className="size-3" />
                  </a>
                ) : null}
              </div>
            </div>
            {item.warcFilename ? (
              <div className="text-muted-foreground font-mono text-xs break-all md:col-span-2">
                {item.warcFilename} · byte {item.warcRecordOffset}, length{" "}
                {item.warcRecordLength}
              </div>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}

function WikidataEvidenceDetails({
  evidence,
}: {
  evidence: WikidataDomainEvidence[];
}) {
  if (evidence.length === 0) return null;
  return (
    <div className="flex flex-col gap-4">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Company link</TableHead>
            <TableHead>Company record</TableHead>
            <TableHead>Wikidata value</TableHead>
            <TableHead>Wikidata property</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {evidence.map((item) => (
            <TableRow key={item.sourceRecordId}>
              <TableCell>
                <div className="font-medium">
                  Exact {identifierLabel(item.identifierType)} match
                </div>
                <div className="text-muted-foreground text-xs">
                  Links {item.wikidataId} to this Swedish company
                </div>
              </TableCell>
              <TableCell className="font-mono text-xs">
                {item.companyValue}
              </TableCell>
              <TableCell className="font-mono text-xs">
                {item.wikidataValue}
              </TableCell>
              <TableCell>
                <a
                  href={`https://www.wikidata.org/wiki/Property:${item.propertyId}`}
                  target="_blank"
                  rel="noreferrer"
                  className="underline underline-offset-2"
                >
                  {item.propertyId}
                </a>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <div className="text-muted-foreground flex flex-wrap gap-x-4 gap-y-1 text-xs">
        <span>Official website claim: P856</span>
        <span>Wikidata item: {evidence[0].wikidataId}</span>
        <span>Identifier retrieved: {evidence[0].retrievedAt} UTC</span>
      </div>
    </div>
  );
}

function SourceEvidenceCard({ source }: { source: CompanyDomainSource }) {
  const sourceUrl = safeUrl(source.sourceUrl);
  const commonCrawlEvidence = source.evidence.filter(
    (evidence): evidence is CommonCrawlDomainEvidence =>
      evidence.type === "common_crawl_match",
  );
  const wikidataEvidence = source.evidence.filter(
    (evidence): evidence is WikidataDomainEvidence =>
      evidence.type === "wikidata_match",
  );
  return (
    <Card size="sm">
      <CardHeader>
        <CardTitle>
          <Badge variant="outline">
            {sourceLabels[source.name] ?? source.name}
          </Badge>
        </CardTitle>
        <CardDescription>{sourceBasisDescription(source)}</CardDescription>
        <CardAction>
          <Badge variant="secondary">
            {percent.format(source.confidence)} confidence
          </Badge>
        </CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <CommonCrawlEvidenceDetails evidence={commonCrawlEvidence} />
        <WikidataEvidenceDetails evidence={wikidataEvidence} />
        {source.evidence.length === 0 && sourceUrl ? (
          <a
            href={sourceUrl}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 underline underline-offset-2"
          >
            Open source evidence
            <ExternalLink className="size-3" />
          </a>
        ) : null}
      </CardContent>
    </Card>
  );
}

function ReviewControls({
  domain,
  action,
}: {
  domain: CompanyDomain;
  action: string;
}) {
  const fetcher = useFetcher<ReviewActionData>();
  const submitting = fetcher.state !== "idle";

  return (
    <div className="flex flex-col gap-3">
      <fetcher.Form
        method="post"
        action={action}
        className="flex flex-wrap gap-2"
      >
        <input type="hidden" name="root_domain" value={domain.rootDomain} />
        <Button
          type="submit"
          name="review_status"
          value="confirmed_primary"
          size="sm"
          variant={
            domain.reviewStatus === "confirmed_primary" ? "default" : "outline"
          }
          disabled={submitting || domain.reviewStatus === "confirmed_primary"}
        >
          {submitting ? (
            <LoaderCircle data-icon="inline-start" className="animate-spin" />
          ) : (
            <CheckCircle2 data-icon="inline-start" />
          )}
          Confirm primary
        </Button>
        <Button
          type="submit"
          name="review_status"
          value="confirmed_related"
          size="sm"
          variant={
            domain.reviewStatus === "confirmed_related"
              ? "secondary"
              : "outline"
          }
          disabled={submitting || domain.reviewStatus === "confirmed_related"}
        >
          <Link2 data-icon="inline-start" />
          Confirm related
        </Button>
        <Button
          type="submit"
          name="review_status"
          value="rejected"
          size="sm"
          variant={
            domain.reviewStatus === "rejected" ? "destructive" : "outline"
          }
          disabled={submitting || domain.reviewStatus === "rejected"}
        >
          <XCircle data-icon="inline-start" />
          Reject
        </Button>
        {domain.reviewStatus !== "unreviewed" ? (
          <Button
            type="submit"
            name="review_status"
            value="unreviewed"
            size="sm"
            variant="ghost"
            disabled={submitting}
          >
            <RotateCcw data-icon="inline-start" />
            Clear review
          </Button>
        ) : null}
      </fetcher.Form>

      {fetcher.data && !fetcher.data.ok ? (
        <Alert variant="destructive">
          <AlertTitle>Review was not saved</AlertTitle>
          <AlertDescription>{fetcher.data.error}</AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}

function DomainCard({
  domain,
  reviewAction,
  technologyPath,
}: {
  domain: CompanyDomain;
  reviewAction: string;
  technologyPath: string;
}) {
  const websiteUrl = safeUrl(
    domain.websiteUrl || `https://${domain.rootDomain}`,
  );
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          {websiteUrl ? (
            <a
              href={websiteUrl}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-2 underline-offset-4 hover:underline"
            >
              {domain.rootDomain}
              <ExternalLink />
            </a>
          ) : (
            domain.rootDomain
          )}
        </CardTitle>
        <CardDescription>
          Proposed by {domain.sources.length} source
          {domain.sources.length === 1 ? "" : "s"}. Automated confidence does
          not replace human verification.
        </CardDescription>
        <CardAction className="flex flex-wrap gap-2">
          <Badge variant={reviewBadgeVariant(domain.reviewStatus)}>
            {reviewLabels[domain.reviewStatus]}
          </Badge>
          <Badge variant="outline">
            {percent.format(domain.suggestedConfidence)} suggested confidence
          </Badge>
          {domain.evidenceChanged ? (
            <Badge variant="destructive">Source evidence changed</Badge>
          ) : null}
        </CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        <div className="flex flex-col gap-3">
          <div>
            <h3 className="font-medium">Source evidence</h3>
            <p className="text-muted-foreground text-sm">
              Why each source links this domain to this company.
            </p>
          </div>
          {domain.sources.map((source) => (
            <SourceEvidenceCard
              key={`${source.name}:${source.sourceRecordId}`}
              source={source}
            />
          ))}
        </div>
        <ReviewControls domain={domain} action={reviewAction} />
      </CardContent>
      <CardFooter className="justify-between gap-3">
        <span className="text-muted-foreground text-xs">
          First observed {domain.firstSeenAt}
        </span>
        <Button
          variant="outline"
          size="sm"
          nativeButton={false}
          render={
            <Link
              to={`${technologyPath}?domain=${encodeURIComponent(domain.rootDomain)}`}
            />
          }
        >
          Inspect technology
        </Button>
      </CardFooter>
    </Card>
  );
}

export function CompanyDomainSuggestionsSection({
  domains,
  reviewAction,
  technologyPath,
}: {
  domains: CompanyDomain[];
  reviewAction: string;
  technologyPath: string;
}) {
  return (
    <div className="flex w-full max-w-6xl flex-col gap-5">
      <header>
        <h2 className="text-xl font-semibold tracking-tight">
          Company domains
        </h2>
        <p className="text-muted-foreground mt-1 max-w-3xl text-sm">
          Every domain proposed by Wikidata, ESEF filings, or deterministic
          Common Crawl matching. Review decisions apply to the company/domain
          association, not to an individual source.
        </p>
      </header>

      {domains.length === 0 ? (
        <Empty>
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <SearchX />
            </EmptyMedia>
            <EmptyTitle>No associated domains</EmptyTitle>
            <EmptyDescription>
              No source currently proposes a domain for this Swedish company.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div className="flex flex-col gap-4">
          {domains.map((domain) => (
            <DomainCard
              key={domain.rootDomain}
              domain={domain}
              reviewAction={reviewAction}
              technologyPath={technologyPath}
            />
          ))}
        </div>
      )}
    </div>
  );
}
