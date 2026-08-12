import {
  CheckCircle2,
  ExternalLink,
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
  CompanyDomain,
  CompanyDomainReviewStatus,
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
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Source</TableHead>
              <TableHead>Confidence</TableHead>
              <TableHead>Basis</TableHead>
              <TableHead>Evidence</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {domain.sources.map((source) => {
              const sourceUrl = safeUrl(source.sourceUrl);
              return (
                <TableRow key={`${source.name}:${source.sourceRecordId}`}>
                  <TableCell>
                    <Badge variant="outline">
                      {sourceLabels[source.name] ?? source.name}
                    </Badge>
                  </TableCell>
                  <TableCell className="font-medium tabular-nums">
                    {percent.format(source.confidence)}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {source.confidenceBasis.replaceAll("_", " ")}
                  </TableCell>
                  <TableCell>
                    {sourceUrl ? (
                      <a
                        href={sourceUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="underline underline-offset-2"
                      >
                        Open source
                      </a>
                    ) : (
                      <span className="text-muted-foreground">
                        No source URL
                      </span>
                    )}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
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
