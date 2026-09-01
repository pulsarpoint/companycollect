import { useState, type ReactNode } from "react";
import { Link } from "react-router";
import { ArrowLeft, ExternalLink, FileSearch } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
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
import { Input } from "~/components/ui/input";
import { XbrlFactsAccordion } from "~/components/detail/xbrl-facts-accordion";
import {
  esefConceptLabel,
  esefFactConceptLabels,
} from "~/lib/esef-financial-reports";
import type { getEsefFinancialReport } from "~/lib/esef-financial-reports.server";

// The one ESEF facts reader, shared by the public financials page and the
// admin per-document subpage. Only navigation targets differ between hosts,
// so they arrive as props.
export type EsefReport = NonNullable<
  Awaited<ReturnType<typeof getEsefFinancialReport>>
>;

const FACT_BATCH_SIZE = 200;

export function EsefReportView({
  report,
  backHref,
  backLabel,
  notesHref,
  extraActions,
}: {
  report: EsefReport;
  backHref: string;
  backLabel: string;
  notesHref: string;
  extraActions?: ReactNode;
}) {
  const { summary, facts } = report;
  const [filter, setFilter] = useState("");
  const [visibleLimit, setVisibleLimit] = useState(FACT_BATCH_SIZE);
  const needle = filter.trim().toLowerCase();
  const matchingFacts = needle
    ? facts.filter((fact) =>
        [
          fact.conceptQname,
          fact.conceptLocalName,
          esefConceptLabel(fact.conceptLocalName, fact.conceptQname),
          esefFactConceptLabels(fact).submitted,
          esefFactConceptLabels(fact).english,
          fact.rawValue,
          fact.unit,
          fact.currency,
          fact.language,
          fact.dimensions,
        ].some((value) => value.toLowerCase().includes(needle)),
      )
    : facts;
  const visibleFacts = matchingFacts.slice(0, visibleLimit);

  return (
    <div className="flex w-full flex-col gap-5">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to={backHref} />}
        >
          <ArrowLeft data-icon="inline-start" />
          {backLabel}
        </Button>
      </div>

      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex min-w-0 flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="break-all text-xl font-semibold tracking-tight">
              ESEF report · {summary.fiscalYear}
            </h2>
            <Badge variant="outline">Consolidated IFRS</Badge>
            {summary.filingVersion > 0 ? (
              <Badge variant="secondary">
                Amendment {summary.filingVersion}
              </Badge>
            ) : null}
          </div>
          <p className="text-muted-foreground text-sm">
            {summary.entityName} · period ending {summary.periodEnd} ·{" "}
            {summary.factCount.toLocaleString("en-US")} tagged facts
          </p>
          <p className="text-muted-foreground break-all font-mono text-xs">
            {summary.fxoId}
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          {extraActions}
          <Button
            variant="outline"
            nativeButton={false}
            render={<Link to={notesHref} />}
          >
            Report notes
          </Button>
          {summary.sourceUrl ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={
                <a href={summary.sourceUrl} target="_blank" rel="noreferrer" />
              }
            >
              Source
              <ExternalLink data-icon="inline-end" />
            </Button>
          ) : null}
          {summary.viewerUrl ? (
            <Button
              nativeButton={false}
              render={
                <a href={summary.viewerUrl} target="_blank" rel="noreferrer" />
              }
            >
              Open report
              <ExternalLink data-icon="inline-end" />
            </Button>
          ) : null}
          {summary.packageUrl &&
          summary.packageUrl !== summary.sourceUrl &&
          summary.packageUrl !== summary.viewerUrl ? (
            <Button
              variant="ghost"
              nativeButton={false}
              render={
                <a href={summary.packageUrl} target="_blank" rel="noreferrer" />
              }
            >
              Package
              <ExternalLink data-icon="inline-end" />
            </Button>
          ) : null}
        </div>
      </div>

      <dl className="grid grid-cols-2 rounded-xl bg-muted/35 px-4 ring-1 ring-foreground/10 lg:grid-cols-5">
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:pr-4">
          <dt className="text-muted-foreground text-xs">Tagged facts</dt>
          <dd className="mt-1 font-medium tabular-nums">
            {summary.factCount.toLocaleString("en-US")}
          </dd>
        </div>
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:px-4">
          <dt className="text-muted-foreground text-xs">
            Current-period facts
          </dt>
          <dd className="mt-1 font-medium tabular-nums">
            {summary.sourceFactCount.toLocaleString("en-US")}
          </dd>
        </div>
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:px-4">
          <dt className="text-muted-foreground text-xs">Standardized facts</dt>
          <dd className="mt-1 font-medium tabular-nums">
            {summary.mappedFactCount.toLocaleString("en-US")}
          </dd>
        </div>
        <div className="py-3 lg:border-r lg:px-4">
          <dt className="text-muted-foreground text-xs">Report currency</dt>
          <dd className="mt-1 font-medium">
            {summary.currency || "Mixed / unavailable"}
          </dd>
        </div>
        <div className="py-3 lg:pl-4">
          <dt className="text-muted-foreground text-xs">Validation</dt>
          <dd className="mt-1 font-medium tabular-nums">
            {summary.errorCount} errors · {summary.warningCount} warnings
          </dd>
        </div>
      </dl>

      <Card className="min-w-0">
        <CardHeader className="border-b">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div>
              <CardTitle>Tagged source facts</CardTitle>
              <CardDescription>
                Exact ESEF concepts and values are shown before any standardized
                metric mapping.
              </CardDescription>
            </div>
            <Input
              value={filter}
              onChange={(event) => {
                setFilter(event.target.value);
                setVisibleLimit(FACT_BATCH_SIZE);
              }}
              placeholder="Search concepts, values, units, or dimensions…"
              aria-label="Search ESEF report facts"
              className="w-full sm:w-96"
            />
          </div>
        </CardHeader>
        <CardContent className="min-w-0">
          {visibleFacts.length === 0 ? (
            <Empty className="border">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <FileSearch />
                </EmptyMedia>
                <EmptyTitle>No matching facts</EmptyTitle>
                <EmptyDescription>
                  Try an IFRS concept, source value, currency, or dimension
                  member.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <>
              <XbrlFactsAccordion
                facts={visibleFacts}
                ariaLabel="ESEF report facts"
              />
              <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                <p className="text-muted-foreground text-xs">
                  Showing {visibleFacts.length.toLocaleString("en-US")} of{" "}
                  {matchingFacts.length.toLocaleString("en-US")} matching facts
                  {needle
                    ? ` · ${facts.length.toLocaleString("en-US")} in report`
                    : ""}
                  .
                </p>
                {visibleFacts.length < matchingFacts.length ? (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      setVisibleLimit((limit) => limit + FACT_BATCH_SIZE)
                    }
                  >
                    Show{" "}
                    {Math.min(
                      FACT_BATCH_SIZE,
                      matchingFacts.length - visibleFacts.length,
                    )}{" "}
                    more
                  </Button>
                ) : null}
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
