import { useState } from "react";
import { Link } from "react-router";
import { ArrowLeft, ExternalLink, FileSearch, FileText } from "lucide-react";
import type { Route } from "./+types/company-financial-report";
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { getCountry } from "~/lib/countries";
import {
  formatFileSize,
  parseWarnings,
  statementTypeLabel,
  type NorwayFinancialFact,
} from "~/lib/norway-financial-reports";
import { getNorwayFinancialReport } from "~/lib/norway-financial-reports.server";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country?.detail?.financialReports || country.code !== "no") {
    throw new Response("Not found", { status: 404 });
  }
  const report = await getNorwayFinancialReport(params.id, params.documentId);
  if (!report) throw new Response("Report not found", { status: 404 });
  return report;
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.summary.sourceFileName ?? params.documentId} – CompanyCollect Backoffice`,
    },
  ];
}

function dateOnly(value: string): string {
  return value ? value.slice(0, 10) : "Unavailable";
}

function extractionLabel(nativeTextPages: number, ocrPages: number): string {
  if (nativeTextPages > 0 && ocrPages > 0) {
    return `${nativeTextPages} text · ${ocrPages} OCR`;
  }
  if (ocrPages > 0) return `${ocrPages} OCR pages`;
  if (nativeTextPages > 0) return `${nativeTextPages} text pages`;
  return "Metadata unavailable";
}

function displayWarning(warning: string): string {
  return warning.replaceAll("_", " ");
}

function FactValue({ fact }: { fact: NorwayFinancialFact }) {
  if (fact.rawValue.length > 220) {
    return (
      <details className="text-left">
        <summary className="text-muted-foreground cursor-pointer select-none">
          {fact.rawValue.slice(0, 160)}… <span className="text-primary">more</span>
        </summary>
        <p className="mt-2 break-words whitespace-pre-wrap">{fact.rawValue}</p>
      </details>
    );
  }
  const showCurrency =
    fact.currency && fact.valueKind === "monetary" && fact.statementType !== "other";
  return (
    <>
      <span className="block break-words whitespace-pre-wrap">{fact.rawValue || "—"}</span>
      {showCurrency ? (
        <span className="text-muted-foreground mt-1 block text-xs">{fact.currency}</span>
      ) : null}
    </>
  );
}

const FACT_BATCH_SIZE = 100;

export default function CompanyFinancialReport({
  loaderData,
  params,
}: Route.ComponentProps) {
  const { summary, facts } = loaderData;
  const [filter, setFilter] = useState("");
  const [visibleLimit, setVisibleLimit] = useState(FACT_BATCH_SIZE);
  const needle = filter.trim().toLowerCase();
  const matchingFacts = needle
    ? facts.filter(
        (fact) =>
          fact.rawLabel.toLowerCase().includes(needle) ||
          fact.normalizedLabel.toLowerCase().includes(needle) ||
          (fact.canonicalConcept ?? "").toLowerCase().includes(needle) ||
          fact.rawValue.toLowerCase().includes(needle) ||
          fact.tableTitle.toLowerCase().includes(needle) ||
          statementTypeLabel(fact.statementType).toLowerCase().includes(needle),
      )
    : facts;
  const visibleFacts = matchingFacts.slice(0, visibleLimit);
  const mappedCount = facts.filter((fact) => fact.canonicalConcept).length;
  const warnings = parseWarnings(summary.parseWarnings);
  const backHref = `/company/${params.country}/${params.id}/financials`;

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
          All annual reports
        </Button>
      </div>

      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="break-all text-xl font-semibold tracking-tight">
              {summary.sourceFileName}
            </h2>
            <Badge variant="outline">{summary.filingYear}</Badge>
          </div>
          <p className="text-muted-foreground mt-1 text-sm">
            Official annual accounts · {facts.length.toLocaleString("en-US")} extracted facts
          </p>
        </div>
        <Button
          nativeButton={false}
          render={<a href={summary.sourceUrl} target="_blank" rel="noreferrer" />}
        >
          <FileText data-icon="inline-start" />
          Open source PDF
          <ExternalLink data-icon="inline-end" />
        </Button>
      </div>

      <dl className="grid grid-cols-2 rounded-xl bg-muted/35 px-4 ring-1 ring-foreground/10 lg:grid-cols-5">
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:pr-4">
          <dt className="text-muted-foreground text-xs">Pages</dt>
          <dd className="mt-1 font-medium tabular-nums">{summary.pageCount || "Unavailable"}</dd>
        </div>
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:px-4">
          <dt className="text-muted-foreground text-xs">Extraction</dt>
          <dd className="mt-1 font-medium">
            {extractionLabel(summary.nativeTextPageCount, summary.ocrPageCount)}
          </dd>
        </div>
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:px-4">
          <dt className="text-muted-foreground text-xs">Mapped facts</dt>
          <dd className="mt-1 font-medium tabular-nums">
            {mappedCount.toLocaleString("en-US")} / {facts.length.toLocaleString("en-US")}
          </dd>
        </div>
        <div className="py-3 lg:border-r lg:px-4">
          <dt className="text-muted-foreground text-xs">PDF size</dt>
          <dd className="mt-1 font-medium">{formatFileSize(summary.pdfSizeBytes) ?? "Unavailable"}</dd>
        </div>
        <div className="py-3 lg:pl-4">
          <dt className="text-muted-foreground text-xs">Processed</dt>
          <dd className="mt-1 font-medium tabular-nums">{dateOnly(summary.resolvedAt)}</dd>
        </div>
      </dl>

      {warnings.length > 0 ? (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-sm">
          <span className="font-medium">Extraction notes:</span>{" "}
          <span className="text-muted-foreground">
            {warnings.map(displayWarning).join(", ")}
          </span>
        </div>
      ) : null}

      <Card className="min-w-0">
        <CardHeader className="border-b">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div>
              <CardTitle>Extracted facts</CardTitle>
              <CardDescription>
                Values are shown exactly as read from the PDF; canonical mappings are secondary.
              </CardDescription>
            </div>
            <Input
              value={filter}
              onChange={(event) => {
                setFilter(event.target.value);
                setVisibleLimit(FACT_BATCH_SIZE);
              }}
              placeholder="Search labels, concepts, and values…"
              aria-label="Search extracted report facts"
              className="w-full sm:w-80"
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
                  Try a label, canonical concept, table title, or source value.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <>
              <Table className="min-w-[52rem] table-fixed">
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[31%]">Source label</TableHead>
                    <TableHead className="w-[15%]">Period</TableHead>
                    <TableHead className="w-[32%]">Source value</TableHead>
                    <TableHead className="w-[22%]">Location</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {visibleFacts.map((fact) => (
                    <TableRow key={fact.factOrdinal}>
                      <TableCell className="align-top whitespace-normal">
                        <div className="font-medium break-words">{fact.rawLabel || "Unlabelled fact"}</div>
                        {fact.canonicalConcept ? (
                          <div className="text-muted-foreground mt-1 break-words font-mono text-[11px]">
                            {fact.canonicalConcept}
                          </div>
                        ) : null}
                      </TableCell>
                      <TableCell className="align-top whitespace-normal">
                        <div className="tabular-nums">
                          {fact.columnLabel || fact.fiscalYear || "—"}
                        </div>
                        {fact.isComparative ? (
                          <div className="text-muted-foreground mt-1 text-xs">Comparative</div>
                        ) : null}
                      </TableCell>
                      <TableCell className="align-top whitespace-normal">
                        <FactValue fact={fact} />
                      </TableCell>
                      <TableCell className="align-top whitespace-normal">
                        <Badge variant="outline">{statementTypeLabel(fact.statementType)}</Badge>
                        <div className="text-muted-foreground mt-2 text-xs">
                          Page {fact.pageNumber || "—"}
                          {fact.tableTitle ? ` · ${fact.tableTitle}` : ""}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                <p className="text-muted-foreground text-xs">
                  Showing {visibleFacts.length.toLocaleString("en-US")} of{" "}
                  {matchingFacts.length.toLocaleString("en-US")} matching facts
                  {needle ? ` · ${facts.length.toLocaleString("en-US")} in report` : ""}.
                </p>
                {visibleFacts.length < matchingFacts.length ? (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setVisibleLimit((limit) => limit + FACT_BATCH_SIZE)}
                  >
                    Show {Math.min(FACT_BATCH_SIZE, matchingFacts.length - visibleFacts.length)} more
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
