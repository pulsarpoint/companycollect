import { CoinsIcon } from "lucide-react";
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type {
  SeCompanyFinancialDetail,
  SeCompanyFinancialReportRow,
  SeCompanyFinancialYearRow,
  SeCompanyFinancialsLatestRow,
} from "~/lib/se-company-financial.server";

const EMPTY_VALUE = <span className="text-muted-foreground">—</span>;

/** The label each serving view carries on this page. */
const SOURCE_LABELS: Record<string, { title: string; description: string }> = {
  "bolagsverket-annual-accounts": {
    title: "Bolagsverket annual accounts",
    description:
      "Standalone legal-entity accounts filed with Bolagsverket. A comparative year is a prior-year column lifted from a later filing, not its own report.",
  },
  esef: {
    title: "ESEF consolidated IFRS",
    description:
      "Consolidated IFRS figures extracted from filed ESEF reports. Group accounts stay distinct from the standalone filings above.",
  },
};

/**
 * Amounts are shown exactly as stored, only grouped: these are register
 * figures a reviewer checks against a filing, so rounding or unit-scaling
 * them would make that check impossible. Anything that is not a plain number
 * (already-formatted text, a value past Number's safe range) is passed
 * through untouched.
 */
const groupedNumber = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 6,
});

function formatAmount(value: string): React.ReactNode {
  if (value === "") return EMPTY_VALUE;
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || Math.abs(numeric) > Number.MAX_SAFE_INTEGER) {
    return <span className="tabular-nums">{value}</span>;
  }
  return <span className="tabular-nums">{groupedNumber.format(numeric)}</span>;
}

function text(value: string): React.ReactNode {
  return value === "" ? EMPTY_VALUE : value;
}

/** Every column of se_company_financials_latest, under its own label. */
function LatestCard({
  latest,
  companyId,
}: {
  latest: SeCompanyFinancialsLatestRow;
  companyId: string;
}) {
  const currency = latest.currency === "" ? "original currency" : latest.currency;
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle>Latest figures</CardTitle>
          {latest.fiscal_year === "" ? null : (
            <Badge>{latest.fiscal_year}</Badge>
          )}
          {latest.currency === "" ? null : (
            <Badge variant="secondary">{latest.currency}</Badge>
          )}
          <Badge variant="outline">{latest.years_count} years resolved</Badge>
        </div>
        <CardDescription>
          The se_company_financials_latest row every surface reads for this
          company. Amounts are shown as stored, in {currency} and in USD.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Figure</TableHead>
              <TableHead className="text-right">{currency}</TableHead>
              <TableHead className="text-right">USD</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {[
              ["Revenue", latest.revenue_amount_original, latest.revenue_amount_usd],
              [
                "Net result",
                latest.net_result_amount_original,
                latest.net_result_amount_usd,
              ],
              [
                "Total assets",
                latest.total_assets_amount_original,
                latest.total_assets_amount_usd,
              ],
              ["Equity", latest.equity_amount_original, latest.equity_amount_usd],
            ].map(([label, original, usd]) => (
              <TableRow key={label}>
                <TableCell>{label}</TableCell>
                <TableCell className="text-right">
                  {formatAmount(original)}
                </TableCell>
                <TableCell className="text-right">{formatAmount(usd)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <dl className="mt-4 grid grid-cols-1 gap-x-6 gap-y-1 text-sm sm:grid-cols-[minmax(11rem,auto)_1fr]">
          {[
            ["Fiscal year", text(latest.fiscal_year)],
            ["Period end", text(latest.period_end_date)],
            ["Employees", formatAmount(latest.employees)],
            ["Years resolved", text(latest.years_count)],
            ["Resolved at", text(latest.resolved_at)],
            [
              "Facts",
              latest.fiscal_year === "" ? (
                EMPTY_VALUE
              ) : (
                <Link
                  className="underline underline-offset-2"
                  to={`/company/se/${encodeURIComponent(companyId)}/facts/${latest.fiscal_year}`}
                >
                  Tagged facts for {latest.fiscal_year}
                </Link>
              ),
            ],
          ].map(([label, value]) => (
            <div key={String(label)} className="contents">
              <dt className="text-muted-foreground text-xs uppercase tracking-wide sm:pt-0.5">
                {label}
              </dt>
              <dd className="mb-2 sm:mb-0">{value}</dd>
            </div>
          ))}
        </dl>
      </CardContent>
    </Card>
  );
}

const YEAR_FIGURES: Array<[string, keyof SeCompanyFinancialYearRow]> = [
  ["Revenue", "revenue_amount_original"],
  ["Operating result", "operating_result_amount_original"],
  ["Net result", "net_result_amount_original"],
  ["Total assets", "total_assets_amount_original"],
  ["Equity", "equity_amount_original"],
  ["Liabilities", "liabilities_amount_original"],
  ["Cash and bank", "cash_and_bank_amount_original"],
  ["Current assets", "current_assets_amount_original"],
  ["Current liabilities", "current_liabilities_amount_original"],
  ["Personnel expenses", "personnel_expenses_amount_original"],
  ["Wages and salaries", "wages_and_salaries_amount_original"],
  ["Employees", "employees"],
];

/**
 * One source's years as a matrix: figures down, years across. A register
 * reader compares one figure across years far more often than one year across
 * figures, and this way a source that maps only four of the twelve rows shows
 * exactly which eight it leaves empty.
 */
function SourceYearsCard({
  sourceId,
  years,
}: {
  sourceId: string;
  years: SeCompanyFinancialYearRow[];
}) {
  const label = SOURCE_LABELS[sourceId];
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle className="text-base">
            {label?.title ?? sourceId}
          </CardTitle>
          <Badge variant="outline">{years.length} years</Badge>
          {years[0]?.accounting_scope ? (
            <Badge variant="secondary">{years[0].accounting_scope}</Badge>
          ) : null}
        </div>
        <CardDescription>{label?.description ?? sourceId}</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Figure</TableHead>
              {years.map((year) => (
                <TableHead
                  key={year.source_document_id}
                  className="text-right tabular-nums"
                >
                  {year.fiscal_year === "" ? "—" : year.fiscal_year}
                  <span className="ml-1 text-xs font-normal text-muted-foreground">
                    {year.currency}
                  </span>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {YEAR_FIGURES.map(([figureLabel, key]) => (
              <TableRow key={figureLabel}>
                <TableCell>{figureLabel}</TableCell>
                {years.map((year) => (
                  <TableCell
                    key={year.source_document_id}
                    className="text-right"
                  >
                    {formatAmount(String(year[key]))}
                  </TableCell>
                ))}
              </TableRow>
            ))}
            <TableRow>
              <TableCell>Observation</TableCell>
              {years.map((year) => (
                <TableCell key={year.source_document_id} className="text-right">
                  <Badge
                    variant={
                      year.observation === "filed" ? "secondary" : "outline"
                    }
                  >
                    {year.observation}
                  </Badge>
                </TableCell>
              ))}
            </TableRow>
            <TableRow>
              <TableCell>Period</TableCell>
              {years.map((year) => (
                <TableCell
                  key={year.source_document_id}
                  className="text-right text-xs tabular-nums"
                >
                  {year.report_period_start === "" && year.report_period_end === ""
                    ? EMPTY_VALUE
                    : `${year.report_period_start} → ${year.report_period_end}`}
                </TableCell>
              ))}
            </TableRow>
            <TableRow>
              <TableCell>FX to USD</TableCell>
              {years.map((year) => (
                <TableCell
                  key={year.source_document_id}
                  className="text-right text-xs"
                >
                  {year.fx_rate_to_usd === "" ? (
                    EMPTY_VALUE
                  ) : (
                    <span className="tabular-nums">
                      {year.fx_rate_to_usd}
                      {year.fx_rate_date === "" ? "" : ` (${year.fx_rate_date})`}
                    </span>
                  )}
                </TableCell>
              ))}
            </TableRow>
            <TableRow>
              <TableCell>Facts mapped</TableCell>
              {years.map((year) => (
                <TableCell
                  key={year.source_document_id}
                  className="text-right text-xs tabular-nums"
                >
                  {year.mapped_fact_count} / {year.source_fact_count}
                </TableCell>
              ))}
            </TableRow>
            <TableRow>
              <TableCell>Document</TableCell>
              {years.map((year) => (
                <TableCell
                  key={year.source_document_id}
                  className="text-right text-xs"
                >
                  {year.viewer_url === "" ? (
                    <code className="font-mono">
                      {year.source_document_id.slice(0, 12)}
                    </code>
                  ) : (
                    <a
                      className="underline underline-offset-2"
                      href={year.viewer_url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      viewer
                    </a>
                  )}
                </TableCell>
              ))}
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

function ReportsCard({
  reports,
  companyId,
}: {
  reports: SeCompanyFinancialReportRow[];
  companyId: string;
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle className="text-base">Filed reports</CardTitle>
          <Badge variant="outline">{reports.length}</Badge>
        </div>
        <CardDescription>
          Every XBRL/iXBRL report the parser recorded for this company, newest
          period first. One period can carry several reports: the register
          publishes the annual report and the structured accounts as separate
          documents.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Fiscal year</TableHead>
              <TableHead>Period</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Reported name</TableHead>
              <TableHead>Taxonomy</TableHead>
              <TableHead className="text-right">Facts</TableHead>
              <TableHead>Document</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {reports.map((report) => (
              <TableRow key={report.statement_key}>
                <TableCell className="tabular-nums">
                  {report.fiscal_year === "" ? (
                    EMPTY_VALUE
                  ) : (
                    <Link
                      className="underline underline-offset-2"
                      to={`/company/se/${encodeURIComponent(companyId)}/facts/${report.fiscal_year}`}
                    >
                      {report.fiscal_year}
                    </Link>
                  )}
                </TableCell>
                <TableCell className="text-xs tabular-nums">
                  {report.report_period_start} → {report.report_period_end}
                </TableCell>
                <TableCell className="text-xs">{report.source_slug}</TableCell>
                <TableCell className="text-xs">
                  {text(report.reported_company_name)}
                </TableCell>
                <TableCell className="max-w-[18rem] truncate text-xs">
                  {text(report.taxonomy_entrypoint)}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {report.facts_count}
                </TableCell>
                <TableCell className="text-xs">
                  <code className="font-mono">
                    {report.statement_key.slice(0, 12)}
                  </code>
                  <span className="ml-1 text-muted-foreground">
                    {report.source_archive_name}
                  </span>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

/**
 * What this company has filed, from three angles: the one row every surface
 * serves, the per-source year matrices behind it, and the raw report list the
 * figures were parsed from.
 */
export function SeCompanyFinancialTab({
  companyId,
  detail,
}: {
  companyId: string;
  detail: SeCompanyFinancialDetail;
}) {
  const sourcesWithYears = detail.sources.filter(
    (source) => source.years.length > 0,
  );
  const nothing =
    detail.latest === null &&
    sourcesWithYears.length === 0 &&
    detail.reports.length === 0;
  if (nothing) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <CoinsIcon />
          </EmptyMedia>
          <EmptyTitle>No financials recorded</EmptyTitle>
          <EmptyDescription>
            No annual accounts have been filed, parsed or resolved for this
            company yet.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <section className="flex flex-col gap-4">
      {detail.latest ? (
        <LatestCard latest={detail.latest} companyId={companyId} />
      ) : null}
      {sourcesWithYears.map((source) => (
        <SourceYearsCard
          key={source.source_id}
          sourceId={source.source_id}
          years={source.years}
        />
      ))}
      {detail.reports.length > 0 ? (
        <ReportsCard reports={detail.reports} companyId={companyId} />
      ) : null}
    </section>
  );
}
