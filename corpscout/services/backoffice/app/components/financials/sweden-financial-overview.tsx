import { useState, type ReactNode } from "react";
import {
  Check,
  CircleHelp,
  FileCheck2,
  FileSearch,
  FileText,
  Minus,
  X,
} from "lucide-react";
import { Link } from "react-router";
import {
  financialCopy,
  interpolate,
  type FinancialLocale,
} from "~/components/financials/copy";
import type { FinancialComparisonRow } from "~/components/financials/financial-comparison-table";
import { FinancialComparisonTable } from "~/components/financials/financial-comparison-table";
import { FinancialKpiStrip } from "~/components/financials/financial-kpi-strip";
import { FinancialTrendChart } from "~/components/financials/financial-trend-chart";
import { formatDate, type MoneyPair } from "~/components/financials/formatters";
import {
  calculationValue,
  financialMoney,
  hasFinancialData,
  latestFinancialRows,
  percentage,
  percentageChange,
  type FinancialMoneyMetric,
} from "~/components/financials/metrics";
import { EvidencePanel } from "~/components/detail/evidence";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { buttonVariants } from "~/components/ui/button";
import {
  Card,
  CardAction,
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
  Field,
  FieldLabel,
  FieldLegend,
  FieldSet,
} from "~/components/ui/field";
import { RadioGroup, RadioGroupItem } from "~/components/ui/radio-group";
import type { CompanyFinancialFilingStatus } from "~/lib/financial-filing-status";
import type { FinancialYearRow } from "~/lib/queries.server";

function perEmployee(
  financial: FinancialYearRow,
  metric: FinancialMoneyMetric,
): MoneyPair {
  const employees = financial.employees;
  const pair = financialMoney(financial, metric);
  if (employees == null || employees === 0) {
    return { original: null, usd: null, currency: pair.currency };
  }
  return {
    original: pair.original == null ? null : pair.original / employees,
    usd: pair.usd == null ? null : pair.usd / employees,
    currency: pair.currency,
  };
}

function hasComparisonRowValues(
  row: FinancialComparisonRow,
  financials: FinancialYearRow[],
): boolean {
  return financials.some((financial, index) => {
    const value = row.value(financial, index);
    if (typeof value === "number") return Number.isFinite(value);
    if (value === null) return false;
    return value.original != null || value.usd != null;
  });
}

function statusPresentation(
  filing: CompanyFinancialFilingStatus | null,
  locale: FinancialLocale,
) {
  const copy = financialCopy[locale];
  switch (filing?.status) {
    case "data_available":
      return {
        label: copy.available,
        icon: Check,
        variant: "default" as const,
      };
    case "filed_unstructured":
      return {
        label: copy.otherFormat,
        icon: Minus,
        variant: "secondary" as const,
      };
    case "not_submitted":
      return {
        label: copy.notSubmitted,
        icon: X,
        variant: "destructive" as const,
      };
    default:
      return {
        label: copy.unknownStatus,
        icon: CircleHelp,
        variant: "outline" as const,
      };
  }
}

export function SwedenFinancialOverview({
  financials,
  filingStatus,
  factsHref,
  children,
}: {
  financials: FinancialYearRow[];
  filingStatus: CompanyFinancialFilingStatus | null;
  factsHref?: (year: string) => string;
  children?: ReactNode;
}) {
  const [locale, setLocale] = useState<FinancialLocale>("en");
  const copy = financialCopy[locale];
  const displayedFinancials = latestFinancialRows(financials);
  const unavailableFinancials = financials.filter(
    (financial) => !hasFinancialData(financial),
  );
  const latest =
    displayedFinancials.find(
      (financial) => financial.observation !== "comparative",
    ) ?? displayedFinancials[0];
  const previous = latest
    ? displayedFinancials.find(
        (financial) => financial.fiscal_year !== latest.fiscal_year,
      )
    : undefined;
  const status = statusPresentation(filingStatus, locale);
  const StatusIcon = status.icon;
  const years = displayedFinancials.map((financial) => financial.fiscal_year);
  const yearRange = years.length ? `${years.at(-1)}–${years[0]}` : null;
  const factsLabel = (sourceYear: string) =>
    interpolate(copy.fromFiling, { year: sourceYear });

  const ratioRows = (
    [
      {
        label: copy.ratios.rows.currentRatio,
        value: (financial) =>
          percentage(
            calculationValue(financial, "currentAssets"),
            calculationValue(financial, "currentLiabilities"),
          ),
        format: "percentage",
      },
      {
        label: copy.ratios.rows.equityRatio,
        value: (financial) =>
          percentage(
            calculationValue(financial, "equity"),
            calculationValue(financial, "totalAssets"),
          ),
        format: "percentage",
      },
      {
        label: copy.ratios.rows.operatingMargin,
        value: (financial) =>
          percentage(
            calculationValue(financial, "operatingResult"),
            calculationValue(financial, "revenue"),
          ),
        format: "percentage",
      },
      {
        label: copy.ratios.rows.staffCostsPerEmployee,
        value: (financial) => perEmployee(financial, "personnelExpenses"),
      },
      {
        label: copy.ratios.rows.revenuePerEmployee,
        value: (financial) => perEmployee(financial, "revenue"),
      },
      {
        label: copy.ratios.rows.revenueChange,
        value: (financial, index) => {
          const older = displayedFinancials[index + 1];
          return older
            ? percentageChange(
                calculationValue(financial, "revenue"),
                calculationValue(older, "revenue"),
              )
            : null;
        },
        format: "percentage",
      },
      {
        label: copy.ratios.rows.employees,
        value: (financial) => financial.employees,
        format: "count",
      },
    ] satisfies FinancialComparisonRow[]
  ).filter((row) => hasComparisonRowValues(row, displayedFinancials));

  const incomeStatementRows = (
    [
      {
        label: copy.incomeStatement.rows.revenue,
        value: (financial) => financialMoney(financial, "revenue"),
        emphasis: true,
      },
      {
        label: copy.incomeStatement.rows.personnelExpenses,
        value: (financial) => financialMoney(financial, "personnelExpenses"),
        indent: true,
      },
      {
        label: copy.incomeStatement.rows.wagesAndSalaries,
        value: (financial) => financialMoney(financial, "wagesAndSalaries"),
        indent: true,
      },
      {
        label: copy.incomeStatement.rows.operatingResult,
        value: (financial) => financialMoney(financial, "operatingResult"),
        emphasis: true,
      },
      {
        label: copy.incomeStatement.rows.netResult,
        value: (financial) => financialMoney(financial, "netResult"),
        emphasis: true,
      },
    ] satisfies FinancialComparisonRow[]
  ).filter((row) => hasComparisonRowValues(row, displayedFinancials));

  const balanceSheetRows = (
    [
      {
        label: copy.balanceSheet.rows.cashAndBank,
        value: (financial) => financialMoney(financial, "cashAndBank"),
        indent: true,
      },
      {
        label: copy.balanceSheet.rows.currentAssets,
        value: (financial) => financialMoney(financial, "currentAssets"),
        indent: true,
      },
      {
        label: copy.balanceSheet.rows.totalAssets,
        value: (financial) => financialMoney(financial, "totalAssets"),
        emphasis: true,
      },
      {
        label: copy.balanceSheet.rows.equity,
        value: (financial) => financialMoney(financial, "equity"),
        emphasis: true,
      },
      {
        label: copy.balanceSheet.rows.liabilities,
        value: (financial) => financialMoney(financial, "liabilities"),
        emphasis: true,
      },
      {
        label: copy.balanceSheet.rows.currentLiabilities,
        value: (financial) => financialMoney(financial, "currentLiabilities"),
        indent: true,
      },
    ] satisfies FinancialComparisonRow[]
  ).filter((row) => hasComparisonRowValues(row, displayedFinancials));

  const period = latest
    ? latest.report_period_start && latest.report_period_end
      ? `${formatDate(latest.report_period_start, locale)} – ${formatDate(latest.report_period_end, locale)}`
      : latest.report_period_end
        ? formatDate(latest.report_period_end, locale)
        : null
    : null;
  const filingPeriodNote = period
    ? locale === "sv"
      ? ` för perioden ${period}`
      : ` for the period ${period}`
    : "";
  const fxSource = latest?.fx_source || (locale === "sv" ? "lagrad" : "stored");
  const fxDate = latest?.fx_rate_date
    ? locale === "sv"
      ? ` den ${formatDate(latest.fx_rate_date, locale)}`
      : ` on ${formatDate(latest.fx_rate_date, locale)}`
    : "";

  return (
    <main
      lang={locale}
      className="flex w-full animate-in flex-col gap-10 fade-in slide-in-from-bottom-2 duration-500"
    >
      <header className="flex flex-col gap-6">
        <div className="flex flex-wrap items-start justify-between gap-5">
          <div className="flex min-w-0 flex-col gap-3">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="secondary">
                <FileCheck2 data-icon="inline-start" />
                {copy.source}
              </Badge>
              {yearRange ? <Badge variant="outline">{yearRange}</Badge> : null}
              <Badge variant="outline">SEK / USD</Badge>
            </div>
            <div className="flex flex-col gap-1">
              <h2 className="text-2xl font-semibold tracking-tight sm:text-3xl">
                {copy.pageTitle}
              </h2>
              <p className="text-muted-foreground max-w-3xl text-sm">
                {copy.pageDescription}
              </p>
            </div>
          </div>

          <FieldSet className="w-fit gap-2">
            <FieldLegend variant="label">{copy.language}</FieldLegend>
            <RadioGroup
              value={locale}
              onValueChange={(value) => setLocale(value as FinancialLocale)}
              className="flex w-fit items-center gap-4"
              aria-label={copy.language}
            >
              <Field orientation="horizontal" className="w-fit">
                <RadioGroupItem value="sv" id="financial-language-sv" />
                <FieldLabel
                  htmlFor="financial-language-sv"
                  className="font-normal"
                >
                  Svenska
                </FieldLabel>
              </Field>
              <Field orientation="horizontal" className="w-fit">
                <RadioGroupItem value="en" id="financial-language-en" />
                <FieldLabel
                  htmlFor="financial-language-en"
                  className="font-normal"
                >
                  English
                </FieldLabel>
              </Field>
            </RadioGroup>
          </FieldSet>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Badge variant={status.variant}>
            <StatusIcon data-icon="inline-start" />
            {status.label}
          </Badge>
          <span className="text-muted-foreground text-xs">
            {copy.sourceDescription}
          </span>
        </div>
      </header>

      {displayedFinancials.length === 0 || !latest ? (
        <Empty className="border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <FileText />
            </EmptyMedia>
            <EmptyTitle>{copy.noDataTitle}</EmptyTitle>
            <EmptyDescription>{copy.noDataDescription}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <>
          <section id="overview" className="flex scroll-mt-6 flex-col gap-6">
            <div className="flex flex-wrap items-end justify-between gap-4">
              <div className="flex flex-col gap-1">
                <p className="text-muted-foreground text-sm">
                  {copy.latestFiling}
                </p>
                <h2 className="text-xl font-semibold tracking-tight">
                  {copy.financialYear} {latest.fiscal_year}
                </h2>
              </div>
              <div className="flex flex-col items-end gap-3">
                <div className="text-muted-foreground text-right text-sm">
                  {period ? <p>{period}</p> : null}
                  <p>
                    {latest.currency || "SEK"} / USD · {copy.standaloneAccounts}
                  </p>
                </div>
                <div className="flex flex-wrap items-center justify-end gap-2">
                  <EvidencePanel evidence={latest.evidence ?? []} />
                  {factsHref && latest.observation !== "comparative" ? (
                    <Link
                      to={factsHref(latest.fiscal_year)}
                      className={buttonVariants({
                        variant: "outline",
                        size: "sm",
                      })}
                    >
                      <FileSearch data-icon="inline-start" />
                      {copy.allSourceFacts}
                    </Link>
                  ) : null}
                </div>
              </div>
            </div>
            <FinancialKpiStrip
              latest={latest}
              previous={previous}
              locale={locale}
              copy={copy.kpis}
            />
          </section>

          <FinancialTrendChart
            data={displayedFinancials}
            locale={locale}
            copy={copy.chart}
          />

          {ratioRows.length > 0 ? (
            <FinancialComparisonTable
              id="ratios"
              title={copy.ratios.title}
              description={copy.ratios.description}
              data={displayedFinancials}
              rows={ratioRows}
              locale={locale}
              factsHref={factsHref}
              factsLabel={copy.sourceFactsForYear}
              comparativeLabel={factsLabel}
            />
          ) : null}

          {incomeStatementRows.length > 0 ? (
            <FinancialComparisonTable
              id="income-statement"
              title={copy.incomeStatement.title}
              description={copy.incomeStatement.description}
              data={displayedFinancials}
              rows={incomeStatementRows}
              locale={locale}
              factsHref={factsHref}
              factsLabel={copy.sourceFactsForYear}
              comparativeLabel={factsLabel}
            />
          ) : null}

          {balanceSheetRows.length > 0 ? (
            <FinancialComparisonTable
              id="balance-sheet"
              title={copy.balanceSheet.title}
              description={copy.balanceSheet.description}
              data={displayedFinancials}
              rows={balanceSheetRows}
              locale={locale}
              factsHref={factsHref}
              factsLabel={copy.sourceFactsForYear}
              comparativeLabel={factsLabel}
            />
          ) : null}

          {unavailableFinancials.length > 0 ? (
            <Alert>
              <FileText />
              <AlertTitle>{copy.unavailableYears}</AlertTitle>
              <AlertDescription className="flex flex-col gap-2">
                <p>{copy.unavailableYearsDescription}</p>
                <div className="flex flex-wrap gap-2">
                  {unavailableFinancials.map((financial) =>
                    factsHref && financial.observation !== "comparative" ? (
                      <Link
                        key={financial.fiscal_year}
                        to={factsHref(financial.fiscal_year)}
                        className="underline underline-offset-4"
                      >
                        {financial.fiscal_year}
                      </Link>
                    ) : (
                      <span key={financial.fiscal_year}>
                        {financial.fiscal_year}
                      </span>
                    ),
                  )}
                </div>
              </AlertDescription>
            </Alert>
          ) : null}

          <Card size="sm">
            <CardHeader>
              <CardTitle>{copy.notes.title}</CardTitle>
              <CardDescription>{copy.notes.description}</CardDescription>
              {latest.source_fact_count ? (
                <CardAction>
                  <Badge variant="outline">
                    {interpolate(copy.notes.taggedFacts, {
                      count: latest.source_fact_count.toLocaleString(
                        locale === "sv" ? "sv-SE" : "en-US",
                      ),
                      year: latest.fiscal_year,
                    })}
                  </Badge>
                </CardAction>
              ) : null}
            </CardHeader>
            <CardContent className="grid gap-5 md:grid-cols-3">
              <div className="flex flex-col gap-1">
                <FileCheck2 className="text-muted-foreground size-5" />
                <p className="font-medium">{copy.notes.officialFiling}</p>
                <p className="text-muted-foreground text-sm">
                  {interpolate(copy.notes.officialFilingDescription, {
                    period: filingPeriodNote,
                  })}
                </p>
              </div>
              <div className="flex flex-col gap-1">
                <p className="font-medium">{copy.notes.currencyPair}</p>
                <p className="text-muted-foreground text-sm">
                  {interpolate(copy.notes.currencyPairDescription, {
                    source: fxSource,
                    date: fxDate,
                  })}
                </p>
              </div>
              <div className="flex flex-col gap-1">
                <p className="font-medium">{copy.notes.calculatedRatios}</p>
                <p className="text-muted-foreground text-sm">
                  {copy.notes.calculatedRatiosDescription}
                </p>
              </div>
            </CardContent>
          </Card>
        </>
      )}

      {children}
    </main>
  );
}
