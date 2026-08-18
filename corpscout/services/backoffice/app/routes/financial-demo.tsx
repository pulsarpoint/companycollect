import { useState } from "react";
import {
  ArrowLeft,
  ArrowUpRight,
  FileCheck2,
  FileSearch,
  FlaskConical,
} from "lucide-react";
import { Link } from "react-router";
import type { Route } from "./+types/financial-demo";
import { financialDemoCopy } from "~/components/financial-demo/copy";
import {
  financialDemoData,
  latestFinancialDemoPoint,
  previousFinancialDemoPoint,
} from "~/components/financial-demo/data";
import type { FinancialComparisonRow } from "~/components/financial-demo/financial-comparison-table";
import { FinancialComparisonTable } from "~/components/financial-demo/financial-comparison-table";
import type { FinancialDemoLocale } from "~/components/financial-demo/formatters";
import { FinancialKpiStrip } from "~/components/financial-demo/financial-kpi-strip";
import { FinancialTrendChart } from "~/components/financial-demo/financial-trend-chart";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Field,
  FieldLabel,
  FieldLegend,
  FieldSet,
} from "~/components/ui/field";
import { RadioGroup, RadioGroupItem } from "~/components/ui/radio-group";

export function meta({}: Route.MetaArgs) {
  return [{ title: "Financial page prototype – CompanyCollect Backoffice" }];
}

export default function FinancialDemo() {
  const [locale, setLocale] = useState<FinancialDemoLocale>("en");
  const copy = financialDemoCopy[locale];
  const factsHref = (year: number) => `/company/se/5569658767/facts/${year}`;

  const ratioRows: FinancialComparisonRow[] = [
    {
      label: copy.ratios.rows.quickRatio,
      value: (point) => (point.currentAssets / point.currentLiabilities) * 100,
      format: "percentage",
    },
    {
      label: copy.ratios.rows.equityRatio,
      value: (point) => (point.equity / point.totalAssets) * 100,
      format: "percentage",
    },
    {
      label: copy.ratios.rows.operatingMargin,
      value: (point) => (point.operatingResult / point.revenue) * 100,
      format: "percentage",
    },
    {
      label: copy.ratios.rows.ebitda,
      value: (point) => point.operatingResult,
    },
    {
      label: copy.ratios.rows.staffCostsPerEmployee,
      value: (point) => point.personnelExpenses / point.employees,
    },
    {
      label: copy.ratios.rows.revenuePerEmployee,
      value: (point) => point.revenue / point.employees,
    },
    {
      label: copy.ratios.rows.revenueChange,
      value: (point, index) => {
        const previous = financialDemoData[index - 1];
        return previous
          ? ((point.revenue - previous.revenue) / previous.revenue) * 100
          : null;
      },
      format: "percentage",
    },
  ];

  const incomeStatementRows: FinancialComparisonRow[] = [
    {
      label: copy.incomeStatement.rows.netTurnover,
      value: (point) => point.revenue,
      emphasis: true,
    },
    {
      label: copy.incomeStatement.rows.otherOperatingIncome,
      value: (point) => point.otherOperatingIncome,
      indent: true,
    },
    {
      label: copy.incomeStatement.rows.totalOperatingIncome,
      value: (point) => point.operatingIncome,
    },
    {
      label: copy.incomeStatement.rows.operatingExpenses,
      value: (point) => point.operatingExpenses,
    },
    {
      label: copy.incomeStatement.rows.operatingResult,
      value: (point) => point.operatingResult,
      emphasis: true,
    },
    {
      label: copy.incomeStatement.rows.resultAfterFinancialItems,
      value: (point) => point.profitAfterFinancialItems,
    },
    {
      label: copy.incomeStatement.rows.tax,
      value: (point) => point.tax,
      indent: true,
    },
    {
      label: copy.incomeStatement.rows.netResult,
      value: (point) => point.netResult,
      emphasis: true,
    },
  ];

  const balanceSheetRows: FinancialComparisonRow[] = [
    {
      label: copy.balanceSheet.rows.fixedAssets,
      value: (point) => point.fixedAssets,
      indent: true,
    },
    {
      label: copy.balanceSheet.rows.currentAssets,
      value: (point) => point.currentAssets,
      indent: true,
    },
    {
      label: copy.balanceSheet.rows.totalAssets,
      value: (point) => point.totalAssets,
      emphasis: true,
    },
    {
      label: copy.balanceSheet.rows.equity,
      value: (point) => point.equity,
      emphasis: true,
    },
    {
      label: copy.balanceSheet.rows.untaxedReserves,
      value: (point) => point.untaxedReserves,
      indent: true,
    },
    {
      label: copy.balanceSheet.rows.longTermLiabilities,
      value: (point) => point.longTermLiabilities,
      indent: true,
    },
    {
      label: copy.balanceSheet.rows.currentLiabilities,
      value: (point) => point.currentLiabilities,
      indent: true,
    },
    {
      label: copy.balanceSheet.rows.equityAndLiabilities,
      value: (point) => point.totalAssets,
      emphasis: true,
    },
  ];

  return (
    <main
      lang={locale}
      className="mx-auto flex w-full max-w-7xl animate-in flex-col gap-10 fade-in slide-in-from-bottom-2 duration-500"
    >
      <header className="flex flex-col gap-6">
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2 w-fit"
          nativeButton={false}
          render={
            <Link to="/company/se/5569658767/financials">
              <ArrowLeft data-icon="inline-start" />
              {copy.currentFinancialsPage}
            </Link>
          }
        />

        <div className="flex flex-wrap items-start justify-between gap-5">
          <div className="flex min-w-0 flex-col gap-3">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="secondary">
                <FlaskConical data-icon="inline-start" />
                {copy.prototypeData}
              </Badge>
              <Badge variant="outline">2021–2025</Badge>
              <Badge variant="outline">SEK / USD</Badge>
            </div>
            <div className="flex flex-col gap-1">
              <h1 className="text-3xl font-semibold tracking-tight sm:text-4xl">
                1404 Reklam AB
              </h1>
              <p className="text-muted-foreground font-mono text-sm">
                556965-8767 · {copy.annualOverview}
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-start justify-end gap-5">
            <FieldSet className="w-fit gap-2">
              <FieldLegend variant="label">{copy.language}</FieldLegend>
              <RadioGroup
                value={locale}
                onValueChange={(value) =>
                  setLocale(value as FinancialDemoLocale)
                }
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

            <Button
              variant="outline"
              nativeButton={false}
              render={
                <a
                  href="https://www.ratsit.se/5569658767-1404_Reklam_AB#ebita"
                  target="_blank"
                  rel="noreferrer"
                >
                  {copy.referencePage}
                  <ArrowUpRight data-icon="inline-end" />
                </a>
              }
            />
          </div>
        </div>

        <Alert>
          <FlaskConical />
          <AlertTitle>{copy.prototypeTitle}</AlertTitle>
          <AlertDescription>{copy.prototypeDescription}</AlertDescription>
        </Alert>
      </header>

      <section id="overview" className="flex scroll-mt-6 flex-col gap-6">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div className="flex flex-col gap-1">
            <p className="text-muted-foreground text-sm">{copy.latestFiling}</p>
            <h2 className="text-xl font-semibold tracking-tight">
              {copy.financialYear} {latestFinancialDemoPoint.year}
            </h2>
          </div>
          <div className="flex flex-col items-end gap-3">
            <div className="text-muted-foreground text-right text-sm">
              <p>
                {latestFinancialDemoPoint.periodStart} –{" "}
                {latestFinancialDemoPoint.periodEnd}
              </p>
              <p>{copy.filingBasis}</p>
            </div>
            <Button
              variant="outline"
              size="sm"
              nativeButton={false}
              render={<Link to={factsHref(latestFinancialDemoPoint.year)} />}
            >
              <FileSearch data-icon="inline-start" />
              {copy.allSourceFacts}
            </Button>
          </div>
        </div>
        <FinancialKpiStrip
          latest={latestFinancialDemoPoint}
          previous={previousFinancialDemoPoint}
          locale={locale}
          copy={copy.kpis}
        />
      </section>

      <FinancialTrendChart
        data={financialDemoData}
        locale={locale}
        copy={copy.chart}
      />

      <FinancialComparisonTable
        id="ratios"
        title={copy.ratios.title}
        description={copy.ratios.description}
        data={financialDemoData}
        rows={ratioRows}
        locale={locale}
        factsHref={factsHref}
        factsLabel={copy.sourceFactsForYear}
      />

      <FinancialComparisonTable
        id="income-statement"
        title={copy.incomeStatement.title}
        description={copy.incomeStatement.description}
        data={financialDemoData}
        rows={incomeStatementRows}
        locale={locale}
        factsHref={factsHref}
        factsLabel={copy.sourceFactsForYear}
      />

      <FinancialComparisonTable
        id="balance-sheet"
        title={copy.balanceSheet.title}
        description={copy.balanceSheet.description}
        data={financialDemoData}
        rows={balanceSheetRows}
        locale={locale}
        factsHref={factsHref}
        factsLabel={copy.sourceFactsForYear}
      />

      <Card size="sm">
        <CardHeader>
          <CardTitle>{copy.notes.title}</CardTitle>
          <CardDescription>{copy.notes.description}</CardDescription>
          <CardAction>
            <Badge variant="outline">{copy.notes.taggedFacts}</Badge>
          </CardAction>
        </CardHeader>
        <CardContent className="grid gap-5 md:grid-cols-3">
          <div className="flex flex-col gap-1">
            <FileCheck2 className="text-muted-foreground size-5" />
            <p className="font-medium">{copy.notes.officialFiling}</p>
            <p className="text-muted-foreground text-sm">
              {copy.notes.officialFilingDescription}
            </p>
          </div>
          <div className="flex flex-col gap-1">
            <p className="font-medium">{copy.notes.currencyPair}</p>
            <p className="text-muted-foreground text-sm">
              {copy.notes.currencyPairDescription}
            </p>
          </div>
          <div className="flex flex-col gap-1">
            <p className="font-medium">{copy.notes.formulaAwareRatios}</p>
            <p className="text-muted-foreground text-sm">
              {copy.notes.formulaAwareRatiosDescription}
            </p>
          </div>
        </CardContent>
      </Card>
    </main>
  );
}
