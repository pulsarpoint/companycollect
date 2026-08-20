import { useState } from "react";
import { Link } from "react-router";
import { ArrowLeft, ExternalLink, FileSearch, FileText } from "lucide-react";
import type { Route } from "./+types/company-facts";
import { XbrlFactsAccordion } from "~/components/detail/xbrl-facts-accordion";
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
import { chQuery } from "~/lib/clickhouse.server";
import { getCountry } from "~/lib/countries";
import type {
  XbrlConceptTextSource,
  XbrlFact,
} from "~/lib/xbrl-facts";
import {
  getCompanyFacts,
  getFactsDocument,
  type FactRow,
} from "~/lib/queries.server";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country?.detail?.factsQuery) {
    throw new Response("Not found", { status: 404 });
  }
  const year = Number(params.year);
  if (!Number.isInteger(year) || year < 1900 || year > 2200) {
    throw new Response("Not found", { status: 404 });
  }
  const [names, facts, doc] = await Promise.all([
    chQuery<{ name: string }>(
      `SELECT ${country.nameColumn} AS name FROM ${country.companiesTable}
       WHERE ${country.idColumn} = {id:String} LIMIT 1`,
      { id: params.id },
    ),
    getCompanyFacts(country, params.id, year),
    getFactsDocument(country, params.id, year),
  ]);
  if (names.length === 0) {
    throw new Response("Company not found", { status: 404 });
  }
  return {
    name: names[0].name,
    facts,
    year,
    doc: doc
      ? {
          hasObject:
            doc.object_key !== "" && doc.source_uri.startsWith("s3://"),
          archiveUrl: doc.archive_url,
          archiveName: doc.archive_name,
          nestedZipName: doc.nested_zip_name,
        }
      : null,
  };
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const name = loaderData?.name ?? params.id;
  return [
    { title: `${name} – ${params.year} facts – CompanyCollect Backoffice` },
  ];
}

const FACT_BATCH_SIZE = 200;

/** period0/balans0 is the filing's own year; periodN/balansN is N years back. */
function contextLabel(contextId: string, year: number): string {
  const match = /^(period|balans)(\d+)$/.exec(contextId.toLowerCase());
  if (!match) return contextId;
  const offset = Number(match[2]);
  const point = match[1] === "balans" ? "balance" : "period";
  return offset === 0 ? `${year} ${point}` : `${year - offset} ${point}`;
}

function hasDimensions(dimensions: string): boolean {
  return dimensions !== "" && dimensions !== "{}";
}

function reportPeriodEnd(nestedZipName: string, year: number): string {
  return (
    /_(\d{4}-\d{2}-\d{2})(?:\.zip)?$/.exec(nestedZipName)?.[1] ?? `${year}`
  );
}

function readerValueKind(fact: FactRow): string {
  if (fact.value_kind === "numeric" && fact.currency) return "monetary";
  return fact.value_kind;
}

function readerDecimals(fact: FactRow): number | null {
  if (!fact.decimals || fact.decimals.toUpperCase() === "INF") return null;
  const decimals = Number(fact.decimals);
  return Number.isInteger(decimals) ? decimals : null;
}

function conceptTextSource(
  value: string | null | undefined,
): XbrlConceptTextSource | undefined {
  if (
    value === "taxonomy" ||
    value === "translation" ||
    value === "identifier"
  ) {
    return value;
  }
  return undefined;
}

function readerFact(
  fact: FactRow,
  factIndex: number,
  year: number,
  countryLanguage: string,
): XbrlFact {
  const conceptLabelEnglish = fact.concept_label_en?.trim() ?? "";
  const conceptLabelOriginal = fact.concept_label_original?.trim() ?? "";
  const conceptLabelOriginalLanguage =
    fact.concept_label_original_language?.trim() || countryLanguage;
  const conceptDescriptionEnglish = fact.concept_description_en?.trim() ?? "";
  const conceptDescriptionOriginal =
    fact.concept_description_original?.trim() ?? "";
  return {
    factId: `${factIndex}:${fact.context_id}:${fact.concept}`,
    conceptQname: fact.concept,
    conceptLocalName:
      fact.concept_local_name ??
      fact.concept.split(":").pop() ??
      fact.concept,
    valueKind: readerValueKind(fact),
    rawValue: fact.raw_value,
    amountOriginal: fact.amount_original,
    amountUsd: fact.amount_usd,
    fxRateDate: fact.fx_rate_date ?? "",
    fxSource: fact.fx_source ?? "",
    decimals: readerDecimals(fact),
    periodStart: fact.period_start ?? "",
    periodInstant: fact.period_instant ?? "",
    periodDurationEnd:
      fact.period_duration_end ?? contextLabel(fact.context_id, year),
    unit: fact.unit_id ?? fact.currency ?? "",
    currency: fact.currency ?? "",
    dimensions: fact.dimensions,
    language: fact.language ?? countryLanguage,
    conceptLabels: [
      ...(conceptLabelOriginal
        ? [
            {
              language: conceptLabelOriginalLanguage,
              label: conceptLabelOriginal,
              isReportLanguage: true,
              source: "taxonomy" as const,
            },
          ]
        : []),
      ...(conceptLabelEnglish
        ? [
            {
              language: "en",
              label: conceptLabelEnglish,
              isReportLanguage: false,
              source: conceptTextSource(fact.concept_label_en_source),
              translationProvider:
                fact.concept_label_translation_provider ?? undefined,
              translationModel:
                fact.concept_label_translation_model ?? undefined,
              translationVersion:
                fact.concept_label_translation_version ?? undefined,
            },
          ]
        : []),
    ],
    conceptDocumentation: [
      ...(conceptDescriptionOriginal
        ? [
            {
              language: conceptLabelOriginalLanguage,
              label: conceptDescriptionOriginal,
              isReportLanguage: true,
              source: "taxonomy" as const,
            },
          ]
        : []),
      ...(conceptDescriptionEnglish
        ? [
            {
              language: "en",
              label: conceptDescriptionEnglish,
              isReportLanguage: false,
              source: conceptTextSource(fact.concept_description_en_source),
              translationProvider:
                fact.concept_description_translation_provider ?? undefined,
              translationModel:
                fact.concept_description_translation_model ?? undefined,
              translationVersion:
                fact.concept_description_translation_version ?? undefined,
            },
          ]
        : []),
    ],
    conceptTaxonomy:
      fact.concept_taxonomy_entrypoint || fact.concept_source_url
        ? {
            entrypoint: fact.concept_taxonomy_entrypoint ?? "",
            sourceUrl: fact.concept_source_url ?? "",
          }
        : undefined,
    structuredDisclosure: null,
    disclosureEvidence: null,
  };
}

function matchesFilter(fact: XbrlFact, needle: string): boolean {
  if (!needle) return true;
  return [
    fact.conceptQname,
    fact.conceptLocalName,
    fact.conceptLabels?.map((label) => label.label).join(" ") ?? "",
    fact.conceptDocumentation?.map((entry) => entry.label).join(" ") ?? "",
    fact.rawValue,
    fact.currency,
    fact.periodDurationEnd,
    fact.dimensions,
  ].some((value) => value.toLowerCase().includes(needle));
}

export default function CompanyFacts({
  loaderData,
  params,
}: Route.ComponentProps) {
  const { name, facts, year, doc } = loaderData;
  const country = getCountry(params.country)!;
  const hasFinancialSources = Boolean(country.detail?.financialSources?.length);
  const backHref = hasFinancialSources
    ? `/company/${params.country}/${params.id}/financials`
    : `/company/${params.country}/${params.id}`;
  const factsSource = country.detail?.financialSources?.find(
    (source) => source.kind === "registry" && source.yearFacts,
  );
  const reportTitle = factsSource?.title ?? "Annual account";
  const [filter, setFilter] = useState("");
  const [visibleLimit, setVisibleLimit] = useState(FACT_BATCH_SIZE);
  const needle = filter.trim().toLowerCase();
  const readerFacts = facts.map((fact, factIndex) =>
    readerFact(fact, factIndex, year, country.code),
  );
  const matchingFacts = readerFacts.filter((fact) =>
    matchesFilter(fact, needle),
  );
  const visibleFacts = matchingFacts.slice(0, visibleLimit);
  const monetaryCount = readerFacts.filter(
    (fact) => fact.valueKind === "monetary",
  ).length;
  const currentPeriodCount = facts.filter((fact) =>
    fact.is_comparative == null
      ? /^(period|balans)0$/i.test(fact.context_id)
      : Number(fact.is_comparative) === 0,
  ).length;
  const dimensionedFactCount = facts.filter((fact) =>
    hasDimensions(fact.dimensions),
  ).length;
  const currencies = [
    ...new Set(facts.map((fact) => fact.currency).filter(Boolean)),
  ];
  const reportCurrency =
    currencies.length === 1
      ? currencies[0]
      : currencies.length > 1
        ? "Mixed"
        : "Unavailable";
  const periodEnd =
    facts.find((fact) => fact.report_period_end)?.report_period_end ??
    reportPeriodEnd(doc?.nestedZipName ?? "", year);

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
          {hasFinancialSources ? "All financial sources" : name}
        </Button>
      </div>

      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex min-w-0 flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="text-xl font-semibold tracking-tight">
              {reportTitle} · {year}
            </h2>
            <Badge variant="outline">Standalone annual account</Badge>
          </div>
          <p className="text-muted-foreground text-sm">
            {name} · period ending {periodEnd} ·{" "}
            {facts.length.toLocaleString("en-US")} tagged facts
          </p>
          {doc?.nestedZipName ? (
            <p className="text-muted-foreground break-all font-mono text-xs">
              {doc.nestedZipName}
              {doc.archiveName ? ` inside ${doc.archiveName}` : ""}
            </p>
          ) : null}
        </div>
        <div className="flex flex-wrap gap-2">
          {doc?.archiveUrl ? (
            <Button
              variant="outline"
              nativeButton={false}
              render={
                <a href={doc.archiveUrl} target="_blank" rel="noreferrer" />
              }
            >
              Source archive
              <ExternalLink data-icon="inline-end" />
            </Button>
          ) : null}
          {doc?.hasObject ? (
            <Button
              nativeButton={false}
              render={
                <a
                  href={`/company/${params.country}/${params.id}/facts/${year}/document`}
                  target="_blank"
                  rel="noreferrer"
                />
              }
            >
              <FileText data-icon="inline-start" />
              Open report
              <ExternalLink data-icon="inline-end" />
            </Button>
          ) : null}
        </div>
      </div>

      <dl className="grid grid-cols-2 rounded-xl bg-muted/35 px-4 ring-1 ring-foreground/10 lg:grid-cols-5">
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:pr-4">
          <dt className="text-muted-foreground text-xs">Tagged facts</dt>
          <dd className="mt-1 font-medium tabular-nums">
            {facts.length.toLocaleString("en-US")}
          </dd>
        </div>
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:px-4">
          <dt className="text-muted-foreground text-xs">
            Current-period facts
          </dt>
          <dd className="mt-1 font-medium tabular-nums">
            {currentPeriodCount.toLocaleString("en-US")}
          </dd>
        </div>
        <div className="border-b py-3 lg:border-r lg:border-b-0 lg:px-4">
          <dt className="text-muted-foreground text-xs">Monetary facts</dt>
          <dd className="mt-1 font-medium tabular-nums">
            {monetaryCount.toLocaleString("en-US")}
          </dd>
        </div>
        <div className="py-3 lg:border-r lg:px-4">
          <dt className="text-muted-foreground text-xs">Report currency</dt>
          <dd className="mt-1 font-medium">{reportCurrency}</dd>
        </div>
        <div className="py-3 lg:pl-4">
          <dt className="text-muted-foreground text-xs">Dimensions</dt>
          <dd className="mt-1 font-medium tabular-nums">
            {dimensionedFactCount.toLocaleString("en-US")} facts
          </dd>
        </div>
      </dl>

      <Card className="min-w-0">
        <CardHeader className="border-b">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div>
              <CardTitle>Tagged source facts</CardTitle>
              <CardDescription>
                Exact source XBRL values before standardized metric mapping.
              </CardDescription>
            </div>
            <Input
              value={filter}
              onChange={(event) => {
                setFilter(event.target.value);
                setVisibleLimit(FACT_BATCH_SIZE);
              }}
              placeholder="Search concepts, values, currency, or dimensions…"
              aria-label={`Search ${reportTitle} facts`}
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
                <EmptyTitle>
                  {facts.length === 0 ? "No facts loaded" : "No matching facts"}
                </EmptyTitle>
                <EmptyDescription>
                  {facts.length === 0
                    ? "Tagged facts will appear here after this filing is loaded from the source archive."
                    : "Try a concept label, source value, currency, context, or dimension member."}
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <>
              <XbrlFactsAccordion
                facts={visibleFacts}
                ariaLabel={`${reportTitle} facts`}
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
