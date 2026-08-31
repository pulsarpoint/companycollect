import {
  Banknote,
  CalendarDays,
  FileText,
  Hash,
  Languages,
  Ruler,
} from "lucide-react";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
import { Badge } from "~/components/ui/badge";
import { EsefDisclosureReader } from "~/components/detail/esef-disclosure-reader";
import { parseEsefDisclosure } from "~/lib/esef-disclosures";
import {
  xbrlConceptLabel,
  xbrlDecimalsLabel,
  xbrlDimensionSummary,
  xbrlFactConceptLabels,
  xbrlFactPeriod,
  xbrlTextValue,
  type XbrlConceptText,
  type XbrlFact,
} from "~/lib/xbrl-facts";

const compactNumber = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 2,
});

const exactNumber = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 20,
});

const usdAmount = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});

function valueKindLabel(valueKind: string): string {
  if (valueKind === "text") return "Narrative";
  if (valueKind === "monetary") return "Monetary";
  return xbrlConceptLabel(valueKind);
}

function sourceUnitLabel(fact: XbrlFact): string {
  if (fact.currency) return fact.currency;
  const localUnit = fact.unit.split(":").pop() || fact.unit;
  return localUnit ? xbrlConceptLabel(localUnit) : "No unit";
}

function numericValue(fact: XbrlFact): number | null {
  if (fact.amountOriginal !== null) return fact.amountOriginal;
  const parsed = Number(fact.rawValue);
  return Number.isFinite(parsed) ? parsed : null;
}

function sourceValuePreview(fact: XbrlFact): string {
  if (fact.valueKind === "text") {
    const value =
      fact.structuredDisclosure?.plainText ||
      xbrlTextValue(fact.rawValue) ||
      "Empty text fact";
    return value.length > 220 ? `${value.slice(0, 220).trimEnd()}…` : value;
  }
  const value = numericValue(fact);
  if (value === null) return fact.rawValue || "—";
  return `${compactNumber.format(value)} ${sourceUnitLabel(fact)}`;
}

function dimensionEntries(dimensions: string): Array<[string, string]> {
  if (!dimensions || dimensions === "{}") return [];
  try {
    return Object.entries(JSON.parse(dimensions) as Record<string, string>);
  } catch {
    return [];
  }
}

function FactIcon({ valueKind }: { valueKind: string }) {
  const iconClassName = "size-4";
  if (valueKind === "text") return <FileText className={iconClassName} />;
  if (valueKind === "monetary") {
    return <Banknote className={iconClassName} />;
  }
  return <Hash className={iconClassName} />;
}

function DetailField({
  label,
  children,
  mono = false,
}: {
  label: string;
  children: React.ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-1 border-l-2 border-border pl-3">
      <dt className="text-muted-foreground text-xs font-medium">{label}</dt>
      <dd
        className={
          mono
            ? "break-all font-mono text-xs leading-5"
            : "break-words text-sm leading-5"
        }
      >
        {children}
      </dd>
    </div>
  );
}

function englishConceptText(
  entries: XbrlConceptText[] | undefined,
): XbrlConceptText | undefined {
  return entries?.find(
    (entry) => entry.language === "en" || entry.language.startsWith("en-"),
  );
}

function ConceptTextProvenance({
  entry,
}: {
  entry: XbrlConceptText | undefined;
}) {
  if (!entry?.source) return null;

  const sourceLabel = {
    taxonomy: "Official taxonomy",
    translation: "Machine translation",
    identifier: "Identifier fallback",
  }[entry.source];
  const translator = [entry.translationProvider, entry.translationModel]
    .filter(Boolean)
    .join(" · ");

  return (
    <span className="mt-2 flex flex-wrap gap-1.5">
      <Badge variant={entry.source === "translation" ? "secondary" : "outline"}>
        {sourceLabel}
      </Badge>
      {translator ? <Badge variant="outline">{translator}</Badge> : null}
      {entry.translationVersion ? (
        <Badge variant="outline">Version {entry.translationVersion}</Badge>
      ) : null}
    </span>
  );
}

function NarrativeFactValue({ fact }: { fact: XbrlFact }) {
  const disclosure =
    fact.structuredDisclosure ?? parseEsefDisclosure(fact.rawValue);
  return (
    <section
      className="flex min-w-0 flex-col gap-3"
      aria-label="Disclosure text"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h4 className="font-medium">Disclosure text</h4>
        <Badge variant="outline">
          {disclosure.plainText.length.toLocaleString("en-US")} characters
        </Badge>
        {fact.disclosureEvidence ? (
          <Badge variant="secondary">
            Structured extraction v{fact.disclosureEvidence.parserVersion}
          </Badge>
        ) : null}
      </div>
      <div className="max-h-[38rem] min-w-0 overflow-y-auto rounded-lg border bg-background p-4 shadow-xs sm:p-5">
        <EsefDisclosureReader disclosure={disclosure} />
      </div>
    </section>
  );
}

function NumericFactValue({ fact }: { fact: XbrlFact }) {
  const value = numericValue(fact);
  const precision = xbrlDecimalsLabel(fact.decimals)
    .replace(/^Reported precision: /, "")
    .replace(/ \(XBRL decimals -?\d+\)$/, "");
  return (
    <section className="flex flex-col gap-3" aria-label="Reported value">
      <h4 className="font-medium">Reported value</h4>
      <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <div className="rounded-lg border bg-background p-4 shadow-xs">
          <dt className="text-muted-foreground text-xs font-medium">
            Source amount
          </dt>
          <dd className="mt-2 break-words text-xl font-semibold tabular-nums">
            {value === null ? fact.rawValue || "—" : exactNumber.format(value)}
          </dd>
          <p className="text-muted-foreground mt-1 text-xs">
            {sourceUnitLabel(fact)}
          </p>
        </div>
        {fact.amountUsd !== null && fact.currency !== "USD" ? (
          <div className="rounded-lg border bg-background p-4 shadow-xs">
            <dt className="text-muted-foreground text-xs font-medium">
              USD equivalent
            </dt>
            <dd className="mt-2 text-xl font-semibold tabular-nums">
              {usdAmount.format(fact.amountUsd)}
            </dd>
            <p className="text-muted-foreground mt-1 text-xs">
              {fact.fxRateDate ? `Rate date ${fact.fxRateDate}` : "Converted"}
            </p>
          </div>
        ) : null}
        <div className="rounded-lg border bg-background p-4 shadow-xs">
          <dt className="text-muted-foreground text-xs font-medium">Unit</dt>
          <dd className="mt-2 break-words text-base font-medium">
            {sourceUnitLabel(fact)}
          </dd>
          <p className="text-muted-foreground mt-1 break-all font-mono text-xs">
            {fact.unit || "No XBRL unit"}
          </p>
        </div>
        <div className="rounded-lg border bg-background p-4 shadow-xs">
          <dt className="text-muted-foreground text-xs font-medium">
            Reported precision
          </dt>
          <dd className="mt-2 text-sm font-medium">
            {precision
              ? `${precision.charAt(0).toUpperCase()}${precision.slice(1)}`
              : "Not specified"}
          </dd>
          {fact.decimals !== null ? (
            <p className="text-muted-foreground mt-1 text-xs">
              XBRL decimals {fact.decimals}
            </p>
          ) : null}
        </div>
      </dl>
      <p className="text-muted-foreground break-all font-mono text-xs">
        Raw XBRL value: {fact.rawValue || "—"}
      </p>
    </section>
  );
}

export function XbrlFactDetails({ fact }: { fact: XbrlFact }) {
  const dimensions = dimensionEntries(fact.dimensions);
  const dimensionFallback = xbrlDimensionSummary(fact.dimensions);
  const conceptLabels = xbrlFactConceptLabels(fact);
  const conceptDocumentation = fact.conceptDocumentation?.length
    ? xbrlFactConceptLabels({
        conceptLabels: fact.conceptDocumentation,
        conceptLocalName: "",
        conceptQname: "",
        language: fact.language,
      })
    : null;
  const englishConceptLabel = englishConceptText(fact.conceptLabels);
  const englishConceptDocumentation = englishConceptText(
    fact.conceptDocumentation,
  );
  return (
    <div className="flex flex-col gap-6 border-t bg-muted/20 px-4 py-5 sm:px-5 lg:px-6">
      {fact.valueKind === "text" ? (
        <NarrativeFactValue fact={fact} />
      ) : (
        <NumericFactValue fact={fact} />
      )}

      {conceptDocumentation ? (
        <section
          className="flex flex-col gap-2 rounded-lg border bg-background p-4 shadow-xs"
          aria-label="Taxonomy concept description"
        >
          <h4 className="font-medium">Taxonomy description</h4>
          <p className="text-sm leading-6">
            {conceptDocumentation.english || conceptDocumentation.submitted}
          </p>
          <ConceptTextProvenance entry={englishConceptDocumentation} />
          {conceptDocumentation.english &&
          conceptDocumentation.english !== conceptDocumentation.submitted ? (
            <div className="text-muted-foreground border-t pt-2 text-sm leading-6">
              <p>
                <span className="font-medium">
                  Original
                  {conceptDocumentation.submittedLanguage
                    ? ` (${conceptDocumentation.submittedLanguage})`
                    : ""}
                  :{" "}
                </span>
                {conceptDocumentation.submitted}
              </p>
            </div>
          ) : null}
        </section>
      ) : null}

      <section className="flex flex-col gap-3" aria-label="Fact metadata">
        <h4 className="font-medium">Fact metadata</h4>
        <dl className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <DetailField label="Reporting period">
            {xbrlFactPeriod(fact)}
          </DetailField>
          <DetailField label="Value type">
            {valueKindLabel(fact.valueKind)}
          </DetailField>
          <DetailField label="Language">
            {fact.language || "Not specified"}
          </DetailField>
          <DetailField label="Fact ID" mono>
            {fact.factId}
          </DetailField>
          <DetailField label="Concept label">
            <span>{conceptLabels.english || conceptLabels.submitted}</span>
            {conceptLabels.english ? (
              <ConceptTextProvenance entry={englishConceptLabel} />
            ) : null}
          </DetailField>
          {conceptLabels.english !== "" &&
          conceptLabels.english !== conceptLabels.submitted ? (
            <DetailField
              label={
                conceptLabels.submittedLanguage
                  ? `Original concept label (${conceptLabels.submittedLanguage})`
                  : "Original concept label"
              }
            >
              {conceptLabels.submitted}
            </DetailField>
          ) : null}
          <div className="sm:col-span-2">
            <DetailField label="Exact XBRL concept" mono>
              {fact.conceptQname}
            </DetailField>
          </div>
          {fact.conceptTaxonomy?.entrypoint ? (
            <div className="sm:col-span-2">
              <DetailField label="Taxonomy entrypoint" mono>
                {fact.conceptTaxonomy.entrypoint}
              </DetailField>
            </div>
          ) : null}
          {fact.conceptTaxonomy?.sourceUrl ? (
            <div className="sm:col-span-2">
              <DetailField label="Taxonomy concept source">
                <a
                  className="break-all text-primary underline-offset-4 hover:underline"
                  href={fact.conceptTaxonomy.sourceUrl}
                  rel="noreferrer"
                  target="_blank"
                >
                  {fact.conceptTaxonomy.sourceUrl}
                </a>
              </DetailField>
            </div>
          ) : null}
          {fact.fxSource ? (
            <div className="sm:col-span-2">
              <DetailField label="Currency conversion source">
                {fact.fxSource}
                {fact.fxRateDate ? ` · ${fact.fxRateDate}` : ""}
              </DetailField>
            </div>
          ) : null}
          {fact.disclosureEvidence ? (
            <>
              <DetailField label="Disclosure parser" mono>
                {fact.disclosureEvidence.parserName} v
                {fact.disclosureEvidence.parserVersion}
              </DetailField>
              <div className="sm:col-span-2">
                <DetailField label="Source record UID" mono>
                  {fact.disclosureEvidence.sourceRecordUid}
                </DetailField>
              </div>
              <div className="sm:col-span-2">
                <DetailField label="Text SHA-256" mono>
                  {fact.disclosureEvidence.textSha256}
                </DetailField>
              </div>
            </>
          ) : null}
        </dl>
      </section>

      {dimensions.length > 0 ? (
        <section className="flex flex-col gap-3" aria-label="Dimensions">
          <div className="flex items-center gap-2">
            <Ruler className="text-muted-foreground size-4" />
            <h4 className="font-medium">Dimensions</h4>
            <Badge variant="secondary">{dimensions.length}</Badge>
          </div>
          <dl className="grid gap-3 md:grid-cols-2">
            {dimensions.map(([axis, member]) => (
              <div
                key={`${axis}:${member}`}
                className="rounded-lg border bg-background p-4 shadow-xs"
              >
                <dt className="text-muted-foreground text-xs font-medium">
                  {xbrlConceptLabel("", axis)}
                </dt>
                <dd className="mt-1 font-medium">
                  {xbrlConceptLabel("", member)}
                </dd>
                <p className="text-muted-foreground mt-2 break-all font-mono text-[11px] leading-4">
                  {axis} → {member}
                </p>
              </div>
            ))}
          </dl>
        </section>
      ) : dimensionFallback ? (
        <DetailField label="Dimensions">{dimensionFallback}</DetailField>
      ) : null}
    </div>
  );
}

function XbrlFactSummary({ fact }: { fact: XbrlFact }) {
  const dimensionCount = dimensionEntries(fact.dimensions).length;
  const preview = sourceValuePreview(fact);
  const conceptLabels = xbrlFactConceptLabels(fact);
  const distinctEnglishLabel =
    conceptLabels.english !== "" &&
    conceptLabels.english !== conceptLabels.submitted;
  const primaryLabel = conceptLabels.english || conceptLabels.submitted;
  return (
    <div className="flex min-w-0 flex-1 items-start gap-3 pr-2">
      <div className="mt-0.5 flex size-9 shrink-0 items-center justify-center rounded-lg border bg-muted/50 text-muted-foreground">
        <FactIcon valueKind={fact.valueKind} />
      </div>
      <div className="grid min-w-0 flex-1 gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(12rem,0.5fr)_minmax(14rem,0.7fr)] lg:items-center">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-semibold leading-5">
              {primaryLabel}
            </span>
            <Badge variant="outline">{valueKindLabel(fact.valueKind)}</Badge>
          </div>
          {distinctEnglishLabel ? (
            <p className="text-muted-foreground mt-1 text-xs font-normal">
              Original
              {conceptLabels.submittedLanguage
                ? ` (${conceptLabels.submittedLanguage})`
                : ""}
              : {conceptLabels.submitted}
            </p>
          ) : null}
          <p className="text-muted-foreground mt-1 truncate font-mono text-[11px] font-normal">
            {fact.conceptQname}
          </p>
        </div>
        <div className="flex min-w-0 flex-col gap-1 text-xs font-normal">
          <span className="flex items-center gap-1.5 tabular-nums">
            <CalendarDays className="text-muted-foreground size-3.5 shrink-0" />
            {xbrlFactPeriod(fact)}
          </span>
          <span className="text-muted-foreground flex flex-wrap items-center gap-1.5">
            {fact.language ? (
              <span className="flex items-center gap-1">
                <Languages className="size-3.5" />
                {fact.language}
              </span>
            ) : null}
            {dimensionCount > 0 ? (
              <span>
                {dimensionCount} dimension{dimensionCount === 1 ? "" : "s"}
              </span>
            ) : null}
          </span>
        </div>
        <div className="min-w-0 font-normal">
          <p
            className={
              fact.valueKind === "text"
                ? "text-muted-foreground line-clamp-2 text-xs leading-5"
                : "truncate text-sm font-medium tabular-nums"
            }
          >
            {preview}
          </p>
          {fact.amountUsd !== null && fact.currency !== "USD" ? (
            <p className="text-muted-foreground mt-1 text-xs tabular-nums">
              {usdAmount.format(fact.amountUsd)} USD equivalent
            </p>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export function XbrlFactsAccordion({
  facts,
  ariaLabel = "XBRL report facts",
}: {
  facts: XbrlFact[];
  ariaLabel?: string;
}) {
  return (
    <Accordion
      multiple
      className="overflow-hidden rounded-xl border bg-background"
      aria-label={ariaLabel}
    >
      {facts.map((fact) => (
        <AccordionItem key={fact.factId} value={fact.factId}>
          <AccordionTrigger className="rounded-none px-4 py-4 hover:bg-muted/40 hover:no-underline aria-expanded:bg-muted/40 sm:px-5">
            <XbrlFactSummary fact={fact} />
          </AccordionTrigger>
          <AccordionContent className="pb-0">
            <XbrlFactDetails fact={fact} />
          </AccordionContent>
        </AccordionItem>
      ))}
    </Accordion>
  );
}

// Compatibility exports while ESEF-specific callers migrate to the shared
// source-neutral component name.
export const EsefFactsAccordion = XbrlFactsAccordion;
export const EsefFactDetails = XbrlFactDetails;
