import { esefDisclosureText } from "~/lib/esef-disclosures";
import type { EsefDisclosureDocument } from "~/lib/esef-disclosures";

/** Source-neutral fact shape rendered by the shared XBRL fact reader.
 * Country pipelines adapt their native rows to this boundary; source tables
 * keep their complete native metadata and evidence identities. */
export interface XbrlFact {
  factId: string;
  conceptQname: string;
  conceptLocalName: string;
  valueKind: string;
  rawValue: string;
  amountOriginal: number | null;
  amountUsd: number | null;
  fxRateDate: string;
  fxSource: string;
  decimals: number | null;
  periodStart: string;
  periodInstant: string;
  periodDurationEnd: string;
  unit: string;
  currency: string;
  dimensions: string;
  language: string;
  conceptLabels?: XbrlConceptText[];
  conceptDocumentation?: XbrlConceptText[];
  conceptTaxonomy?: {
    entrypoint: string;
    sourceUrl: string;
  };
  structuredDisclosure?: EsefDisclosureDocument | null;
  disclosureEvidence?: {
    sourceRecordUid: string;
    textSha256: string;
    parserName: string;
    parserVersion: string;
  } | null;
}

export type XbrlConceptTextSource =
  | "taxonomy"
  | "translation"
  | "identifier";

export interface XbrlConceptText {
  language: string;
  label: string;
  isReportLanguage: boolean;
  source?: XbrlConceptTextSource;
  translationProvider?: string;
  translationModel?: string;
  translationVersion?: number;
}

export interface XbrlFactConceptLabels {
  submitted: string;
  submittedLanguage: string;
  english: string;
}

/** Turn an XML/XBRL local name into a readable label without changing the
 * exact QName retained as source evidence. */
export function xbrlConceptLabel(
  conceptLocalName: string,
  conceptQname = "",
): string {
  const sourceName =
    conceptLocalName || conceptQname.split(":").pop() || conceptQname;
  if (!sourceName) return "—";

  return sourceName
    .replace(/[_-]+/g, " ")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Za-z])([0-9])/g, "$1 $2")
    .replace(/([0-9])([A-Za-z])/g, "$1 $2")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .map((word) =>
      /^[A-Z0-9]+$/.test(word)
        ? word
        : `${word.charAt(0).toUpperCase()}${word.slice(1)}`,
    )
    .join(" ");
}

/** Prefer the submitted taxonomy label and expose English independently.
 * The identifier formatter is only the final fallback. */
export function xbrlFactConceptLabels(
  fact: Pick<
    XbrlFact,
    "conceptLabels" | "conceptLocalName" | "conceptQname" | "language"
  >,
): XbrlFactConceptLabels {
  const labels = fact.conceptLabels ?? [];
  const english =
    labels.find((entry) => entry.language === "en") ??
    labels.find((entry) => entry.language.startsWith("en-"));
  const submitted =
    labels.find(
      (entry) => entry.isReportLanguage && !entry.language.startsWith("en"),
    ) ??
    labels.find((entry) => entry.isReportLanguage) ??
    labels.find((entry) => entry.language === fact.language) ??
    labels.find(
      (entry) =>
        fact.language !== "" &&
        entry.language.split("-", 1)[0] === fact.language.split("-", 1)[0],
    ) ??
    english;
  return {
    submitted:
      submitted?.label ??
      english?.label ??
      xbrlConceptLabel(fact.conceptLocalName, fact.conceptQname),
    submittedLanguage: submitted?.language ?? "",
    english: english?.label ?? "",
  };
}

export function xbrlFactPeriod(fact: XbrlFact): string {
  if (fact.periodInstant) return fact.periodInstant;
  if (fact.periodStart && fact.periodDurationEnd) {
    return `${fact.periodStart} – ${fact.periodDurationEnd}`;
  }
  return fact.periodDurationEnd || fact.periodStart || "—";
}

export function xbrlDecimalsLabel(decimals: number | null): string {
  if (decimals === null) return "";
  const increment = 10 ** -decimals;
  const formattedIncrement = new Intl.NumberFormat("en-US", {
    maximumFractionDigits: Math.max(0, decimals),
  }).format(increment);
  return `Reported precision: nearest ${formattedIncrement} (XBRL decimals ${decimals})`;
}

export function xbrlDimensionSummary(dimensions: string): string {
  if (!dimensions || dimensions === "{}") return "";
  try {
    const parsed = JSON.parse(dimensions) as Record<string, string>;
    return Object.entries(parsed)
      .map(([axis, member]) => {
        const axisName = axis.split(":").pop() ?? axis;
        const memberName = member.split(":").pop() ?? member;
        return `${axisName}: ${memberName}`;
      })
      .join(", ");
  } catch {
    return dimensions;
  }
}

/** OIM text facts may retain safe-to-store XHTML fragments from an inline
 * report. This normalization is presentation-only; rawValue remains intact. */
export function xbrlTextValue(rawValue: string): string {
  return esefDisclosureText(rawValue);
}
