import { formatFieldValue, humanizeFieldKey, splitFields } from "~/components/detail/fields";

export type Lang = "en" | "original";

const CANONICAL_STATUS_LABELS: Record<string, string> = {
  active: "Active",
  inactive: "Inactive",
  unknown: "Unknown",
};

export function formatCompanyStatusLabel(status: string): string {
  return CANONICAL_STATUS_LABELS[status.trim().toLowerCase()] ?? status;
}

export type ResolvedField = {
  /** Base key for a collapsed pair (e.g. "articles_purpose"), original record key otherwise. */
  key: string;
  /** humanizeFieldKey of the display key. */
  label: string;
  value: unknown;
  /** True when the requested lang's variant was empty and we fell back to the other one. */
  fromOtherLang: boolean;
  isLongText: boolean;
};

// Base keys that are *always* long text regardless of how short the current
// value happens to be — these are free-text narrative fields by nature.
const LONG_TEXT_BASE_KEYS = new Set(["articles_purpose", "activity_text"]);
const LONG_TEXT_LENGTH_THRESHOLD = 240;

function isEmptyValue(value: unknown): boolean {
  return value === null || value === undefined || value === "";
}

function isLongTextField(key: string, value: unknown): boolean {
  if (LONG_TEXT_BASE_KEYS.has(key)) return true;
  return typeof value === "string" && value.length > LONG_TEXT_LENGTH_THRESHOLD;
}

/**
 * Splits a record's visible (non-lineage) fields into the pair-collapsed
 * grid fields, the long-text fields, and a count of how many `_en`/`_original`
 * pairs were collapsed (0 hides the language toggle in the UI).
 *
 * Pairing rule: a base key is a pair iff BOTH `<base>_en` and `<base>_original`
 * are present in the record (regardless of whether their values are empty).
 * A key that only has one of the two variants (e.g. BR's `status_en` with no
 * `status_original`, or `share_capital_amount_original` with no `_en`) is
 * never collapsed — it passes through under its own literal key name.
 *
 * Lineage exclusion and the general visible/lineage split are delegated to
 * `splitFields`/`isLineageKey` in ./fields — this module does not re-implement
 * that classification.
 */
export function resolveRecordFields(
  record: Record<string, unknown>,
  lang: Lang,
): { fields: ResolvedField[]; longTexts: ResolvedField[]; pairCount: number } {
  const { visible } = splitFields(record);
  const visibleKeys = new Set(visible.map(([key]) => key));

  const pairBases = new Set<string>();
  for (const [key] of visible) {
    if (key.endsWith("_en")) {
      const base = key.slice(0, -3);
      if (visibleKeys.has(`${base}_original`)) pairBases.add(base);
    }
  }

  const consumed = new Set<string>();
  const fields: ResolvedField[] = [];
  const longTexts: ResolvedField[] = [];

  for (const [key, value] of visible) {
    if (consumed.has(key)) continue;

    const pairBase = pairBaseOf(key, pairBases);
    const resolved =
      pairBase !== null
        ? resolvePair(record, pairBase, lang, consumed)
        : {
            key,
            label: humanizeFieldKey(key),
            value,
            fromOtherLang: false,
            isLongText: isLongTextField(key, value),
          };

    (resolved.isLongText ? longTexts : fields).push(resolved);
  }

  return { fields, longTexts, pairCount: pairBases.size };
}

function pairBaseOf(key: string, pairBases: Set<string>): string | null {
  if (key.endsWith("_en") && pairBases.has(key.slice(0, -3))) return key.slice(0, -3);
  if (key.endsWith("_original") && pairBases.has(key.slice(0, -9))) return key.slice(0, -9);
  return null;
}

function resolvePair(
  record: Record<string, unknown>,
  base: string,
  lang: Lang,
  consumed: Set<string>,
): ResolvedField {
  const enKey = `${base}_en`;
  const originalKey = `${base}_original`;
  consumed.add(enKey);
  consumed.add(originalKey);

  const selectedKey = lang === "en" ? enKey : originalKey;
  const otherKey = lang === "en" ? originalKey : enKey;
  const selectedValue = record[selectedKey];
  const otherValue = record[otherKey];
  const fromOtherLang = isEmptyValue(selectedValue) && !isEmptyValue(otherValue);
  const value = fromOtherLang ? otherValue : selectedValue;

  return {
    key: base,
    label: humanizeFieldKey(base),
    value,
    fromOtherLang,
    isLongText: isLongTextField(base, value),
  };
}

// Candidate key lists for keyFacts, ordered most- to least-specific. Each
// list mixes pair-resolved base keys (e.g. "legal_form") with the raw
// unpaired `_en`/`_original` singles seen in the live-audited landscape
// (FR/CZ's `legal_form_en`, LV's `legal_form_description_en`, BR/FR's
// `status_en`) so a fact is still surfaced when a country only ever
// publishes one language variant.
const LEGAL_FORM_CANDIDATES = [
  "legal_form_description",
  "legal_form",
  "legal_form_description_en",
  "legal_form_description_original",
  "legal_form_en",
  "legal_form_original",
];
const STATUS_CANDIDATES = [
  "status",
  "lifecycle_status",
  "company_status",
  "status_en",
  "status_original",
];
const REGISTERED_DATE_CANDIDATES = [
  "registration_date",
  "registered_date",
  "incorporation_date",
  "founded_date",
];
const WEBSITE_CANDIDATES = ["primary_website_url", "website"];

function firstPresentValue(
  resolvedByKey: Map<string, ResolvedField>,
  candidates: string[],
): { key: string; text: string; fromOtherLang: boolean } | null {
  for (const candidate of candidates) {
    const resolved = resolvedByKey.get(candidate);
    if (!resolved) continue;
    const text = formatFieldValue(resolved.key, resolved.value);
    if (text !== null) return { key: resolved.key, text, fromOtherLang: resolved.fromOtherLang };
  }
  return null;
}

type KeyFact = {
  key: string;
  label: string;
  value: string;
  href?: string;
  /** True when this fact's value came from the other language (fallback). */
  fromOtherLang?: boolean;
};

/**
 * Shared computation behind `keyFacts`/`keyFactKeys`: picks the small set of
 * "at a glance" facts for a company detail card — legal form, status,
 * registered date, and website (as an href) — each carrying the resolved
 * field `key` it was sourced from so callers can dedupe the fact against the
 * general field grid. Each fact has its own candidate key list; the first
 * present (non-empty) candidate wins, and absent facts are skipped rather
 * than rendered empty.
 */
function computeKeyFacts(record: Record<string, unknown>, lang: Lang): KeyFact[] {
  // Long-text fields (e.g. a 240+-char legal_form value) are excluded from
  // key-fact candidacy entirely — a compact "at a glance" strip is the wrong
  // place for prose-length text, so only the short `fields` bucket is
  // eligible. The detail-sections dedup filter is kept anyway as a
  // belt-and-braces guard in case some other path ever surfaces a long-text
  // key here.
  const { fields } = resolveRecordFields(record, lang);
  const resolvedByKey = new Map<string, ResolvedField>();
  for (const field of fields) resolvedByKey.set(field.key, field);

  const facts: KeyFact[] = [];

  const legalForm = firstPresentValue(resolvedByKey, LEGAL_FORM_CANDIDATES);
  if (legalForm) {
    facts.push({
      key: legalForm.key,
      label: "Legal form",
      value: legalForm.text,
      ...(legalForm.fromOtherLang ? { fromOtherLang: true } : {}),
    });
  }

  const status = firstPresentValue(resolvedByKey, STATUS_CANDIDATES);
  if (status) {
    facts.push({
      key: status.key,
      label: "Status",
      value: formatCompanyStatusLabel(status.text),
      ...(status.fromOtherLang ? { fromOtherLang: true } : {}),
    });
  }

  const registeredDate = firstPresentValue(resolvedByKey, REGISTERED_DATE_CANDIDATES);
  if (registeredDate) {
    facts.push({
      key: registeredDate.key,
      label: "Registered",
      value: registeredDate.text,
      ...(registeredDate.fromOtherLang ? { fromOtherLang: true } : {}),
    });
  }

  const website = firstPresentValue(resolvedByKey, WEBSITE_CANDIDATES);
  if (website) {
    facts.push({
      key: website.key,
      label: "Website",
      value: website.text,
      href: website.text,
      ...(website.fromOtherLang ? { fromOtherLang: true } : {}),
    });
  }

  return facts;
}

/**
 * Picks the small set of "at a glance" facts for a company detail card:
 * legal form, status, registered date, and website (as an href). Each fact
 * has its own candidate key list; the first present (non-empty) candidate
 * wins, and absent facts are skipped rather than rendered empty.
 */
export function keyFacts(
  record: Record<string, unknown>,
  lang: Lang,
): { label: string; value: string; href?: string; fromOtherLang?: boolean }[] {
  return computeKeyFacts(record, lang).map(({ key: _key, ...fact }) => fact);
}

/**
 * The resolved field `key`s consumed by `keyFacts` for this record/lang, so
 * the detail-page field grid can exclude them and avoid rendering the same
 * pair twice (once in the key-facts strip, once in the grid).
 */
export function keyFactKeys(record: Record<string, unknown>, lang: Lang): Set<string> {
  return new Set(computeKeyFacts(record, lang).map((fact) => fact.key));
}
