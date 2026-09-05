/**
 * The basic-info entity as the Info tab reads it: the eight decidable fields (description_language rides with description) in
 * display order and the seven sources in the order the suggestions panel lists
 * them when the precedence table does not rank a source for a field.
 *
 * Client-safe on purpose (no `.server` import): the workspace component, the
 * form parser and the route all render from these.
 */

export const BASIC_INFO_FIELDS = [
  { name: "legal_name", label: "Legal name", kind: "text" },
  { name: "legal_form_code", label: "Legal form", kind: "code" },
  { name: "status", label: "Status", kind: "text" },
  { name: "incorporation_date", label: "Incorporated", kind: "date" },
  { name: "lei", label: "LEI", kind: "identifier" },
  { name: "wikidata_id", label: "Wikidata", kind: "identifier" },
  { name: "description", label: "Description", kind: "paragraph" },
  { name: "description_sv", label: "Description (Swedish)", kind: "paragraph" },
] as const;

export type SeBasicInfoField = (typeof BASIC_INFO_FIELDS)[number]["name"];
export type SeBasicInfoFieldKind = (typeof BASIC_INFO_FIELDS)[number]["kind"];

const FIELD_BY_NAME = new Map<string, (typeof BASIC_INFO_FIELDS)[number]>(
  BASIC_INFO_FIELDS.map((field) => [field.name, field]),
);

export function isBasicInfoField(value: string): value is SeBasicInfoField {
  return FIELD_BY_NAME.has(value);
}

export function basicInfoFieldLabel(field: SeBasicInfoField): string {
  return FIELD_BY_NAME.get(field)?.label ?? field;
}

export function basicInfoFieldKind(field: SeBasicInfoField): SeBasicInfoFieldKind {
  return FIELD_BY_NAME.get(field)?.kind ?? "text";
}

/**
 * Spec section 11's source names; the reviewer first because it outranks all,
 * the reviewer's own draft last (slice 3c) since a draft is never active and
 * the suggestions panel never offers "Use this" on it.
 */
export const BASIC_INFO_SOURCES = [
  "reviewer",
  "llm",
  "scb",
  "bolagsverket",
  "esef",
  "wikidata",
  "ratsit",
  "reviewer_draft",
] as const;

export type SeBasicInfoSource = (typeof BASIC_INFO_SOURCES)[number];

const SOURCE_LABELS: Record<SeBasicInfoSource, string> = {
  reviewer: "Reviewer",
  llm: "Model",
  scb: "SCB",
  bolagsverket: "Bolagsverket",
  esef: "ESEF",
  wikidata: "Wikidata",
  ratsit: "Ratsit",
  reviewer_draft: "Draft",
};

export function isBasicInfoSource(value: string): value is SeBasicInfoSource {
  return (BASIC_INFO_SOURCES as readonly string[]).includes(value);
}

/** What a reader calls a source token; an unknown token reads as itself. */
export function basicInfoSourceLabel(source: string): string {
  return isBasicInfoSource(source) ? SOURCE_LABELS[source] : source;
}

export const DEFAULT_BASIC_INFO_FIELD: SeBasicInfoField = "legal_name";

/** The suggestions panel's field is the URL (`?field=status`), so a link can
 * open the page on one field; anything else falls back to the legal name. */
export function selectedFieldFromSearch(search: URLSearchParams): SeBasicInfoField {
  const value = search.get("field") ?? "";
  return isBasicInfoField(value) ? value : DEFAULT_BASIC_INFO_FIELD;
}

/**
 * Whether the next fold would change this company: a suggestion row newer than
 * the fold, or suggestions for a company that has never been folded. Both
 * stamps are ClickHouse `YYYY-MM-DD HH:MM:SS.mmm` strings (UTC), so string
 * order is time order.
 */
export function foldPending(
  foldedAt: string | null,
  suggestedAts: readonly string[],
): boolean {
  if (suggestedAts.length === 0) return false;
  if (foldedAt === null) return true;
  return suggestedAts.some((suggestedAt) => suggestedAt > foldedAt);
}

/** The `status` field's only two legal values. */
export const BASIC_INFO_STATUSES = ["active", "inactive"] as const;

/** The `description` field's only two legal languages. */
export const BASIC_INFO_LANGUAGES = ["en", "sv"] as const;

export const MAX_LEGAL_NAME_LENGTH = 500;
export const MAX_DESCRIPTION_LENGTH = 8000;

// Storage floor, not a historical judgment: `Nullable(Date32)` saturates any
// date before 1900-01-01 to 1900-01-01 silently, so a value older than that
// could never round-trip -- the validator's floor has to match the column's.
const INCORPORATION_DATE_MIN = "1900-01-01";
const LEI_PATTERN = /^[A-Z0-9]{20}$/;
const LEGAL_FORM_CODE_PATTERN = /^[0-9]{2}$/;
const INCORPORATION_DATE_PATTERN = /^([0-9]{4})-([0-9]{2})-([0-9]{2})$/;
const WIKIDATA_ID_PATTERN = /^Q[0-9]+$/;

export interface SeBasicInfoValueOptions {
  /** The SCB legal-form codes the loader offered; an empty list refuses every code. */
  legalFormCodes: readonly string[];
  /** Today's date, `YYYY-MM-DD`, UTC; the upper bound of `incorporation_date`. */
  today: string;
}

export type SeBasicInfoValueResult =
  | { ok: true; value: string; language: string }
  | { ok: false; error: string };

function ok(value: string, language = ""): SeBasicInfoValueResult {
  return { ok: true, value, language };
}

function fail(error: string): SeBasicInfoValueResult {
  return { ok: false, error };
}

function validateText(trimmed: string, maxLength: number): SeBasicInfoValueResult {
  if (trimmed.length > maxLength) return fail(`Value is longer than ${maxLength} characters.`);
  return ok(trimmed);
}

function validateLegalFormCode(trimmed: string, legalFormCodes: readonly string[]): SeBasicInfoValueResult {
  if (!LEGAL_FORM_CODE_PATTERN.test(trimmed) || !legalFormCodes.includes(trimmed)) {
    return fail("Legal form must be one of the SCB codes.");
  }
  return ok(trimmed);
}

function validateStatus(trimmed: string): SeBasicInfoValueResult {
  if (trimmed !== "active" && trimmed !== "inactive") {
    return fail("Status must be active or inactive.");
  }
  return ok(trimmed);
}

function isRealCalendarDate(year: number, month: number, day: number): boolean {
  const date = new Date(Date.UTC(year, month - 1, day));
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  );
}

function validateIncorporationDate(trimmed: string, today: string): SeBasicInfoValueResult {
  const match = INCORPORATION_DATE_PATTERN.exec(trimmed);
  if (!match) return fail("Date must be YYYY-MM-DD.");
  const [, yearText, monthText, dayText] = match;
  if (!isRealCalendarDate(Number(yearText), Number(monthText), Number(dayText))) {
    return fail("Date must be YYYY-MM-DD.");
  }
  if (trimmed < INCORPORATION_DATE_MIN || trimmed > today) {
    return fail("Date must be between 1900-01-01 and today.");
  }
  return ok(trimmed);
}

function validateLei(trimmed: string): SeBasicInfoValueResult {
  const upper = trimmed.toUpperCase();
  if (!LEI_PATTERN.test(upper)) return fail("LEI must be 20 letters or digits.");
  return ok(upper);
}

function validateWikidataId(trimmed: string): SeBasicInfoValueResult {
  if (!WIKIDATA_ID_PATTERN.test(trimmed)) return fail("Wikidata id must be Q followed by digits.");
  return ok(trimmed);
}

function validateDescription(trimmed: string, language: string): SeBasicInfoValueResult {
  if (trimmed.length > MAX_DESCRIPTION_LENGTH) return fail(`Value is longer than ${MAX_DESCRIPTION_LENGTH} characters.`);
  if (language !== "en" && language !== "sv") return fail("Language must be en or sv.");
  return ok(trimmed, language);
}

/**
 * Validates one field's edited value, dispatching by field NAME rather than
 * kind: `status` and `legal_name` share kind `text` but validate differently.
 * Check order is always trim -> empty -> the field's own rule. `language` is
 * only meaningful (and only echoed back) for `description`; every other field
 * returns `''` regardless of what was sent.
 */
export function validateSeBasicInfoValue(
  field: SeBasicInfoField,
  value: string,
  language: string,
  options: SeBasicInfoValueOptions,
): SeBasicInfoValueResult {
  const trimmed = value.trim();
  if (trimmed.length === 0) return fail("Value cannot be empty.");

  switch (field) {
    case "legal_name":
      return validateText(trimmed, MAX_LEGAL_NAME_LENGTH);
    case "legal_form_code":
      return validateLegalFormCode(trimmed, options.legalFormCodes);
    case "status":
      return validateStatus(trimmed);
    case "incorporation_date":
      return validateIncorporationDate(trimmed, options.today);
    case "lei":
      return validateLei(trimmed);
    case "wikidata_id":
      return validateWikidataId(trimmed);
    case "description":
      return validateDescription(trimmed, language);
    case "description_sv":
      return validateText(trimmed, MAX_DESCRIPTION_LENGTH);
  }
}
