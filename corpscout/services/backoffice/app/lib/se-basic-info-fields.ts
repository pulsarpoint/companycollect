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

/** Spec section 11's source names; the reviewer first because it outranks all. */
export const BASIC_INFO_SOURCES = [
  "reviewer",
  "llm",
  "scb",
  "bolagsverket",
  "esef",
  "wikidata",
  "ratsit",
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
