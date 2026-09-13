/**
 * The address entity's catalogue and the client-safe validation of a typed
 * address (spec 2026-09-06 sections 3.1, 8). No `.server` import: the route's
 * module must not drag ClickHouse into the client bundle.
 */

export const ADDRESS_SOURCES = ["scb", "bolagsverket", "ratsit", "esef", "reviewer", "reviewer_draft"] as const;
export const ADDRESS_KINDS = ["postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown"] as const;
export const REVIEWER_KINDS = ["postal", "visiting", "visiting_or_postal", "registered"] as const;
export type SeAddressSource = (typeof ADDRESS_SOURCES)[number];
export type SeAddressKind = (typeof ADDRESS_KINDS)[number];
export const MAX_NOTE_LENGTH = 500;
const MAX_STREET = 200;
const MAX_CARE_OF = 200;
const MAX_CITY = 100;
const KEY_PATTERN = /^[0-9a-f]{64}$/;
const CONTROL = /[\u0000-\u001f\u007f]/;
/** The note is a 3-row Textarea, so what the reviewer typed may hold line
 * breaks (and a tab): this class refuses every other C0 control and DEL but
 * lets tab, line feed and carriage return through -- a browser submits a
 * textarea's newlines as CRLF, so refusing CR would refuse every multi-line
 * note. Every other field keeps the stricter `CONTROL`. */
const NOTE_CONTROL = /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/;

export function isAddressSource(value: string): value is SeAddressSource {
  return (ADDRESS_SOURCES as readonly string[]).includes(value);
}
export function isAddressKind(value: string): value is SeAddressKind {
  return (ADDRESS_KINDS as readonly string[]).includes(value);
}
export function isAddressKey(value: string): boolean {
  return KEY_PATTERN.test(value);
}

const SOURCE_LABELS: Record<SeAddressSource, string> = {
  scb: "SCB", bolagsverket: "Bolagsverket", ratsit: "Ratsit", esef: "ESEF", reviewer: "Reviewer", reviewer_draft: "Reviewer draft",
};
export function addressSourceLabel(source: string): string {
  return isAddressSource(source) ? SOURCE_LABELS[source] : source;
}
const KIND_LABELS: Record<SeAddressKind, string> = {
  postal: "Postal", visiting: "Visiting", visiting_or_postal: "Visiting or postal", registered: "Registered",
  workplace: "Workplace", unknown: "Unknown",
};
export function addressKindLabel(kind: string): string {
  return isAddressKind(kind) ? KIND_LABELS[kind] : kind;
}
const GEOCODE_LABELS: Record<string, string> = {
  matched_exact: "Exact", matched_street: "Street", matched_corrected: "Corrected", matched_site: "Site",
  matched_area: "Area (centroid)", unmatched: "Unmatched", ambiguous: "Ambiguous", postal_box: "Box",
  property_identifier: "Property", invalid_address: "Invalid", foreign: "Foreign", "": "Not geocoded",
};
export function geocodeStatusLabel(status: string): string {
  return GEOCODE_LABELS[status] ?? status;
}

export function selectedAddressFromSearch(params: URLSearchParams): string | null {
  const key = params.get("address") ?? "";
  return isAddressKey(key) ? key : null;
}

/** The Workplaces card's page size (spec 8, amended 2026-09-13): fifty rows a
 * page, counted and cut in ClickHouse, never in the browser. */
export const WORKPLACE_PAGE_SIZE = 50;
/** The filter box is a contains search, not a query language: a hundred
 * characters is longer than any `normalized_address` and caps what a
 * hand-typed URL can push into the query parameter. */
export const MAX_WORKPLACE_QUERY_LENGTH = 100;

/** `?workplaces=<page>`, 1-based. Anything that is not a whole number of at
 * least 1 -- absent, empty, `0`, `-2`, `2.5`, `abc` -- is page 1. */
export function workplacePageFromSearch(params: URLSearchParams): number {
  const raw = (params.get("workplaces") ?? "").trim();
  if (!/^[0-9]+$/.test(raw)) return 1;
  const page = Number(raw);
  return Number.isSafeInteger(page) && page >= 1 ? page : 1;
}

/** `?workplace_q=<text>`: trimmed and capped. The loader hands it to ClickHouse
 * as a query parameter, so it is never SQL. */
export function workplaceQueryFromSearch(params: URLSearchParams): string {
  return (params.get("workplace_q") ?? "").trim().slice(0, MAX_WORKPLACE_QUERY_LENGTH);
}

/** The three parameters the Address tab keeps in its URL. */
export interface SeAddressSearchState {
  /** The `?address=` key, or null for no selection. */
  address: string | null;
  /** 1-based; page 1 is the absent parameter. */
  workplacePage: number;
  /** The workplace filter; `''` is the absent parameter. */
  workplaceQuery: string;
}

/**
 * The tab's whole query string, leading `?` included (`''` when everything is
 * at its default). Every link on the page builds its search here -- selecting
 * an address keeps the workplace page and its filter, paging keeps the selected
 * address -- so there is one place where the parameter names live.
 */
export function addressSearchString(state: SeAddressSearchState): string {
  const params = new URLSearchParams();
  if (state.address !== null && state.address !== "") params.set("address", state.address);
  if (state.workplacePage > 1) params.set("workplaces", String(state.workplacePage));
  if (state.workplaceQuery !== "") params.set("workplace_q", state.workplaceQuery);
  const search = params.toString();
  return search === "" ? "" : `?${search}`;
}

export interface SeAddressInput {
  careOf: string;
  streetLine: string;
  postalCode: string;
  city: string;
  country: string;
  kind: SeAddressKind;
  note: string;
}
export type SeAddressValidation = { ok: true; input: SeAddressInput } | { ok: false; error: string };

function plain(
  label: string,
  value: string,
  max: number,
  control: RegExp = CONTROL,
): string | { error: string } {
  const trimmed = value.trim();
  if (control.test(trimmed)) return { error: `${label} must be plain text.` };
  if (trimmed.length > max) return { error: `${label} is longer than ${max} characters.` };
  return trimmed;
}

/** Spec 8's validation: a box or a street line, a five-digit postcode, a city,
 * capped lengths, plain text, a catalogue kind, Sweden only. */
export function validateSeAddressInput(raw: Record<string, string>): SeAddressValidation {
  const streetLine = plain("Street line", raw.streetLine ?? "", MAX_STREET);
  if (typeof streetLine !== "string") return { ok: false, error: streetLine.error };
  if (streetLine === "") return { ok: false, error: "A street line or a box is required." };
  const careOf = plain("Care-of", raw.careOf ?? "", MAX_CARE_OF);
  if (typeof careOf !== "string") return { ok: false, error: careOf.error };
  const postalCode = (raw.postalCode ?? "").replace(/\s+/g, "");
  if (!/^[0-9]{5}$/.test(postalCode)) return { ok: false, error: "Postcode must be five digits." };
  const city = plain("City", raw.city ?? "", MAX_CITY);
  if (typeof city !== "string") return { ok: false, error: city.error };
  if (city === "") return { ok: false, error: "City is required." };
  const country = (raw.country ?? "SE").trim().toUpperCase() || "SE";
  if (country !== "SE") return { ok: false, error: "Only Swedish addresses can be typed here." };
  const kind = raw.kind ?? "";
  if (!isAddressKind(kind)) return { ok: false, error: "Unknown address kind." };
  const note = plain("Note", raw.note ?? "", MAX_NOTE_LENGTH, NOTE_CONTROL);
  if (typeof note !== "string") return { ok: false, error: note.error };
  return { ok: true, input: { careOf, streetLine, postalCode, city, country, kind, note } };
}

/** Dagster's selection (batch._changed_company_ids) in miniature: a non-draft
 * normalized version or a rule version newer than the newest fold, or normalized
 * rows with no fold at all. Drafts are never among `stamps`. */
export function addressFoldPending(foldedAt: string | null, stamps: readonly string[], hasNormalized: boolean): boolean {
  if (foldedAt === null) return hasNormalized;
  return stamps.some((stamp) => stamp > foldedAt);
}
