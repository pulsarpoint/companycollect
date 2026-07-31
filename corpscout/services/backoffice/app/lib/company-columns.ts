/**
 * Which columns a country's company list shows, held in the URL.
 *
 * Every register describes a company slightly differently, but they agree on
 * more than they disagree: all ten declare an id, a name, a legal form, a
 * status and a registration date, and all ten can produce an industry. Those,
 * plus the town, are the CORE — shown everywhere, in one order, so a reader
 * moving between countries reads the same table rather than relearning it.
 *
 * Everything else is the country's own. Brazil has a trade name and a size
 * band; Norway and Finland have a website. Those are offered but start hidden,
 * because a column that only one country fills should not set the shape of the
 * default view for the other nine.
 *
 * Availability is derived from the country's config rather than a second list
 * that could drift from it — including the two columns the table injects
 * rather than selects, industry and place, which a picker reading
 * `country.columns` alone would silently omit.
 *
 * Client-safe: no `.server` imports, so the picker can use it directly.
 */

import type { CountryConfig } from "~/lib/countries";
import { availableCompanyFlags } from "~/lib/company-flags";

export type CompanyColumn = {
  id: string;
  label: string;
  /** Part of the uniform set: shown by default, in the order below. */
  core?: boolean;
  /** Cannot be hidden. */
  locked?: boolean;
};

/**
 * The uniform core, in the order every country renders it.
 *
 * The array IS the canonical order — a selection is sorted back into it, so a
 * table never reorders itself because of the sequence a reader ticked boxes
 * in, and Brazil (which declares legal_form last) reads like everywhere else.
 */
const CORE_ORDER = [
  "id",
  "name",
  "industry",
  "legal_form",
  "status",
  "registered",
  "place",
  "data",
];

// The only route to a company's own page. Hiding it would leave a reader with
// rows they can see and cannot open.
const LOCKED = ["name"];

/** Fallbacks for the two columns the table injects rather than selects. */
const INJECTED_LABELS: Record<string, string> = {
  industry: "Industry",
  place: "Place",
  data: "Data",
};

/**
 * Every column this country can actually show, in canonical order.
 *
 * A column is offered only when the country has something behind it: a
 * declared column, or — for industry and place — the query that fills it.
 */
export function availableCompanyColumns(country: CountryConfig): CompanyColumn[] {
  const declared = new Map(country.columns.map((c) => [c.key, c.label]));
  const has = (id: string): boolean => {
    if (id === "industry") return Boolean(country.industryQuery) || declared.has(id);
    if (id === "place") return Boolean(country.placeQuery) || declared.has(id);
    // Offered only where at least one kind of data can actually be held, so a
    // country with no sources does not get a column of permanently dark
    // glyphs describing our coverage rather than the company.
    if (id === "data") return availableCompanyFlags(country.code).length > 0;
    return declared.has(id);
  };

  const core = CORE_ORDER.filter(has).map((id) => ({
    id,
    label: declared.get(id) ?? INJECTED_LABELS[id] ?? id,
    core: true,
    ...(LOCKED.includes(id) ? { locked: true } : {}),
  }));

  // The country's own columns, in the order it declares them.
  const extra = country.columns
    .filter((c) => !CORE_ORDER.includes(c.key))
    .map((c) => ({ id: c.key, label: c.label }));

  return [...core, ...extra];
}

/** Canonical order, deduplicated, restricted to what the country can show. */
function canonical(ids: Iterable<string>, available: CompanyColumn[]): string[] {
  const wanted = new Set(ids);
  return available.filter((c) => wanted.has(c.id) || c.locked).map((c) => c.id);
}

/**
 * The core, and only the core.
 *
 * Unlike the contracts table — which offers everything the register fills —
 * this default is the same list in every country by construction. That
 * uniformity is the reason the core exists, so a country's extras wait to be
 * asked for.
 */
export function defaultCompanyColumns(available: CompanyColumn[]): string[] {
  return available.filter((c) => c.core).map((c) => c.id);
}

export function parseCompanyColumns(
  searchParams: URLSearchParams,
  available: CompanyColumn[],
): string[] {
  const raw = searchParams.get("cols");
  // Absent means "not customised" and takes the default. Present-but-empty
  // means the reader unticked everything, which is a choice worth keeping —
  // it still leaves the locked name column.
  if (raw === null) return defaultCompanyColumns(available);

  const offered = new Set(available.map((c) => c.id));
  const chosen = raw
    .split(",")
    .map((v) => v.trim())
    // A URL shared from Brazil naming trade_name must not add a permanently
    // empty column to Norway.
    .filter((v) => offered.has(v));

  return canonical(chosen, available);
}

/** The `cols` value, or null when the selection is just the default. */
export function serializeCompanyColumns(
  visible: string[],
  available: CompanyColumn[],
): string | null {
  const chosen = canonical(visible, available);
  const fallback = defaultCompanyColumns(available);
  if (chosen.length === fallback.length && chosen.every((id, i) => id === fallback[i])) {
    return null;
  }
  return chosen.join(",");
}
