/**
 * The URL-facing filter state of the two `/admin/se/companies*` list pages:
 * the shape the filter sheet edits, the chips that summarise what is applied,
 * and the search string a chip's remove link (or Clear) navigates to.
 *
 * Client-safe: the sheet, the chip row and the route loaders all need this,
 * and `se-company-info-lists.server.ts` imports the sentinels so the select's
 * "Any"/"none" options mean the same thing on both sides of the boundary
 * instead of being re-spelled in each.
 *
 * Every helper here is pure and derives the whole search string from the
 * filter state it is handed -- never from the live location -- so a chip's
 * link is identical on the server render and in a test, and `page` is dropped
 * on purpose (page 7 of the old result set is meaningless once a filter
 * changes) while `pageSize`, `sort` and `dir` survive.
 */
import type { SortDir } from "~/lib/countries";
import { clampPage, clampPageSize, DEFAULT_PAGE_SIZE } from "~/lib/paging";
import {
  SE_INFO_CORRECTION_KINDS,
  SE_INFO_CORRECTION_STATUSES,
} from "~/lib/se-info-corrections";

/** Sentinel `<Select>` value for "no filter". Base UI's Select reserves the
 * empty string for the unselected/placeholder state, so an explicit,
 * selectable "Any" needs a real value. Server-side it is treated as absent. */
export const ANY_FILTER_VALUE = "any";

/** Sentinel for "this column is empty on the row" -- a company with no legal
 * form code, or no status recorded. The empty string cannot travel as a URL
 * value (it is indistinguishable from an absent filter), so it travels as this
 * and is mapped back to '' in SQL. */
export const NONE_FILTER_VALUE = "none";

/**
 * Every source a Swedish company profile can be built from, in the ONE
 * canonical order the whole feature uses: the letters of the Sources column
 * ('BS', 'BSEW'), the legend that explains them, the filter's options and --
 * via `PROFILE_SOURCE_PREDICATES` in the lists server module, which keys off
 * `value` -- the SQL that derives them. Order is pinned here and nowhere
 * else, so a row can never be labelled in one order and sorted in another.
 *
 * This is a GLOBAL alphabet, not the description's: a letter is earned by any
 * of the five datatypes (info, address, financial, people, domains) carrying
 * that register, which is why Bolagsverket is here at all -- it writes
 * addresses, annual accounts and people, but never a company description.
 *
 * `value` is what travels in the URL and what the pipelines' own `sources` /
 * `description_sources` arrays call the register; `letter` is the badge;
 * `label` is what a reader sees in the legend, the chip and the tooltip.
 */
export const PROFILE_SOURCES = [
  // Bolagsverket is the registration authority: it carries the address the
  // register text comes from, the annual accounts and the role evidence.
  { value: "bolagsverket", letter: "B", label: "Bolagsverket" },
  // SCB is the register base: se_company_info publishes nothing without an SCB
  // row (info_rules.py returns None), so every listed company has this one.
  { value: "scb", letter: "S", label: "SCB" },
  { value: "esef", letter: "E", label: "ESEF" },
  { value: "wikidata", letter: "W", label: "Wikidata" },
] as const;

export type ProfileSourceValue = (typeof PROFILE_SOURCES)[number]["value"];

/** The URL whitelist: exactly the catalog's values, nothing else. */
export const PROFILE_SOURCE_VALUES: readonly string[] = PROFILE_SOURCES.map(
  (source) => source.value,
);

/** Both lookups are keyed by plain `string`: they answer questions asked with
 * whatever arrived (a letter from a server-derived string, a value from the
 * URL), and "not a source we know" is a real, handled answer in each. */
type ProfileSource = (typeof PROFILE_SOURCES)[number];

const PROFILE_SOURCE_BY_LETTER = new Map<string, ProfileSource>(
  PROFILE_SOURCES.map((source) => [source.letter, source]),
);

const PROFILE_SOURCE_BY_VALUE = new Map<string, ProfileSource>(
  PROFILE_SOURCES.map((source) => [source.value, source]),
);

/** One line above the table, built from the catalog so it can only ever
 * describe the letters the column actually renders. */
export const PROFILE_SOURCES_LEGEND = `Sources: ${PROFILE_SOURCES.map(
  (source) => `${source.letter} = ${source.label}`,
).join(" · ")}`;

export interface ProfileSourcePart {
  letter: string;
  label: string;
}

/**
 * The derived `profile_sources` string ('BS' | 'SW' | 'BSEW' | ...) as one
 * labelled part per letter, in the string's own (already canonical) order.
 *
 * A letter the catalog does not name -- a source the server started emitting
 * before this bundle knew about it -- is kept and labelled after itself:
 * dropping it would quietly under-report what a company was built from, which
 * is exactly what this column exists to prevent.
 */
export function profileSourceParts(profileSources: string): ProfileSourcePart[] {
  return [...profileSources].map((letter) => ({
    letter,
    label: PROFILE_SOURCE_BY_LETTER.get(letter)?.label ?? letter,
  }));
}

/**
 * The catalog's name for a source VALUE ("wikidata" -> "Wikidata"), falling
 * back to the value itself for one it does not name.
 *
 * ONE function for every place a value is shown to a reader: the filter
 * sheet's `<Select>` options, the applied-filter chip and the detail tabs'
 * Sources strip. Two hand-rolled `find(...)?.label ?? value` expressions is
 * exactly how a renamed source ends up spelled two ways on one page.
 */
export function profileSourceLabel(value: string): string {
  return PROFILE_SOURCE_BY_VALUE.get(value)?.label ?? value;
}

/**
 * The five datatypes a Swedish company profile is assembled from, as the
 * presence columns of `/admin/se/companies`: `key` is the list row's own
 * column (and therefore its `?sort=` value), `label` is the column header.
 *
 * Description is NOT here: it is a column of se_company_info itself, answered
 * by `has_description` without touching another table, and it predates this
 * catalog. These four each cost a set over their datatype's own final table,
 * which is why they are one list -- the server keys its presence SQL by these
 * keys, so a datatype added here without an expression is a type error rather
 * than a column of blanks.
 */
export const PROFILE_DATATYPES = [
  { key: "has_address", label: "Address" },
  { key: "has_financial", label: "Financial" },
  { key: "has_people", label: "People" },
  { key: "has_domains", label: "Domains" },
  { key: "is_publicly_traded", label: "Publicly traded" },
  { key: "has_government_contracts", label: "Gov. contracts" },
  { key: "has_job_ads", label: "Job ads" },
] as const;

export type ProfileDatatypeKey = (typeof PROFILE_DATATYPES)[number]["key"];

const PROFILE_DATATYPE_BY_KEY = new Map<string, (typeof PROFILE_DATATYPES)[number]>(
  PROFILE_DATATYPES.map((datatype) => [datatype.key, datatype]),
);

/**
 * The catalog's name for a datatype KEY ("has_job_ads" -> "Job ads"), falling
 * back to the key itself for one it does not name -- the same contract as
 * `profileSourceLabel`, and for the same reason: the sheet's checkboxes and
 * the applied-filter chips must spell a datatype one way, not two.
 */
export function profileDatatypeLabel(key: string): string {
  return PROFILE_DATATYPE_BY_KEY.get(key)?.label ?? key;
}

/**
 * The `?datatype=` chip's param token: chips remove by param, and seven chips
 * all spelled `datatype` would each remove all of them, so a datatype chip's
 * token names its key too. ONE builder, used by the chip and by the search
 * entries it removes from, so they can never disagree on the spelling.
 */
export function datatypeChipParam(key: ProfileDatatypeKey): string {
  return `datatype:${key}`;
}

/** Task 17 (owner addendum 2026-08-23): the company-info list filters
 * COMPANIES. Its description-PROVENANCE filters (language, suggestion,
 * multi-source, has-corrections) are gone -- that story is the detail page's.
 * `source` is a company-level filter, not one of them: it asks which registers
 * built the profile, never where the published text came from. */
export interface SeCompanyInfoTableFilters {
  companyId: string;
  name: string;
  status: string;
  legalForm: string;
  /** "" | "legal" | "sole" */
  entity: string;
  /** "" | "yes" | "no" -- whether the company has a published description. */
  description: string;
  /** "" | one of PROFILE_SOURCE_VALUES -- companies that HAVE that source. */
  source: string;
  /**
   * PROFILE_DATATYPES keys, deduped and in the catalog's order -- companies
   * that have ALL of them (each travels as a repeated `?datatype=` param, each
   * becomes one ANDed `= 1` predicate). Empty means no datatype filter.
   */
  datatypes: readonly ProfileDatatypeKey[];
}

export const EMPTY_INFO_FILTERS: SeCompanyInfoTableFilters = {
  companyId: "",
  name: "",
  status: "",
  legalForm: "",
  entity: "",
  description: "",
  source: "",
  datatypes: [],
};

export interface SeCompanyInfoCorrectionsTableFilters {
  companyId: string;
  kind: string;
  status: string;
  decidedBy: string;
}

export const EMPTY_CORRECTION_FILTERS: SeCompanyInfoCorrectionsTableFilters = {
  companyId: "",
  kind: "",
  status: "",
  decidedBy: "",
};

/** What a list page carries besides its filters: the sort the headers set and
 * the reviewer's chosen page size, both of which must survive a filter edit. */
export interface TableView {
  sort: string;
  dir: SortDir;
  pageSize: number;
}

export interface FilterChip {
  /** The URL param this chip stands for; removing the chip drops it. */
  param: string;
  label: string;
}

const ENTITY_LABELS: Record<string, string> = {
  legal: "Legal (10-digit)",
  sole: "Sole trader (12-digit)",
};

function chip(param: string, label: string): FilterChip {
  return { param, label };
}

/** One chip per applied filter, in the order the sheet lists them. */
export function infoFilterChips(
  filters: SeCompanyInfoTableFilters,
): FilterChip[] {
  const chips: FilterChip[] = [];
  if (filters.companyId) chips.push(chip("companyId", `Company ${filters.companyId}`));
  if (filters.name) chips.push(chip("name", `Name “${filters.name}”`));
  if (filters.status) chips.push(chip("status", `Status ${chipValue(filters.status)}`));
  if (filters.legalForm) {
    chips.push(chip("legalForm", `Legal form ${chipValue(filters.legalForm)}`));
  }
  if (filters.entity) {
    chips.push(chip("entity", `Entity ${ENTITY_LABELS[filters.entity] ?? filters.entity}`));
  }
  if (filters.description) {
    chips.push(chip("description", `Description ${filters.description}`));
  }
  // One chip per selected datatype (they AND together, and each is removable
  // on its own), in the catalog order the parse already normalized to. The
  // param token carries the key so removing one keeps the rest.
  for (const key of filters.datatypes) {
    chips.push(chip(datatypeChipParam(key), `Has ${profileDatatypeLabel(key)}`));
  }
  if (filters.source) {
    // Named, not spelled: the chip says "Source Wikidata", not "wikidata".
    chips.push(chip("source", `Source ${profileSourceLabel(filters.source)}`));
  }
  return chips;
}

export function correctionFilterChips(
  filters: SeCompanyInfoCorrectionsTableFilters,
): FilterChip[] {
  const chips: FilterChip[] = [];
  if (filters.companyId) chips.push(chip("companyId", `Company ${filters.companyId}`));
  if (filters.kind) chips.push(chip("kind", `Kind ${filters.kind}`));
  if (filters.status) chips.push(chip("status", `Status ${filters.status}`));
  if (filters.decidedBy) {
    chips.push(chip("decidedBy", `Decided by ${chipValue(filters.decidedBy)}`));
  }
  return chips;
}

/** One URL param the search string carries: `param=value`, removable by a chip
 * whose token defaults to the param name. A repeated param (`datatype`) gives
 * each of its entries a DISTINCT token, so one chip's X removes one value. */
type SearchEntry = [param: string, value: string, token?: string];

function searchString(
  entries: SearchEntry[],
  view: TableView,
  omit?: string,
): string {
  const params = new URLSearchParams();
  for (const [key, value, token] of entries) {
    if (value === "" || (token ?? key) === omit) continue;
    // append, not set: single-valued params occur once anyway, and the
    // datatype entries must all travel (`?datatype=a&datatype=b`).
    params.append(key, value);
  }
  // Sorting and page size are not filters: clearing or removing one must not
  // silently throw the reviewer back to the default order or page size.
  params.set("sort", view.sort);
  params.set("dir", view.dir);
  params.set("pageSize", String(view.pageSize));
  return `?${params.toString()}`;
}

/** The URL for these filters, optionally without one of them (a chip's X). */
export function infoListSearch(
  filters: SeCompanyInfoTableFilters,
  view: TableView,
  omit?: string,
): string {
  return searchString(
    [
      ["companyId", filters.companyId],
      ["name", filters.name],
      ["status", filters.status],
      ["legalForm", filters.legalForm],
      ["entity", filters.entity],
      ["description", filters.description],
      ...filters.datatypes.map(
        (key): SearchEntry => ["datatype", key, datatypeChipParam(key)],
      ),
      ["source", filters.source],
    ],
    view,
    omit,
  );
}

export function correctionsListSearch(
  filters: SeCompanyInfoCorrectionsTableFilters,
  view: TableView,
  omit?: string,
): string {
  return searchString(
    [
      ["companyId", filters.companyId],
      ["kind", filters.kind],
      ["status", filters.status],
      ["decidedBy", filters.decidedBy],
    ],
    view,
    omit,
  );
}

/** A select's shown value: "" is the unselected state Base UI reserves, so an
 * unset filter shows the explicit "Any" item instead. */
export function selectValue(value: string): string {
  return value === "" ? ANY_FILTER_VALUE : value;
}

/** A data-driven option's URL value: '' cannot travel, so it travels as "none". */
export function optionValue(value: string): string {
  return value === "" ? NONE_FILTER_VALUE : value;
}

/** The same option shown to a reader: parenthesised, so the absence of a value
 * never reads as a code the register might actually use. */
export function optionLabel(value: string): string {
  const url = optionValue(value);
  return url === value ? value : `(${url})`;
}

/** A filter value in a chip: the "none" sentinel reads as "(none)" there too. */
function chipValue(value: string): string {
  return value === NONE_FILTER_VALUE ? `(${NONE_FILTER_VALUE})` : value;
}

/* -------------------------------------------------------------------- */
/* Parsing the URL (both list routes)                                    */
/* -------------------------------------------------------------------- */

/** What a list page carries besides its filters, parsed and clamped. `sort`
 * and `dir` stay raw here: only the query builder's own whitelist decides
 * whether they name a real column, and it owns the fallback. */
export interface ParsedListView {
  page: number;
  pageSize: number;
  sort: string | undefined;
  dir: string | undefined;
}

export function parseListView(url: URL): ParsedListView {
  return {
    page: clampPage(Number.parseInt(url.searchParams.get("page") || "1", 10)),
    pageSize: clampPageSize(
      Number.parseInt(
        url.searchParams.get("pageSize") || String(DEFAULT_PAGE_SIZE),
        10,
      ),
    ),
    sort: url.searchParams.get("sort") ?? undefined,
    dir: url.searchParams.get("dir") ?? undefined,
  };
}

/**
 * One filter value as APPLIED, which is what the chips and the Filters count
 * describe. Three things collapse to "no filter": an absent/blank param, the
 * select's "Any" sentinel, and -- when `allowed` is given -- a value the query
 * builder's own whitelist would drop. Without that last check a hand-typed
 * `?description=bogus` would show a chip and a count of 1 over a completely
 * unfiltered table.
 *
 * Data-driven columns (status, legal form, decided_by) pass no `allowed`:
 * their values come from the column itself, they reach SQL only as named
 * params, and a value that matches nothing simply returns no rows -- which the
 * chip then correctly describes.
 */
function filterValue(
  url: URL,
  name: string,
  allowed?: readonly string[],
): string {
  const value = (url.searchParams.get(name) ?? "").trim();
  if (value === "" || value === ANY_FILTER_VALUE) return "";
  if (allowed !== undefined && !allowed.includes(value)) return "";
  return value;
}

const ENTITY_VALUES = ["legal", "sole"] as const;

/** The two yes/no filter values, exported so the sheet's `<Select>` options and
 * the URL whitelist are one list rather than two that can drift. */
export const YES_NO_VALUES = ["yes", "no"] as const;

/**
 * The `?datatype=` params as APPLIED: repeated params (what the sheet's
 * checkboxes submit), with a comma-joined single param tolerated (what a
 * hand-edited URL tends to spell). Filtering the CATALOG by what arrived --
 * rather than the arrivals by the catalog -- does the whole contract in one
 * pass: unknown values silently drop (the module's convention: applied
 * filters only), duplicates collapse, and the result is always in the
 * catalog's order, so the chips and the SQL are deterministic no matter how
 * the URL ordered them.
 */
function datatypeValues(url: URL): ProfileDatatypeKey[] {
  const requested = new Set(
    url.searchParams
      .getAll("datatype")
      .flatMap((value) => value.split(","))
      .map((value) => value.trim()),
  );
  return PROFILE_DATATYPES.filter((datatype) => requested.has(datatype.key)).map(
    (datatype) => datatype.key,
  );
}

export function parseInfoFilters(url: URL): SeCompanyInfoTableFilters {
  return {
    companyId: filterValue(url, "companyId"),
    name: filterValue(url, "name"),
    status: filterValue(url, "status"),
    legalForm: filterValue(url, "legalForm"),
    entity: filterValue(url, "entity", ENTITY_VALUES),
    description: filterValue(url, "description", YES_NO_VALUES),
    // Whitelisted against the catalog: a stale `?source=llm` from the removed
    // description-provenance filter names no source and is simply dropped.
    source: filterValue(url, "source", PROFILE_SOURCE_VALUES),
    datatypes: datatypeValues(url),
  };
}

/**
 * A correction ledger's four filters, with the kind/status enums as arguments:
 * the address ledger (`se_company_address_correction`) passes its own, and the
 * defaults are the shared vocabulary in se-info-corrections.ts that the filter
 * sheet's fields default to as well. The info corrections page that first
 * relied on the defaults is retired; they stay so the two keep agreeing.
 */
export function parseCorrectionFilters(
  url: URL,
  enums: {
    kinds?: readonly string[];
    statuses?: readonly string[];
  } = {},
): SeCompanyInfoCorrectionsTableFilters {
  return {
    companyId: filterValue(url, "companyId"),
    kind: filterValue(url, "kind", enums.kinds ?? SE_INFO_CORRECTION_KINDS),
    status: filterValue(url, "status", enums.statuses ?? SE_INFO_CORRECTION_STATUSES),
    decidedBy: filterValue(url, "decidedBy"),
  };
}
