/**
 * The URL-facing filter state of the two `/admin/se/company-info*` list pages:
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

/** Sentinel `<Select>` value for "no filter". Base UI's Select reserves the
 * empty string for the unselected/placeholder state, so an explicit,
 * selectable "Any" needs a real value. Server-side it is treated as absent. */
export const ANY_FILTER_VALUE = "any";

/** Sentinel for "this column is empty on the row" -- a company with no
 * description source, no legal form code or no description language. The
 * empty string cannot travel as a URL value (it is indistinguishable from an
 * absent filter), so it travels as this and is mapped back to '' in SQL. */
export const NONE_FILTER_VALUE = "none";

export interface SeCompanyInfoTableFilters {
  companyId: string;
  name: string;
  source: string;
  status: string;
  legalForm: string;
  language: string;
  /** "" | "yes" | "no" -- whether the company has a model suggestion. */
  suggestion: string;
  /** "" | "legal" | "sole" */
  entity: string;
  multi: boolean;
  corrected: boolean;
}

export const EMPTY_INFO_FILTERS: SeCompanyInfoTableFilters = {
  companyId: "",
  name: "",
  source: "",
  status: "",
  legalForm: "",
  language: "",
  suggestion: "",
  entity: "",
  multi: false,
  corrected: false,
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
  if (filters.source) chips.push(chip("source", `Source ${filters.source}`));
  if (filters.status) chips.push(chip("status", `Status ${filters.status}`));
  if (filters.legalForm) chips.push(chip("legalForm", `Legal form ${filters.legalForm}`));
  if (filters.language) chips.push(chip("language", `Language ${filters.language}`));
  if (filters.suggestion) chips.push(chip("suggestion", `Suggestion ${filters.suggestion}`));
  if (filters.entity) {
    chips.push(chip("entity", `Entity ${ENTITY_LABELS[filters.entity] ?? filters.entity}`));
  }
  if (filters.multi) chips.push(chip("multi", "Multi-source"));
  if (filters.corrected) chips.push(chip("corrected", "Has corrections"));
  return chips;
}

export function correctionFilterChips(
  filters: SeCompanyInfoCorrectionsTableFilters,
): FilterChip[] {
  const chips: FilterChip[] = [];
  if (filters.companyId) chips.push(chip("companyId", `Company ${filters.companyId}`));
  if (filters.kind) chips.push(chip("kind", `Kind ${filters.kind}`));
  if (filters.status) chips.push(chip("status", `Status ${filters.status}`));
  if (filters.decidedBy) chips.push(chip("decidedBy", `Decided by ${filters.decidedBy}`));
  return chips;
}

function searchString(
  entries: Array<[string, string]>,
  view: TableView,
  omit?: string,
): string {
  const params = new URLSearchParams();
  for (const [key, value] of entries) {
    if (value === "" || key === omit) continue;
    params.set(key, value);
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
      ["source", filters.source],
      ["status", filters.status],
      ["legalForm", filters.legalForm],
      ["language", filters.language],
      ["suggestion", filters.suggestion],
      ["entity", filters.entity],
      ["multi", filters.multi ? "1" : ""],
      ["corrected", filters.corrected ? "1" : ""],
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

/** A data-driven option's label: '' is a real value (no code recorded). */
export function optionLabel(value: string): string {
  return value === "" ? NONE_FILTER_VALUE : value;
}

/** A data-driven option's URL value: '' cannot travel, so it travels as "none". */
export function optionValue(value: string): string {
  return value === "" ? NONE_FILTER_VALUE : value;
}
