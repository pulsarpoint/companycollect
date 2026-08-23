/**
 * `description_source` values the se_company_info pipeline writes. `"none"`
 * is the UI's name for the empty-string source (a row whose description has
 * no source yet -- see migration 000297's comment on the column). Client-safe
 * so there is exactly one definition, shared by se-company-info-lists.server
 * .ts's filter validation and the company-info table's filter `<Select>`
 * option list, instead of a copy on each side of the client/server boundary.
 */
export const INFO_LIST_SOURCES = [
  "scb",
  "wikidata",
  "esef",
  "llm",
  "reviewed",
  "none",
] as const;

export type InfoListSource = (typeof INFO_LIST_SOURCES)[number];
