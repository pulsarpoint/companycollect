import { parseDomainNames } from "~/lib/se-domain-crawl.server";
import { parseWorkspaceDomainFilters, type WorkspaceDomainSelection } from "~/lib/workspace-domains";

export function parseWorkspaceDomainSelection(value: unknown): WorkspaceDomainSelection {
  if (!value || typeof value !== "object" || !("mode" in value)) throw new Error("Select domains to submit.");
  if (value.mode === "ids" && "domains" in value) {
    if (Object.keys(value).some(key => !["mode", "domains"].includes(key))) throw new Error("Invalid domain selection fields.");
    const domains = parseDomainNames(value.domains);
    if (!domains.length) throw new Error("Select at least one domain.");
    return { mode: "ids", domains };
  }
  if (value.mode !== "query" || !("query" in value) || !("excludedDomains" in value) || Object.keys(value).some(key => !["mode", "query", "excludedDomains"].includes(key))) throw new Error("Invalid domain selection.");
  const query = value.query;
  const fields = Object.keys(parseWorkspaceDomainFilters(new URLSearchParams()));
  if (!query || typeof query !== "object" || Array.isArray(query) || Object.keys(query).length !== fields.length || Object.keys(query).some(key => !fields.includes(key))) throw new Error("Submit the complete applied domain filters.");
  const params = new URLSearchParams();
  for (const [key, entry] of Object.entries(query)) {
    if (key === "sources") {
      if (!Array.isArray(entry) || entry.some(source => typeof source !== "string")) throw new Error("Invalid domain sources.");
      for (const source of entry) params.append("source", source);
    } else {
      if (typeof entry !== "string") throw new Error(`Invalid domain filter: ${key}.`);
      params.set(key, entry);
    }
  }
  const parsed = parseWorkspaceDomainFilters(params);
  // Never silently drop a bulk filter: that could select the entire inventory.
  if (Object.entries(parsed).some(([key, entry]) => JSON.stringify(entry) !== JSON.stringify(Object.getOwnPropertyDescriptor(query, key)?.value))) throw new Error("Invalid domain filter value.");
  return { mode: "query", query: parsed, excludedDomains: parseDomainNames(value.excludedDomains) };
}

export function workspaceDomainInputConfig(selection: WorkspaceDomainSelection): Record<string, unknown> {
  return {
    source_relation: "corpscout.domains_search", source_final: false,
    id_column: "root_domain", website_column: "root_domain",
    ...(selection.mode === "ids" ? { ids: selection.domains } : {
      select_all: true, excluded_ids: selection.excludedDomains,
      workspace_domain_filters: {
        prefix: selection.query.prefix, suffix: selection.query.suffix,
        sources: selection.query.sources, source_match: selection.query.sourceMatch,
        dns: selection.query.dns, websites: selection.query.websites,
        companies: selection.query.companies, company_matching: selection.query.companyMatching,
      },
    }),
  };
}
