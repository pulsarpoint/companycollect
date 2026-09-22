import { randomUUID } from "node:crypto";
import { dagsterRunUrl, launchRun } from "~/lib/dagster.server";
import { EMPTY_SE_DOMAINS_FILTERS, parseSeDomainsFilters, type SeDomainsFilters } from "~/lib/se-domains-filters";
import { DOMAIN_CRAWL_TYPES, type SeDomainSelection } from "~/lib/se-domain-selection";

function domains(value: unknown): string[] {
  if (!Array.isArray(value) || value.some((domain) => typeof domain !== "string" || domain.length > 253 || !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/.test(domain))) {
    throw new Error("Selection must contain valid domain names.");
  }
  return [...new Set<string>(value)];
}

export function parseSeDomainSelection(value: unknown): SeDomainSelection {
  if (typeof value !== "object" || value === null || !("mode" in value)) throw new Error("Select domains to submit.");
  if (value.mode === "ids" && "domains" in value) {
    if (Object.keys(value).some((key) => !["mode", "domains"].includes(key))) throw new Error("Invalid domain selection fields.");
    const selected = domains(value.domains);
    if (selected.length === 0) throw new Error("Select at least one domain.");
    return { mode: "ids", domains: selected };
  }
  if (value.mode !== "query" || !("query" in value) || !("excludedDomains" in value) || Object.keys(value).some((key) => !["mode", "query", "excludedDomains"].includes(key))) throw new Error("Invalid domain selection.");
  const query = value.query;
  const fields = Object.keys(EMPTY_SE_DOMAINS_FILTERS) as (keyof SeDomainsFilters)[];
  if (typeof query !== "object" || query === null || Array.isArray(query) || Object.keys(query).length !== fields.length || Object.keys(query).some((key) => !fields.includes(key as keyof SeDomainsFilters))) throw new Error("Submit the complete applied domain filters.");
  const params = new URLSearchParams();
  for (const field of fields) {
    const entry = Object.getOwnPropertyDescriptor(query, field)?.value;
    if (typeof entry !== "string") throw new Error(`Invalid domain filter: ${field}.`);
    params.set(field, entry);
  }
  const parsed = parseSeDomainsFilters(params);
  // Browsing can drop an invalid parameter. A bulk action must never broaden it.
  if (fields.some((key) => parsed[key] !== params.get(key))) throw new Error("Invalid domain filter value.");
  if (parsed.minConfidence !== "" && parsed.maxConfidence !== "" && Number(parsed.minConfidence) > Number(parsed.maxConfidence)) throw new Error("Minimum confidence cannot exceed maximum confidence.");
  return { mode: "query", query: parsed, excludedDomains: domains(value.excludedDomains) };
}

export async function saveSeDomainCrawlInputs(value: unknown, crawlType: unknown) {
  if (!DOMAIN_CRAWL_TYPES.some((type) => type.value === crawlType)) throw new Error("Choose full crawl, jobs, or basic info.");
  const selection = parseSeDomainSelection(value);
  const prefix = crawlType === "site_info" ? "website_site_info" : `website_${crawlType}_crawl`;
  const asset = `${prefix}_requests`;
  const config: Record<string, unknown> = {
    source_relation: "corpscout.se_company_domain", source_final: true,
    id_column: "root_domain", website_column: "root_domain",
  };
  if (selection.mode === "ids") {
    config.ids = selection.domains;
  } else {
    const q = selection.query;
    config.select_all = true;
    config.excluded_ids = selection.excludedDomains;
    config.se_domain_filters = {
      domain: q.domain, company: q.company, source: q.source, association: q.association,
      status: q.status, shared: q.shared === "1",
      ...(q.minConfidence === "" ? {} : {min_confidence: Number(q.minConfidence)}),
      ...(q.maxConfidence === "" ? {} : {max_confidence: Number(q.maxConfidence)}),
    };
  }
  const requestId = randomUUID();
  const run = await launchRun({
    job: `${prefix}_input_job`,
    runConfig: {ops: {[asset]: {config}}},
    tags: {"backoffice/action": "select-crawl-inputs", "crawl/type": String(crawlType), "backoffice/request_id": requestId},
  }, {timeoutMs: 15_000});
  return { ok: true as const, ...run, runUrl: dagsterRunUrl(run.runId), table: `corpscout.${asset}`, requestId };
}
