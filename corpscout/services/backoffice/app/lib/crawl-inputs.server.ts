import { chQuery } from "~/lib/clickhouse.server";
import type { CrawlInputsSnapshot, CrawlInputRow, CrawlInputStats } from "~/lib/crawl-inputs";
import { DOMAIN_CRAWL_TYPES, type DomainCrawlType } from "~/lib/se-domain-selection";
import { dagsterRunUrl, launchRun } from "~/lib/dagster.server";
import { formSettings, parseCrawlSettings } from "~/lib/crawl-settings.server";

const INPUT_TABLES = {
  full: "corpscout.website_full_crawl_requests_current",
  jobs: "corpscout.website_jobs_crawl_requests_current",
  site_info: "corpscout.website_site_info_requests_current",
} as const;

export async function loadCrawlInputs(search: URLSearchParams): Promise<CrawlInputsSnapshot> {
  const stats = await chQuery<CrawlInputStats>(DOMAIN_CRAWL_TYPES.map(({ value }) => `
    SELECT '${value}' AS type, toUInt32(count()) AS total,
      toUInt32(countIf(enabled)) AS enabled FROM ${INPUT_TABLES[value]}
  `).join(" UNION ALL "));
  const requestedType = search.get("input_type");
  const type = DOMAIN_CRAWL_TYPES.some(({ value }) => value === requestedType)
    ? requestedType as DomainCrawlType
    : DOMAIN_CRAWL_TYPES.find(({ value }) => stats.some((s) => s.type === value && s.total > 0))?.value ?? "full";
  const domain = (search.get("input_domain") ?? "").trim().slice(0, 253);
  const requestedOffset = Number(search.get("input_offset") ?? 0);
  const offset = Number.isSafeInteger(requestedOffset) && requestedOffset >= 0 ? Math.min(requestedOffset, 1_000_000) : 0;
  const limit = 25;
  const where = "WHERE positionCaseInsensitive(domain, {domain:String}) > 0";
  const params = { domain, offset, limit };
  const [rows, counts] = await Promise.all([
    chQuery<CrawlInputRow>(`SELECT domain, website_url, enabled, priority, page_mode, pages,
      headless, proxy_route, formatDateTime(updated_at, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS updated_at
      FROM ${INPUT_TABLES[type]} ${where}
      ORDER BY priority DESC, domain ASC LIMIT {limit:UInt32} OFFSET {offset:UInt32}`, params),
    chQuery<{ total: number }>(`SELECT toUInt32(count()) AS total FROM ${INPUT_TABLES[type]} ${where}`, params),
  ]);
  return { stats, type, domain, rows, total: counts[0].total, offset, limit };
}

export async function startSavedCrawls(form: FormData) {
  const type = String(form.get("crawl_type") ?? "");
  if (!DOMAIN_CRAWL_TYPES.some(({ value }) => value === type)) throw new Error("Choose a valid crawl type.");
  const batchId = String(form.get("batch_id") ?? "");
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(batchId)) throw new Error("Invalid crawl batch ID.");
  const serialized = String(form.get("domains") ?? "");
  if (serialized.length > 30_000) throw new Error("Select at most 100 saved domains.");
  const domains: unknown = JSON.parse(serialized);
  if (!Array.isArray(domains) || domains.length === 0 || domains.length > 100 || domains.some((domain) => typeof domain !== "string" || domain.length > 253 || !/^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(domain))) {
    throw new Error("Select between 1 and 100 saved domains.");
  }
  const selected = [...new Set(domains)] as string[];
  const rows = await chQuery<{domain: string; enabled: boolean}>(
    `SELECT domain, enabled FROM ${INPUT_TABLES[type as DomainCrawlType]} WHERE domain IN {domains:Array(String)}`,
    { domains: selected },
  );
  if (rows.length !== selected.length) throw new Error("Some selected inputs no longer exist. Refresh the list.");
  if (rows.some((row) => !row.enabled)) throw new Error("Some selected inputs are disabled. Refresh the list.");
  const asset = type === "site_info" ? "website_site_info_results" : `website_${type}_crawl_results`;
  const settings = parseCrawlSettings(formSettings(form), type as DomainCrawlType);
  const run = await launchRun({
    job: `${asset}_job`,
    runConfig: {ops: {[asset]: {config: {batch_id: batchId, domains: selected, batch_size: selected.length, ...settings}}}},
    tags: {"backoffice/action": "crawl-saved-inputs", "crawl/type": type, "crawl/batch_id": batchId},
  }, {timeoutMs: 15_000});
  return {...run, runUrl: dagsterRunUrl(run.runId), batchId, count: selected.length, domains: selected};
}
