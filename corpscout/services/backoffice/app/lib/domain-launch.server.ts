import { randomUUID } from "node:crypto";
import { launchRun, dagsterRunUrl, type DagsterOptions } from "~/lib/dagster.server";
import type { CompanyActionResult } from "~/lib/company-actions";

export async function launchDomainAction(
  input: { operation: string; changedOnly: boolean; requestedBy: string },
  options: DagsterOptions & { databasePath?: string } = {},
): Promise<CompanyActionResult> {
  if (!["sync", "process"].includes(input.operation)) throw new Error("Choose a Domains operation.");
  if (!input.requestedBy.trim()) throw new Error("An operator identity is required.");
  const process = input.operation === "process";
  const domainConfig = { changed_only: input.changedOnly, page_size: 1_000 };
  const run = await launchRun({
    job: process ? "se_company_domain_refresh_job" : "se_company_domain_sync_job",
    runConfig: { ops: {
      ...Object.fromEntries(["brave", "wikidata", "esef_filing", "common_crawl_identity", "crawler_lookup"].map((source) => [
        `se_company_domain_suggestions_${source}`, { config: { execute: true, page_size: 5_000 } },
      ])),
      ...(process ? {
        se_company_domain_publish: { config: domainConfig },
      } : {}),
    } },
    tags: {
      "corpscout/trigger_source": "backoffice", "corpscout/request_id": randomUUID(),
      "corpscout/requested_by": input.requestedBy.trim(), "corpscout/country_iso2": "SE",
      "corpscout/company_area": "domains", "corpscout/company_operation": input.operation,
    },
  }, options);
  return { ok: true, error: "", ...run, runUrl: dagsterRunUrl(run.runId, options.url) };
}
