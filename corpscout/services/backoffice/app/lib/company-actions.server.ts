import { randomUUID } from "node:crypto";
import { dagsterRunUrl, launchRun, listRuns, type DagsterOptions } from "~/lib/dagster.server";
import { getLlmProfile, getLlmProfileApiKey } from "~/lib/llm-settings.server";
import { encryptCrawlLlm } from "~/lib/crawl-llm.server";
import { launchDomainAction } from "~/lib/domain-launch.server";
import { launchPeopleAction } from "~/lib/people-launch.server";
import { ACTIVE_COMPANY_RUN_STATUSES, COMPANY_ACTION_AREAS, type CompanyActionResult } from "~/lib/company-actions";

interface CompanyActionInput {
  area: string; operation: string; profileId: string; promptId: string;
  promptRevision: number; changedOnly: boolean; llmMaxCompanies: number; requestedBy: string; verifyDomains?: boolean;
}

// The backoffice runs as one server. Hold this lock across the Dagster lookup
// and submission so simultaneous requests cannot both pass the status check.
const submittingAreas = new Set<string>();

export async function launchCompanyAction(
  input: CompanyActionInput,
  options: DagsterOptions & { databasePath?: string } = {},
): Promise<CompanyActionResult> {
  const area = COMPANY_ACTION_AREAS.find((entry) => entry.value === input.area);
  if (!area) throw new Error("Choose a company data area.");
  if (!["sync", "process"].includes(input.operation)) throw new Error("Choose a company operation.");
  if (!input.requestedBy.trim()) throw new Error("An operator identity is required.");
  if (submittingAreas.has(area.value)) return { ok: false, error: "A launch is already being submitted for this workflow. Refresh its status before trying again." };
  submittingAreas.add(area.value);
  try {
    const lookups = await Promise.allSettled(Object.values(area.jobs).map((job) =>
      listRuns({ job, limit: 1, statuses: ACTIVE_COMPANY_RUN_STATUSES }, { ...options, timeoutMs: options.timeoutMs ?? 10_000 })));
    for (const lookup of lookups) {
      if (lookup.status === "rejected") throw new Error("Could not verify this workflow's run status in Dagster. Refresh and try again.");
      const active = lookup.value[0];
      if (active) return { ok: false, error: `${area.label} already has a queued or active run. Wait for it to finish before starting another.`, runId: active.runId, status: active.status, runUrl: dagsterRunUrl(active.runId, options.url) };
    }
    return await dispatchCompanyAction(input, options);
  } finally {
    submittingAreas.delete(area.value);
  }
}

async function dispatchCompanyAction(
  input: CompanyActionInput,
  options: DagsterOptions & { databasePath?: string },
): Promise<CompanyActionResult> {
  if (input.area === "domains") return launchDomainAction(input, options);
  if (input.area === "people") return launchPeopleAction(input, options);

  const fullProcessing = input.operation === "process";
  const info = input.area === "info";
  const addresses = input.area === "addresses";
  const prefix = info ? "se_company_basic_info" : addresses ? "se_company_address" : "se_company_financial";
  const sources = info ? ["scb", "bolagsverket", "esef", "wikidata", "ratsit"]
    : addresses ? ["scb", "bolagsverket", "ratsit", "esef"] : ["bolagsverket", "bolagsverket_comparative", "esef", "ratsit"];
  const sourcePageSize = info ? 20_000 : addresses ? 10_000 : 5_000;
  const ops: Record<string, { config: Record<string, unknown> }> = Object.fromEntries(sources.map((source) => [
    `${info ? "se_basic_info" : prefix}_suggestions_${source}`, { config: { execute: true, page_size: sourcePageSize } },
  ]));
  const tags: Record<string, string> = {
    "corpscout/trigger_source": "backoffice",
    "corpscout/request_id": randomUUID(),
    "corpscout/requested_by": input.requestedBy.trim(),
    "corpscout/country_iso2": "SE",
    "corpscout/company_area": input.area,
    "corpscout/company_operation": input.operation,
  };
  if (input.area === "finance") ops.se_ratsit_financial_periods_usd = { config: { execute: true } };
  if (addresses) {
    ops.se_company_address_normalize = { config: { changed_only: true, page_size: 20_000 } };
    if (fullProcessing) ops.se_address_geocodes_warm = { config: { chunk_size: 150_000, limit: 0 } };
  }
  if (info && fullProcessing) {
    const profile = await getLlmProfile(input.profileId);
    if (!profile) throw new Error("Choose a saved LLM profile.");
    if (!Number.isInteger(input.llmMaxCompanies) || input.llmMaxCompanies < 1 || input.llmMaxCompanies > 1_000_000) {
      throw new Error("The description processing limit must be between 1 and 1,000,000 companies.");
    }
    ops.se_basic_info_suggestions_llm = { config: {
      execute: true, max_companies: input.llmMaxCompanies,
      llm: {
        ...encryptCrawlLlm(profile, await getLlmProfileApiKey(profile.profileId, profile.revision), process.env.CRAWLER_LLM_ENCRYPTION_KEY ?? ""),
        temperature: 0, max_tokens: 6_000, concurrency: 1,
      },
    } };
    tags["corpscout/llm_provider"] = profile.provider;
    tags["corpscout/llm_model"] = profile.model;
  }
  if (fullProcessing) ops[`${prefix}_publish`] = { config: { changed_only: true, page_size: input.area === "finance" ? 5_000 : 20_000 } };
  const run = await launchRun({
    job: `${prefix}_${fullProcessing ? "refresh" : "sync"}_job`, runConfig: { ops }, tags,
  }, options);
  return { ok: true, error: "", ...run, runUrl: dagsterRunUrl(run.runId) };
}
