import { randomUUID } from "node:crypto";
import { launchRun, dagsterRunUrl, type DagsterOptions } from "~/lib/dagster.server";
import { getLlmProfile, getLlmProfileApiKey } from "~/lib/llm-settings.server";
import { encryptCrawlLlm } from "~/lib/crawl-llm.server";
import { getDomainPrompt } from "~/lib/domain-prompts.server";
import type { CompanyActionResult } from "~/lib/company-actions";

export async function launchDomainAction(
  input: { operation: string; profileId: string; promptId: string; promptRevision: number;
    changedOnly: boolean; verifyDomains?: boolean; requestedBy: string },
  options: DagsterOptions & { databasePath?: string } = {},
): Promise<CompanyActionResult> {
  if (!["sync", "process"].includes(input.operation)) throw new Error("Choose a Domains operation.");
  if (!input.requestedBy.trim()) throw new Error("An operator identity is required.");
  const process = input.operation === "process";
  let verification: Record<string, unknown> | undefined;
  if (process && input.verifyDomains) {
    const profile = await getLlmProfile(input.profileId);
    if (!profile) throw new Error("Choose a saved LLM profile.");
    const prompt = getDomainPrompt(input.promptId, options.databasePath);
    if (!prompt) throw new Error("Choose a saved Domain prompt.");
    if (prompt.revision !== input.promptRevision) throw new Error("The selected prompt changed. Reload to review its latest revision before launching.");
    verification = {
      ...encryptCrawlLlm(profile, await getLlmProfileApiKey(profile.profileId, profile.revision), globalThis.process.env.CRAWLER_LLM_ENCRYPTION_KEY ?? ""),
      system_prompt: prompt.systemPrompt, prompt_version: `domain:${prompt.promptId}:r${prompt.revision}`,
      temperature: 0, concurrency: 1,
    };
  }
  // Both steps select the same evidence/prompt/model fingerprint. Only verification
  // may call the model; publication reads persisted answers.
  const domainConfig = { changed_only: input.changedOnly, page_size: 1_000, ...(verification ? { verification } : {}) };
  const run = await launchRun({
    job: process ? "se_company_domain_refresh_job" : "se_company_domain_sync_job",
    runConfig: { ops: {
      ...Object.fromEntries(["brave", "wikidata", "esef_filing", "common_crawl_identity"].map((source) => [
        `se_company_domain_suggestions_${source}`, { config: { execute: true, page_size: 5_000 } },
      ])),
      ...(process ? {
        se_company_domain_verification: { config: domainConfig },
        se_company_domain_publish: { config: domainConfig },
      } : {}),
    } },
    tags: {
      "corpscout/trigger_source": "backoffice", "corpscout/request_id": randomUUID(),
      "corpscout/requested_by": input.requestedBy.trim(), "corpscout/country_iso2": "SE",
      "corpscout/company_area": "domains", "corpscout/company_operation": input.operation,
      ...(verification ? {
        "corpscout/llm_provider": String(verification.provider), "corpscout/llm_model": String(verification.model),
        "corpscout/prompt_version": String(verification.prompt_version),
        "corpscout/verification_scope": "uncertain_or_conflicting",
      } : {}),
    },
  }, options);
  return { ok: true, error: "", ...run, runUrl: dagsterRunUrl(run.runId, options.url) };
}
