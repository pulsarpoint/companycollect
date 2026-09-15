import { randomUUID } from "node:crypto";
import { launchRun, dagsterRunUrl, type DagsterOptions } from "~/lib/dagster.server";
import { getLlmProfile } from "~/lib/llm-settings.server";
import { getPeoplePrompt } from "~/lib/people-prompts.server";
import type { CompanyActionResult } from "~/lib/company-actions";

export async function launchPeopleAction(
  input: { operation: string; profileId: string; promptId: string; promptRevision: number; changedOnly: boolean; requestedBy: string },
  options: DagsterOptions & { databasePath?: string } = {},
): Promise<CompanyActionResult> {
  if (!["sync", "process"].includes(input.operation)) throw new Error("Choose a People operation.");
  if (!input.requestedBy.trim()) throw new Error("An operator identity is required.");
  let match: Record<string, unknown> | undefined;
  if (input.operation === "process") {
    const profile = getLlmProfile(input.profileId, options.databasePath);
    if (!profile) throw new Error("Choose a saved LLM profile.");
    const prompt = getPeoplePrompt(input.promptId, options.databasePath);
    if (!prompt) throw new Error("Choose a saved People prompt.");
    if (prompt.revision !== input.promptRevision) throw new Error("The selected prompt changed. Reload to review its latest revision before launching.");
    match = {
      provider: profile.provider,
      model: profile.model,
      base_url: profile.baseUrl,
      api_key_environment_variable: profile.apiKeyEnvironmentVariable,
      system_prompt: prompt.systemPrompt,
      prompt_version: `people:${prompt.promptId}:r${prompt.revision}`,
      temperature: 0,
      concurrency: 1,
      changed_only: input.changedOnly,
    };
  }
  const run = await launchRun({
    job: input.operation === "process" ? "se_company_person_refresh_job" : "se_company_person_sync_job",
    runConfig: { ops: {
      ...Object.fromEntries(["bolagsverket", "esef", "wikidata", "ratsit"].map((source) => [
        `se_company_person_suggestions_${source}`, { config: { execute: true, page_size: 10_000 } },
      ])),
      se_company_person_normalize: { config: { changed_only: true } },
      se_company_person_match_input: { config: { changed_only: true } },
      ...(match ? {
        se_company_person_match: { config: match },
        se_company_person_publish: { config: { changed_only: true, page_size: 10_000 } },
      } : {}),
    } },
    tags: {
      "corpscout/trigger_source": "backoffice",
      "corpscout/request_id": randomUUID(),
      "corpscout/requested_by": input.requestedBy.trim(),
      "corpscout/country_iso2": "SE",
      "corpscout/people_operation": input.operation,
      "corpscout/company_area": "people",
      "corpscout/company_operation": input.operation,
      ...(match ? {
        "corpscout/llm_provider": String(match.provider),
        "corpscout/llm_model": String(match.model),
        "corpscout/prompt_version": String(match.prompt_version),
      } : {}),
    },
  }, options);
  return { ok: true, error: "", ...run, runUrl: dagsterRunUrl(run.runId) };
}
