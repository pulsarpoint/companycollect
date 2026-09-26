import type { DomainCrawlType } from "~/lib/se-domain-selection";

/** Reads one submitted setting; null when the field was not submitted. */
export type SettingReader = (name: string) => string | null;

export function formSettings(form: FormData): SettingReader {
  return (name) => (form.has(name) ? String(form.get(name)) : null);
}

export function objectSettings(value: unknown): SettingReader {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("Submit the crawl settings.");
  const settings = value as Record<string, unknown>;
  return (name) => {
    const entry = settings[name];
    if (entry === undefined || entry === null) return null;
    if (typeof entry !== "string" && typeof entry !== "number" && typeof entry !== "boolean") throw new Error(`Invalid crawl setting: ${name}.`);
    return String(entry);
  };
}

/**
 * Validate the results asset's required execution settings. Shared by the Crawler
 * page's saved-input start and the domain page's crawl workflow launch, so both
 * send Dagster the same explicitly chosen model, limits and freshness policy.
 */
export function parseCrawlSettings(read: SettingReader, type: DomainCrawlType) {
  const agentModel = read("challenge_agent_model") ?? "";
  if (!["deepseek-flash", "z-ai/glm-5.3-flash"].includes(agentModel)) throw new Error("Choose a CAPTCHA model.");
  const agentRuns = requiredInteger(read, "challenge_agent_max_runs", 3, 1000);
  const profileId = (read("llm_profile_id") ?? "").trim();
  if (!profileId || profileId.length > 200) throw new Error("Choose an LLM from LLM settings before starting the crawl.");
  const maxPages = requiredInteger(read, "max_pages", 1, 500);
  const maxModelCalls = requiredInteger(read, "max_model_calls", 1, 1000);
  const pageSelection = read("page_selection") ?? "";
  if (!["saved", "instructions", "basic_info"].includes(pageSelection) || (type === "site_info") !== (pageSelection === "basic_info")) {
    throw new Error("Choose the page selection mode for this crawl type.");
  }
  if (type === "site_info" && maxPages !== 1) throw new Error("Basic info uses one page.");
  const instructions = (read("instructions") ?? "").trim();
  if (pageSelection === "instructions" ? !instructions || instructions.length > 20000 : Boolean(instructions)) {
    throw new Error("Provide instructions only for custom page selection.");
  }
  const forceRefresh = read("force_refresh") ?? "false";
  if (!["true", "false"].includes(forceRefresh)) throw new Error("Invalid refresh option.");
  const fullCrawlAll = read("full_crawl_all") ?? "false";
  if (!["true", "false"].includes(fullCrawlAll) || (type !== "full" && fullCrawlAll === "true")) throw new Error("Full crawl all is a boolean option for full crawls only.");
  const maxInFlight = read("max_in_flight") === null ? 3 : requiredInteger(read, "max_in_flight", 1, 20);
  const refreshDays = read("refresh_interval_days") === null ? 30 : requiredInteger(read, "refresh_interval_days", 1, 3650);
  return {
    force_refresh: forceRefresh === "true",
    ...(type === "full" ? {full_crawl_all: fullCrawlAll === "true"} : {}),
    challenge_agent_model: agentModel, challenge_agent_max_runs: agentRuns, llm_profile_id: profileId,
    max_pages: maxPages, max_model_calls: maxModelCalls, page_selection: pageSelection,
    max_in_flight: maxInFlight, refresh_interval_days: refreshDays,
    ...(pageSelection === "instructions" ? { instructions } : {}),
  };
}

function requiredInteger(read: SettingReader, name: string, min: number, max: number): number {
  const value = read(name) ?? "";
  if (!/^\d+$/.test(value) || Number(value) < min || Number(value) > max) throw new Error(`${name} must be an integer between ${min} and ${max}.`);
  return Number(value);
}
