import { crawlSettingsFor } from "~/lib/crawl-settings";

export const CRAWL_TEST_PROFILES = [
  {value: "site_info", label: "Basic info", description: "Classify the website and collect basic information from one page, including shops and content sites."},
  {value: "company_lookup", label: "Find company", description: "Identify the operator of any website, including shops, news and streaming sites, through its footer, About, contact and legal pages. Propose a Swedish registry match without saving company associations."},
  {value: "full", label: "Full crawl", description: "Research a company website: company details, contacts, products, people, jobs and financial sources."},
  {value: "jobs", label: "Jobs", description: "Discover vacancies and collect full job descriptions, including the company’s recruiting platform."},
  {value: "pages", label: "Specific pages", description: "Collect the pages you provide without discovering additional pages."},
  {value: "custom", label: "Custom discovery", description: "Find and collect pages that match your instructions, optionally restricting collection to specific candidate pages."},
] as const;
export type CrawlTestProfile = typeof CRAWL_TEST_PROFILES[number]["value"];

export function crawlPageTab(search: URLSearchParams) {
  const tab = search.get("tab");
  if (tab === "crawl" || tab === "inputs" || tab === "attempts") return tab;
  if (["domain", "state", "source", "offset"].some(key => search.has(key))) return "attempts";
  return search.has("input_type") ? "inputs" : "crawl";
}

function websiteUrl(value: string, base?: string) {
  let url: URL;
  try { url = new URL(base ? value : /^[a-z][a-z\d+.-]*:/i.test(value) ? value : `https://${value}`, base); }
  catch { throw new Error("Enter a valid domain or HTTP(S) website URL."); }
  if (!["http:", "https:"].includes(url.protocol) || !url.hostname || url.username || url.password || /\s/.test(value)) {
    throw new Error("Enter an HTTP(S) website URL without credentials.");
  }
  url.hash = "";
  return url.href;
}

/** Convert the guided form into the crawler's request contract. */
export function parseTestCrawl(form: FormData) {
  const read = (name: string) => String(form.get(name) ?? "").trim();
  const profile = read("crawl_profile");
  if (!CRAWL_TEST_PROFILES.some(item => item.value === profile)) throw new Error("Choose a crawler profile.");
  if (!read("url")) throw new Error("Enter a domain or website URL.");
  const url = websiteUrl(read("url"));
  const profileId = read("llm_profile_id");
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(profileId)) throw new Error("Choose a saved LLM before crawling.");
  const integer = (name: string, min: number, max: number) => {
    const value = read(name);
    if (!/^\d+$/.test(value) || Number(value) < min || Number(value) > max) throw new Error(`${name} must be an integer between ${min} and ${max}.`);
    return Number(value);
  };
  const boolean = (name: string) => {
    const value = read(name);
    if (value !== "true" && value !== "false") throw new Error(`Choose a value for ${name}.`);
    return value === "true";
  };
  const maxPages = integer("max_pages", 1, 500);
  const maxCalls = integer("max_model_calls", 1, 1000);
  const agentBudget = integer("challenge_agent_max_runs", 3, 1000);
  const agentModel = read("challenge_agent_model");
  if (!["deepseek-flash", "z-ai/glm-5.3-flash"].includes(agentModel)) throw new Error("Choose a CAPTCHA model.");
  const allSites = boolean("full_crawl_all");
  const matching = profile === "company_lookup" || (form.has("match_company") && boolean("match_company"));
  if (matching && !["site_info", "full", "company_lookup"].includes(profile)) throw new Error("Company matching is available for basic and full crawls.");
  const country = (read("country") || (profile !== "company_lookup" ? "SE" : "")).toUpperCase();
  if (matching && country !== "SE") throw new Error("Company matching currently supports Sweden (SE).");
  const skipIfMapped = form.has("skip_company_matching_if_mapped") ? boolean("skip_company_matching_if_mapped") : false;
  if (profile === "company_lookup" && (country !== "SE" || maxPages > 10 || allSites)) throw new Error("Find company supports Sweden (SE) and at most 10 identity pages for any website category; full content crawling is not used.");
  if (profile === "company_lookup" && (new URL(url).pathname !== "/" || new URL(url).search || new URL(url).port)) throw new Error("Find company starts from a domain's homepage; remove the path, query and port.");
  const instructions = read("instructions");
  const pages = [...new Set(read("pages").split(/\r?\n/).map(page => page.trim()).filter(Boolean).map(page => websiteUrl(page, url)))];
  if (pages.length > 1000) throw new Error("Enter at most 1000 pages.");
  if (pages.length && profile !== "pages" && profile !== "custom") throw new Error("Specific pages are available only for Specific pages or Custom discovery.");
  if (profile === "pages" && (!pages.length || pages.length > maxPages)) throw new Error("Enter at least one page and set the page limit to cover every supplied page.");
  if (profile === "custom" ? !instructions || instructions.length > 20000 : Boolean(instructions)) throw new Error("Provide instructions only for Custom discovery (at most 20,000 characters).");
  if (profile === "site_info" && (maxPages !== 1 || allSites)) throw new Error("Basic info uses one page and already supports every site type.");
  if (profile === "pages" && allSites) throw new Error("Specific pages are collected directly without site classification.");
  if (form.has("research_config")) throw new Error("Use the named crawl settings instead of JSON overrides. Reload the form.");
  const settings = crawlSettingsFor(profile as CrawlTestProfile, pages.length > 0);
  const overrides: Record<string, number | boolean> = {};
  for (const [key] of form) {
    if (!key.startsWith("config.")) continue;
    const setting = settings.find(item => key === `config.${item.name}`);
    if (!setting) throw new Error(`Setting ${key.slice(7)} is not available for this crawl profile.`);
    overrides[setting.name] = typeof setting.defaultValue === "boolean" ? boolean(key) : integer(key, setting.min!, setting.max!);
  }
  if (settings.some(setting => setting.name === "max_sitemap_urls") && Number(overrides.max_sitemap_urls ?? 500) >= Number(overrides.max_candidates ?? 1000)) throw new Error("Candidate URL limit must be greater than the sitemap URL limit.");
  const decisionTasks = (["site_eligibility", "link_selection", "company_match"] as const).filter(task => {
    const selected = read(`decision.${task}`) || "processing";
    if (!["processing", "jev"].includes(selected)) throw new Error("Choose Processing LLM or Jev for each decision step.");
    if (selected === "jev" && (profile === "pages" || task === "link_selection" && ["site_info", "company_lookup"].includes(profile))) throw new Error("This decision step is not used by the selected crawl profile.");
    if (selected === "jev" && task === "company_match" && !matching) throw new Error("Enable company matching to rank company candidates.");
    return selected === "jev";
  });
  const decisionProfileId = read("decision_llm_profile_id");
  if (decisionTasks.length && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(decisionProfileId)) throw new Error("Choose a saved Jev model for the selected decision steps.");
  const sessionId = read("session_id");
  if (sessionId && !/^[a-f0-9]{32}$/.test(sessionId)) throw new Error("Browser session ID must be 32 hexadecimal characters.");
  const payload = {
    url,
    debug: true,
    ...(matching ? {company_lookup: {country, skip_if_mapped: skipIfMapped}} : {}),
    ...(decisionTasks.length ? {decision_tasks: decisionTasks} : {}),
    ...(profile !== "pages" ? {site_info: true} : {}),
    ...(profile === "site_info" ? {crawl: false} : {}),
    ...(profile === "full" ? {crawl: "full"} : {}),
    ...(profile === "jobs" ? {instructions: "Collect the target company's current job vacancies and full job descriptions. Discover careers and jobs pages, including the company's confirmed recruiting platform. Preserve job links and simplified HTML; do not infer technologies or analyze jobs."} : {}),
    ...(profile === "custom" ? {instructions} : {}),
    ...(pages.length ? {pages} : {}),
    full_crawl_all: allSites,
    save_artifacts: boolean("save_artifacts"),
    interactive: boolean("interactive"),
    ...(sessionId ? {session_id: sessionId} : {}),
    challenge_agent_model: agentModel,
    challenge_agent_max_runs: agentBudget,
    config: {...overrides, max_pages: maxPages, max_model_calls: maxCalls},
  };
  return {profileId, decisionProfileId: decisionTasks.length ? decisionProfileId : null, payload};
}
