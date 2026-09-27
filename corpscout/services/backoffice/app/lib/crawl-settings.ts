import type { CrawlTestProfile } from "~/lib/crawl-test";

type CrawlSetting = {
  name: string; label: string; description: string;
  group: "fetch" | "model" | "discovery" | "search";
  defaultValue: number | boolean; fullDefault?: number | boolean;
  min?: number; max?: number;
};

/** Operator controls for the collection endpoint (offline extraction has separate settings). */
export const CRAWL_SETTINGS: CrawlSetting[] = [
  {name: "page_timeout_seconds", label: "Page timeout (seconds)", description: "Maximum time allowed for each page navigation.", group: "fetch", defaultValue: 45, min: 1, max: 600},
  {name: "page_attempts", label: "Page attempts", description: "Total navigation attempts per page, including the first attempt.", group: "fetch", defaultValue: 2, min: 1, max: 3},
  {name: "max_browser_restarts", label: "Browser recovery limit", description: "Maximum browser restarts after a browser failure.", group: "fetch", defaultValue: 2, min: 0, max: 10},
  {name: "check_robots_txt", label: "Respect robots.txt", description: "Check the site's crawling rules before fetching pages.", group: "fetch", defaultValue: true},
  {name: "model_timeout_seconds", label: "Model timeout (seconds)", description: "Time limit for a model call, including retries. Applies to processing and Jev.", group: "model", defaultValue: 180, min: 1, max: 1800},
  {name: "max_http_attempts", label: "Model request attempts", description: "Total attempts for a transient model API failure. All attempts count toward the call limit.", group: "model", defaultValue: 2, min: 1, max: 4},
  {name: "max_corrections", label: "Model correction attempts", description: "Additional requests to correct invalid structured responses.", group: "model", defaultValue: 1, min: 0, max: 2},
  {name: "max_output_tokens", label: "Output token limit", description: "Maximum output tokens per processing-model call. Jev returns typed decisions instead of generated text.", group: "model", defaultValue: 65536, min: 1, max: 262144},
  {name: "max_external_pages", label: "External page limit", description: "Maximum pages outside the submitted website, including recruiting and financial sources.", group: "discovery", defaultValue: 3, fullDefault: 30, min: 0, max: 500},
  {name: "max_source_domains", label: "External source domains", description: "Maximum external domains that may be followed as navigation sources.", group: "discovery", defaultValue: 3, min: 0, max: 20},
  {name: "max_source_pages_per_domain", label: "Pages per external source", description: "Page limit for each confirmed external navigation source.", group: "discovery", defaultValue: 4, min: 1, max: 50},
  {name: "max_source_depth", label: "External source depth", description: "How many link levels to follow within a confirmed external source.", group: "discovery", defaultValue: 3, min: 0, max: 10},
  {name: "max_candidates", label: "Candidate URL limit", description: "Maximum URLs considered for collection. Must exceed the sitemap URL limit.", group: "discovery", defaultValue: 1000, min: 1, max: 100000},
  {name: "max_sitemap_urls", label: "Sitemap URL limit", description: "Maximum candidates discovered through sitemaps; zero disables sitemap URLs.", group: "discovery", defaultValue: 500, min: 0, max: 99999},
  {name: "max_sitemap_files", label: "Sitemap file limit", description: "Maximum sitemap files to read.", group: "discovery", defaultValue: 20, min: 0, max: 1000},
  {name: "selection_batch_size", label: "Links per decision batch", description: "Number of candidate links assessed together by the selected discovery model.", group: "discovery", defaultValue: 20, min: 1, max: 100},
  {name: "selection_batches_per_page", label: "Decision batches per pass", description: "Number of batches assessed on each selection pass. Further passes may run to find a matching page.", group: "discovery", defaultValue: 1, min: 1, max: 100},
  {name: "max_assessment_attempts", label: "Link assessment attempts", description: "Maximum times an unresolved candidate can be assessed.", group: "discovery", defaultValue: 2, min: 1, max: 3},
  {name: "exploration_pages", label: "Uncertain page allowance", description: "Maximum pages collected to explore candidates with uncertain relevance.", group: "discovery", defaultValue: 3, min: 0, max: 500},
  {name: "web_search", label: "Discover through Brave search", description: "Use Brave to find additional company sources when website links are insufficient.", group: "search", defaultValue: false, fullDefault: true},
  {name: "max_search_queries", label: "Search query limit", description: "Maximum external search queries per crawl. Zero disables searches.", group: "search", defaultValue: 3, min: 0, max: 20},
  {name: "search_results_per_query", label: "Results per search", description: "Maximum search results considered for each query.", group: "search", defaultValue: 5, min: 1, max: 20},
  {name: "search_context_chars", label: "Search planning context (characters)", description: "Maximum captured context supplied to the processing model for planning searches.", group: "search", defaultValue: 12000, min: 1000, max: 60000},
];

export function crawlSettingsFor(profile: CrawlTestProfile, restrictedPages = false) {
  return CRAWL_SETTINGS.filter(setting => !(profile === "company_lookup" && setting.name === "max_browser_restarts")).filter(setting => setting.group === "fetch"
    || setting.group === "model" && profile !== "pages"
    || ["full", "jobs", "custom"].includes(profile) && (setting.group !== "search" || !restrictedPages)
      && (!restrictedPages || !["max_sitemap_urls", "max_sitemap_files"].includes(setting.name)));
}
