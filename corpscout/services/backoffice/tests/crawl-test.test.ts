import { describe, expect, it } from "vitest";
import { crawlPageTab, parseTestCrawl } from "~/lib/crawl-test";
function form(overrides: Record<string, string> = {}) {
  const result = new FormData();
  for (const [key, value] of Object.entries({crawl_profile: "full", url: "example.com", llm_profile_id: "11111111-1111-4111-8111-111111111111", max_pages: "20", max_model_calls: "100", challenge_agent_max_runs: "3", challenge_agent_model: "deepseek-flash", full_crawl_all: "false", save_artifacts: "true", interactive: "false", ...overrides})) result.set(key, value);
  return result;
}
describe("crawl test profiles", () => {
  it("maps basic, full and jobs to the real crawler modes", () => {
    expect(parseTestCrawl(form({crawl_profile: "site_info", max_pages: "1"})).payload).toMatchObject({site_info: true, crawl: false, config: {max_pages: 1}, full_crawl_all: false});
    expect(parseTestCrawl(form()).payload).toMatchObject({url: "https://example.com/", crawl: "full", full_crawl_all: false});
    const jobs = parseTestCrawl(form({crawl_profile: "jobs"})).payload;
    expect(jobs.instructions).toContain("current job vacancies");
    expect(jobs).not.toHaveProperty("crawl");
  });
  it("normalizes and deduplicates explicit pages and classifies the website before selecting custom candidates", () => {
    expect(parseTestCrawl(form({crawl_profile: "pages", pages: "/about\nhttps://example.com/about\n/contact"})).payload.pages).toEqual(["https://example.com/about", "https://example.com/contact"]);
    expect(parseTestCrawl(form({crawl_profile: "custom", instructions: "Find annual reports", pages: "/investors", full_crawl_all: "true"})).payload).toMatchObject({instructions: "Find annual reports", pages: ["https://example.com/investors"], site_info: true, full_crawl_all: true});
  });
  it.each<Record<string, string>>([
    {url: "javascript:alert(1)"}, {url: "https://user:password@example.com"}, {url: ""},
    {crawl_profile: "site_info"}, {crawl_profile: "pages", pages: ""},
    {crawl_profile: "pages", pages: "/a", full_crawl_all: "true"}, {llm_profile_id: "------------------------------------"},
    {crawl_profile: "pages", pages: "/a\n/b", max_pages: "1"}, {crawl_profile: "custom"},
    {crawl_profile: "full", instructions: "No"}, {max_pages: "0"}, {max_model_calls: "1.5"},
    {"config.model": "override"}, {"config.reasoning_effort": "high"},
    {research_config: '{"api_key":"private"}'}, {research_config: '{"nested":{"api_key":"private"}}'},
    {research_config: '[]'}, {research_config: '{'}, {full_crawl_all: "yes"},
  ])("rejects contradictory or unsafe settings: %j", invalid => {
    expect(() => parseTestCrawl(form(invalid))).toThrow();
  });
  it("parses named settings and rejects controls unused by the profile", () => {
    expect(parseTestCrawl(form({"config.max_output_tokens": "4096", "config.web_search": "true", "config.page_timeout_seconds": "30"})).payload.config).toEqual({max_output_tokens: 4096, web_search: true, page_timeout_seconds: 30, max_pages: 20, max_model_calls: 100});
    expect(() => parseTestCrawl(form({crawl_profile: "site_info", max_pages: "1", "config.web_search": "true"}))).toThrow("not available");
    expect(() => parseTestCrawl(form({"config.max_candidates": "200", "config.max_sitemap_urls": "200"}))).toThrow("greater");
  });
  it("requires a saved Jev for selected decision steps and omits it when unused", () => {
    expect(() => parseTestCrawl(form({"decision.link_selection": "jev"}))).toThrow("saved Jev");
    const configured = parseTestCrawl(form({"decision.link_selection": "jev", decision_llm_profile_id: "22222222-2222-4222-8222-222222222222"}));
    expect(configured.payload.decision_tasks).toEqual(["link_selection"]);
    expect(configured.decisionProfileId).toBe("22222222-2222-4222-8222-222222222222");
    expect(parseTestCrawl(form({decision_llm_profile_id: "unused"})).decisionProfileId).toBeNull();
    expect(() => parseTestCrawl(form({crawl_profile: "site_info", max_pages: "1", "decision.link_selection": "jev"}))).toThrow("not used");
  });
  it("keeps tabs bookmarkable and supports existing saved-input and status links", () => {
    expect(crawlPageTab(new URLSearchParams())).toBe("crawl");
    expect(crawlPageTab(new URLSearchParams("input_type=jobs"))).toBe("inputs");
    expect(crawlPageTab(new URLSearchParams("domain=example.com"))).toBe("attempts");
    expect(crawlPageTab(new URLSearchParams("tab=crawl&input_type=jobs"))).toBe("crawl");
  });
});
