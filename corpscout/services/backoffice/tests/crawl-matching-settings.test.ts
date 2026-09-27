import { describe, expect, it } from "vitest";
import { objectSettings, parseCrawlSettings } from "~/lib/crawl-settings.server";

const settings = {challenge_agent_model: "deepseek-flash", challenge_agent_max_runs: 3, llm_profile_id: "selected", max_pages: 1, max_model_calls: 20, page_selection: "basic_info"};
describe("company matching crawl settings", () => {
  it("keeps request table settings unless explicitly overridden", () => {
    expect(parseCrawlSettings(objectSettings(settings), "site_info")).not.toHaveProperty("match_company");
    expect(parseCrawlSettings(objectSettings({...settings, match_company: "saved"}), "site_info")).not.toHaveProperty("match_company");
  });
  it("can enable or disable matching for both crawl types", () => {
    for (const type of ["site_info", "full"] as const) {
      const collection = {...settings, max_pages: type === "full" ? 20 : 1, page_selection: type === "full" ? "saved" : "basic_info"};
      expect(parseCrawlSettings(objectSettings({...collection, match_company: true}), type)).toMatchObject({match_company: true, company_country: "SE", skip_company_matching_if_mapped: true});
      expect(parseCrawlSettings(objectSettings({...collection, match_company: false}), type)).toMatchObject({match_company: false});
      expect(parseCrawlSettings(objectSettings({...collection, match_company: true, skip_company_matching_if_mapped: false}), type)).toMatchObject({skip_company_matching_if_mapped: false});
    }
    expect(() => parseCrawlSettings(objectSettings({...settings, match_company: true, company_country: "NO"}), "site_info")).toThrow("Sweden");
  });
});
