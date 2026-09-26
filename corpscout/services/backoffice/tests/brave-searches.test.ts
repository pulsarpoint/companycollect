import { beforeEach, describe, expect, it, vi } from "vitest";
import { braveSearchPreview } from "~/lib/brave-searches";
import { listBraveSearches, removeBraveSearch, resolveBraveSearch, saveBraveSearch, validateBraveSearch } from "~/lib/brave-searches.server";

const db = vi.hoisted(() => ({query: vi.fn()}));
vi.mock("~/lib/llm-control.server", () => ({llmControl: () => db}));
const task = "11111111-1111-4111-8111-111111111111";
const search = {searchId: "54d90187-85d5-45dc-9603-cd7c4a7d31d1", name: "Official website", queryType: "official_website", queryTemplate: "Find the official website of {company_name}.", revision: 1};
beforeEach(() => db.query.mockReset());

describe("Brave search templates", () => {
  it("supports company placeholders and literal braces with a matching preview", () => {
    const input = validateBraveSearch("  Website  ", "  Find {company_name} ({company_id}, {country_code})\r\n{{website}}  ");
    expect(input).toEqual({name: "Website", queryTemplate: "Find {company_name} ({company_id}, {country_code})\n{{website}}"});
    expect(braveSearchPreview(input.queryTemplate)).toBe("Find Example AB (5560123456, SE)\n{website}");
  });
  it.each(["Find websites", "{{company_name}}", "{company_name.__class__}", "{company_name!r}", "{company_name:20}", "Find {company_name} {unknown}", "Find {company_id} }"])("rejects an unusable question: %s", template => {
    expect(() => validateBraveSearch("Website", template)).toThrow();
  });
  it("inserts new searches with their own query type", async () => {
    db.query.mockResolvedValue({rowCount: 1});
    await saveBraveSearch({searchId: "", revision: 0, name: "Jobs", queryTemplate: "Find jobs at {company_name}."});
    const [sql, [id, name, type, question]] = db.query.mock.calls[0];
    expect(sql).toContain("INSERT INTO processing.brave_searches");
    expect([name, type, question]).toEqual(["Jobs", `search_${id.replaceAll("-", "")}`, "Find jobs at {company_name}."]);
  });
  it("rejects stale updates and duplicate active names", async () => {
    db.query.mockResolvedValueOnce({rowCount: 0});
    await expect(saveBraveSearch(search)).rejects.toThrow("changed or removed");
    expect(db.query.mock.calls[0][0]).toContain("revision=revision+1");
    db.query.mockRejectedValueOnce({code: "23505"});
    await expect(saveBraveSearch(search)).rejects.toThrow("already exists");
  });
  it("archives removed searches and excludes them from new choices", async () => {
    db.query.mockResolvedValueOnce({rowCount: 1}).mockResolvedValueOnce({rows: []});
    await removeBraveSearch(search.searchId, 1);
    expect(db.query.mock.calls[0][0]).toContain("SET archived_at=now()");
    expect(await listBraveSearches()).toEqual([]);
    expect(db.query.mock.calls[1][0]).toContain("archived_at IS NULL");
  });
});

describe("queue search resolution", () => {
  it("resolves the chosen version into a runtime snapshot", async () => {
    db.query.mockResolvedValueOnce({rows: []}).mockResolvedValueOnce({rows: [search]});
    expect(await resolveBraveSearch(task, search.searchId, 1)).toEqual({query_type: search.queryType, query_template: search.queryTemplate, search_id: search.searchId, search_name: search.name, search_revision: 1});
  });
  it("requires reviewing edits and rejects removed choices", async () => {
    db.query.mockResolvedValueOnce({rows: []}).mockResolvedValueOnce({rows: [{...search, revision: 2}]});
    await expect(resolveBraveSearch(task, search.searchId, 1)).rejects.toThrow("has changed");
    db.query.mockResolvedValueOnce({rows: []}).mockResolvedValueOnce({rows: []});
    await expect(resolveBraveSearch(task, search.searchId, 1)).rejects.toThrow("removed");
  });
  it("resumes the original question without consulting current or removed settings", async () => {
    const frozen = {query_type: search.queryType, query_template: search.queryTemplate, search_id: search.searchId, search_name: search.name, search_revision: 1};
    db.query.mockResolvedValue({rows: [{search: frozen}]});
    expect(await resolveBraveSearch(task, "saved", 1)).toEqual(frozen);
    expect(db.query).toHaveBeenCalledOnce();
    expect(db.query.mock.calls[0][0]).not.toContain("llm");
    await expect(resolveBraveSearch(task, search.searchId, 2)).rejects.toThrow("already started");
  });
  it("preserves the question on tasks started before saved searches existed", async () => {
    const frozen = {query_type: "official_website", query_template: "Original question for {company_name}"};
    db.query.mockResolvedValue({rows: [{search: frozen}]});
    expect(await resolveBraveSearch(task, "saved", 0)).toEqual(frozen);
  });
});
