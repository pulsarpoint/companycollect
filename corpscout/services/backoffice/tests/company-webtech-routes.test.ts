import { beforeEach, expect, it, vi } from "vitest";

const queries = vi.hoisted(() => ({ getCompanyDomains: vi.fn() }));
const webtech = vi.hoisted(() => ({ getDomainWebtech: vi.fn() }));
vi.mock("~/lib/queries.server", () => queries);
vi.mock("~/lib/webtech.server", () => webtech);
const admin =
  await import("~/routes/admin-se-company-technology-web-technologies");
const company = await import("~/routes/company-technology-web-technologies");

beforeEach(() => {
  vi.resetAllMocks();
  queries.getCompanyDomains.mockResolvedValue([
    { domain: "primary.se", is_primary: 1 },
    { domain: "related.se", is_primary: 0 },
  ]);
});

function args(domain: string) {
  return {
    params: { companyId: "123", id: "123", country: "se" },
    request: new Request(
      `http://localhost/technology/web-technologies?domain=${domain}`,
    ),
  } as never;
}

it.each([admin.loader, company.loader])(
  "uses the selected associated domain and rejects unrelated domain overrides",
  async (loader) => {
    await loader(args("RELATED.SE"));
    expect(webtech.getDomainWebtech).toHaveBeenLastCalledWith("related.se");
    await loader(args("unrelated.se"));
    expect(webtech.getDomainWebtech).toHaveBeenLastCalledWith("primary.se");
  },
);

it("keeps company pages without domains consistent with the existing routes", async () => {
  queries.getCompanyDomains.mockResolvedValue([]);
  expect(await admin.loader(args(""))).toBeNull();
  await expect(company.loader(args(""))).rejects.toMatchObject({ status: 404 });
  expect(webtech.getDomainWebtech).not.toHaveBeenCalled();
});
