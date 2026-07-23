import { describe, expect, it } from "vitest";
import { parseCompanyListRequest } from "~/lib/company-list.server";

describe("parseCompanyListRequest", () => {
  it("preserves global company-list filters and request controls", () => {
    const request = new Request(
      "http://test/companies?q=bank&page=3&pageSize=25&sort=revenue&dir=desc&f_country=no&f_industry=62",
    );
    expect(parseCompanyListRequest(request)).toMatchObject({
      q: "bank",
      page: 3,
      pageSize: 25,
      sort: "revenue",
      dir: "desc",
      filters: { country: ["no"], industry: ["62"] },
      queryFilters: { country: ["no"], industry: ["62"] },
    });
  });

  it("locks the route country while preserving all other filters", () => {
    const request = new Request(
      "http://test/countries/se/companies?q=tech&page=2&pageSize=100&sort=name&dir=asc&f_country=br&f_country=no&f_industry=6201&f_legal_form=AB",
    );
    expect(parseCompanyListRequest(request, "se")).toMatchObject({
      q: "tech",
      page: 2,
      pageSize: 100,
      sort: "name",
      dir: "asc",
      filters: { industry: ["6201"], legal_form: ["AB"] },
      queryFilters: { country: ["se"], industry: ["6201"], legal_form: ["AB"] },
    });
  });
});
