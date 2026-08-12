import { describe, expect, it } from "vitest";
import {
  COMPANY_FLAG_SOURCES,
  availableCompanyFlags,
  flagFilterKey,
} from "~/lib/company-flags";
import { getCountry } from "~/lib/countries";
import { parseFilters } from "~/lib/filters";

describe("Sweden technical-information filter", () => {
  it("offers the domain flag only when a registry-domain match query exists", () => {
    expect(availableCompanyFlags("se").map((flag) => flag.id)).toContain(
      "domain",
    );
    expect(COMPANY_FLAG_SOURCES.se.domain).toHaveProperty("idQuery");
  });

  it("offers a distinct flag for unreviewed unified domains", () => {
    expect(availableCompanyFlags("se").map((flag) => flag.id)).toContain(
      "domain_suggestion",
    );
    expect(COMPANY_FLAG_SOURCES.se.domain_suggestion).toEqual({
      idQuery: expect.stringContaining("company_domains FINAL"),
    });
    expect(
      (COMPANY_FLAG_SOURCES.se.domain_suggestion as { idQuery: string })
        .idQuery,
    ).toContain("review_status = 'unreviewed'");
  });

  it("accepts the shareable domain filter URL for Sweden", () => {
    const country = getCountry("se")!;
    const filters = parseFilters(
      new URLSearchParams(
        `${flagFilterKey("domain")}=ignored&f_flag_domain=yes`,
      ),
      country,
    );

    expect(filters).toEqual({ flag_domain: ["yes"] });
  });

  it("accepts the shareable domain-suggestion filter URL for Sweden", () => {
    const country = getCountry("se")!;
    const filters = parseFilters(
      new URLSearchParams("f_flag_domain_suggestion=yes"),
      country,
    );

    expect(filters).toEqual({ flag_domain_suggestion: ["yes"] });
  });
});
