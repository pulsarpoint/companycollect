/**
 * `/admin/technologies/:slug`: the loader's resolve-name-then-fan-out order
 * (404 on an unknown slug, only the ACTIVE tab's rollup queried), plus the
 * detail view's rendering -- full metadata, the "not computed yet" states,
 * the Domains tab ordered by harmonic centrality, and the Companies tab with
 * its country filter, per-country links and NACE industry badges.
 */
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  TechnologyDetailView,
  type TechnologyAdoptionTabData,
} from "~/components/admin/technology-detail";
import type {
  TechnologyAdoption,
  TechnologyCompanyRow,
  TechnologyDetail,
  TechnologyDomainRow,
} from "~/lib/technologies.server";

const loadTechnologyDetail = vi.fn(
  async (): Promise<TechnologyDetail | null> => null,
);
const loadTechnologyAdoption = vi.fn(
  async (): Promise<TechnologyAdoption | null> => null,
);
const loadTechnologyDomainsPage = vi.fn(
  async (): Promise<TechnologyDomainRow[]> => [],
);
const countTechnologyDomains = vi.fn(async (): Promise<number> => 0);
const loadTechnologyDomainsComputedAt = vi.fn(
  async (): Promise<string | null> => null,
);
const loadTechnologyCompaniesPage = vi.fn(
  async (): Promise<TechnologyCompanyRow[]> => [],
);
const countTechnologyCompanies = vi.fn(async (): Promise<number> => 0);
const loadTechnologyCompanyCountries = vi.fn(
  async (): Promise<string[]> => [],
);
const loadTechnologyCompaniesComputedAt = vi.fn(
  async (): Promise<string | null> => null,
);

vi.mock("~/lib/technologies.server", () => ({
  loadTechnologyDetail,
  loadTechnologyAdoption,
  loadTechnologyDomainsPage,
  countTechnologyDomains,
  loadTechnologyDomainsComputedAt,
  loadTechnologyCompaniesPage,
  countTechnologyCompanies,
  loadTechnologyCompanyCountries,
  loadTechnologyCompaniesComputedAt,
}));

const { loader } = await import("~/routes/admin-technology-detail");

function detail(overrides: Partial<TechnologyDetail>): TechnologyDetail {
  return {
    technology: "WordPress",
    slug: "wordpress",
    description: "The web's most common CMS.",
    website: "https://wordpress.org",
    categories: ["CMS", "Blogs"],
    icon: true,
    saas: false,
    oss: true,
    pricing: ["freemium", "onetime"],
    source: "wappalyzer",
    source_version: "2026.08",
    updated_at: "2026-08-20 00:00:00",
    ...overrides,
  };
}

function get(slug: string, search = "") {
  return loader({
    request: new Request(`http://backoffice/admin/technologies/${slug}${search}`),
    params: { slug },
  } as unknown as Parameters<typeof loader>[0]);
}

describe("admin-technology-detail loader", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    loadTechnologyDetail.mockResolvedValue(null);
    loadTechnologyAdoption.mockResolvedValue(null);
    loadTechnologyDomainsPage.mockResolvedValue([]);
    countTechnologyDomains.mockResolvedValue(0);
    loadTechnologyDomainsComputedAt.mockResolvedValue(null);
    loadTechnologyCompaniesPage.mockResolvedValue([]);
    countTechnologyCompanies.mockResolvedValue(0);
    loadTechnologyCompanyCountries.mockResolvedValue([]);
    loadTechnologyCompaniesComputedAt.mockResolvedValue(null);
  });

  it("404s an unknown slug via the standard not-found Response, before any rollup read", async () => {
    await expect(get("no-such-slug")).rejects.toSatisfy(
      (thrown) => thrown instanceof Response && thrown.status === 404,
    );
    expect(loadTechnologyDetail).toHaveBeenCalledWith("no-such-slug");
    expect(loadTechnologyAdoption).not.toHaveBeenCalled();
    expect(loadTechnologyDomainsPage).not.toHaveBeenCalled();
    expect(loadTechnologyCompaniesPage).not.toHaveBeenCalled();
  });

  it("defaults to the Domains tab, hands the exact detector NAME (not the slug) to the rollup reads, and leaves the Companies rollup untouched", async () => {
    loadTechnologyDetail.mockResolvedValue(detail({}));
    const result = await get("wordpress");

    expect(loadTechnologyAdoption).toHaveBeenCalledWith("WordPress");
    expect(loadTechnologyDomainsPage).toHaveBeenCalledWith("WordPress", 1, 50);
    expect(countTechnologyDomains).toHaveBeenCalledWith("WordPress");
    expect(loadTechnologyDomainsComputedAt).toHaveBeenCalledWith("WordPress");
    expect(loadTechnologyCompaniesPage).not.toHaveBeenCalled();
    expect(countTechnologyCompanies).not.toHaveBeenCalled();
    expect(result).toEqual({
      technology: detail({}),
      adoption: null,
      tab: { tab: "domains", rows: [], total: 0, computedAt: null },
      country: "",
      view: { page: 1, pageSize: 50 },
    });
  });

  it("loads the Companies tab with the URL's country filter and paging, and leaves the Domains rollup untouched", async () => {
    loadTechnologyDetail.mockResolvedValue(detail({}));
    loadTechnologyCompanyCountries.mockResolvedValue(["NO", "SE"]);
    loadTechnologyCompaniesComputedAt.mockResolvedValue("2026-08-24 03:00:00");
    const result = await get("wordpress", "?tab=companies&country=se&page=2");

    // ?country=se normalizes to the stored uppercase code.
    expect(loadTechnologyCompaniesPage).toHaveBeenCalledWith("WordPress", "SE", 2, 50);
    expect(countTechnologyCompanies).toHaveBeenCalledWith("WordPress", "SE");
    expect(loadTechnologyCompanyCountries).toHaveBeenCalledWith("WordPress");
    expect(loadTechnologyDomainsPage).not.toHaveBeenCalled();
    expect(countTechnologyDomains).not.toHaveBeenCalled();
    expect(result).toEqual({
      technology: detail({}),
      adoption: null,
      tab: {
        tab: "companies",
        rows: [],
        total: 0,
        countries: ["NO", "SE"],
        computedAt: "2026-08-24 03:00:00",
      },
      country: "SE",
      view: { page: 2, pageSize: 50 },
    });
  });
});

const DOMAINS_EMPTY: TechnologyAdoptionTabData = {
  tab: "domains",
  rows: [],
  total: 0,
  computedAt: null,
};

function render(
  technology: TechnologyDetail,
  adoption: TechnologyAdoption | null,
  tab: TechnologyAdoptionTabData,
  country = "",
): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <TechnologyDetailView
            technology={technology}
            adoption={adoption}
            tab={tab}
            country={country}
            view={{ page: 1, pageSize: 50 }}
          />
        ),
      },
    ],
    { initialEntries: [`/admin/technologies/${technology.slug}`] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("technology detail view", () => {
  it("renders the full catalog record: icon, name, description, website, categories, pricing, source and updated_at", () => {
    const html = render(detail({}), null, DOMAINS_EMPTY);

    expect(html).toContain("WordPress");
    expect(html).toContain('src="/icons/tech/wordpress"');
    expect(html).toContain("The web&#x27;s most common CMS.");
    expect(html).toContain('href="https://wordpress.org"');
    expect(html).toContain("wordpress.org");
    expect(html).toContain(">CMS<");
    expect(html).toContain(">Blogs<");
    expect(html).toContain(">OSS<");
    expect(html).not.toContain(">SaaS<");
    expect(html).toContain(">freemium<");
    expect(html).toContain(">onetime<");
    expect(html).toContain("wappalyzer");
    expect(html).toContain("2026.08");
    expect(html).toContain("2026-08-20 00:00:00");
  });

  it("shows the rollup's count and timestamp when adoption is computed", () => {
    const html = render(
      detail({}),
      { domainCount: 123456, computedAt: "2026-08-24 03:00:00" },
      DOMAINS_EMPTY,
    );
    expect(html).toContain("123,456");
    expect(html).toContain("2026-08-24 03:00:00");
    expect(html).not.toContain("Adoption not computed yet");
  });

  it("says the adoption rollup has not computed yet while it is empty", () => {
    const html = render(detail({}), null, DOMAINS_EMPTY);
    expect(html).toContain("Adoption not computed yet");
  });
});

describe("adoption tabs", () => {
  it("links both tabs, with the default Domains link carrying no ?tab= param", () => {
    const html = render(detail({}), null, DOMAINS_EMPTY);

    expect(html).toContain(">Domains<");
    expect(html).toContain(">Companies<");
    expect(html).toContain('href="/admin/technologies/wordpress?tab=companies"');
    // Switching back to the default tab must not carry a stale ?tab=.
    expect(html).not.toContain("tab=domains");
  });
});

describe("domains tab", () => {
  it("renders rank, an outbound domain link, and the formatted harmonic centrality, in the server's centrality order", () => {
    const html = render(detail({}), null, {
      tab: "domains",
      rows: [
        {
          root_domain: "example.com",
          harmonic_rank: "1",
          harmonic_centrality: 21534096.5,
        },
        {
          root_domain: "second.example",
          harmonic_rank: "2",
          harmonic_centrality: 987654.25,
        },
      ],
      total: 2,
      computedAt: "2026-08-24 03:00:00",
    });

    expect(html).toContain('href="https://example.com"');
    expect(html).toContain('target="_blank"');
    expect(html).toContain("21,534,096.5");
    expect(html).toContain('href="https://second.example"');
    expect(html).toContain("987,654.25");
    // The rollup orders by centrality desc; the table keeps that order.
    expect(html.indexOf("example.com")).toBeLessThan(
      html.indexOf("second.example"),
    );
    expect(html).toContain("Computed weekly");
    expect(html).toContain("2026-08-24 03:00:00");
  });

  it("shows an honest 'not computed yet' while the top-domains rollup is mid first population", () => {
    const html = render(detail({}), null, DOMAINS_EMPTY);
    expect(html).toContain("weekly top-domains rollup has no rows");
    expect(html).not.toContain("Computed weekly");
  });
});

describe("companies tab", () => {
  const COMPUTED: Omit<
    Extract<TechnologyAdoptionTabData, { tab: "companies" }>,
    "rows" | "total"
  > = {
    tab: "companies",
    countries: ["NO", "SE"],
    computedAt: "2026-08-24 03:00:00",
  };

  it("links SE companies into the admin technology area and other countries to the public company page, with country badges", () => {
    const html = render(detail({}), null, {
      ...COMPUTED,
      rows: [
        {
          country_code: "SE",
          company_id: "5560125220",
          root_domain: "example.se",
          legal_name: "Example AB",
          industries: [],
        },
        {
          country_code: "NO",
          company_id: "923609016",
          root_domain: "example.no",
          legal_name: "",
          industries: [],
        },
      ],
      total: 2,
    });

    expect(html).toContain('href="/admin/se/company/5560125220/technology"');
    expect(html).toContain("Example AB");
    expect(html).toContain(">SE<");
    expect(html).toContain("example.se");
    // No name source for the country yet: the id stands in, the public
    // company page link (country lowercased) is already correct.
    expect(html).toContain('href="/company/no/923609016"');
    expect(html).toContain(">923609016<");
    expect(html).toContain(">NO<");
    expect(html).toContain("Computed weekly");
  });

  it("caps the NACE industry badges and collapses the rest into a +N overflow badge", () => {
    const industries = [
      { code: "62.010", label: "Computer programming", is_primary: 1 as const },
      { code: "62.020", label: "Computer consultancy", is_primary: 0 as const },
      { code: "63.110", label: "Data processing", is_primary: 0 as const },
      { code: "63.120", label: "Web portals", is_primary: 0 as const },
      { code: "70.220", label: "Business consultancy", is_primary: 0 as const },
      { code: "73.110", label: "Advertising agencies", is_primary: 0 as const },
    ];
    const html = render(detail({}), null, {
      ...COMPUTED,
      rows: [
        {
          country_code: "SE",
          company_id: "5560125220",
          root_domain: "example.se",
          legal_name: "Example AB",
          industries,
        },
      ],
      total: 1,
    });

    expect(html).toContain("62.010 Computer programming");
    expect(html).toContain("63.120 Web portals");
    // The 5th and 6th collapse into "+2", their titles preserved.
    expect(html).toContain(">+2<");
    expect(html).not.toContain(">70.220 Business consultancy<");
    expect(html).toContain("70.220 Business consultancy, 73.110 Advertising agencies");
  });

  it("shows the applied country in the filter and an empty state scoped to it", () => {
    const html = render(
      detail({}),
      null,
      { ...COMPUTED, rows: [], total: 0 },
      "SE",
    );
    expect(html).toContain("Country");
    expect(html).toContain(
      "No SE company domain carries a detection of this technology.",
    );
  });

  it("shows an honest 'not computed yet' while the companies rollup is mid first population", () => {
    const html = render(detail({}), null, {
      tab: "companies",
      rows: [],
      total: 0,
      countries: [],
      computedAt: null,
    });
    expect(html).toContain("weekly companies rollup has no rows");
    expect(html).not.toContain("Computed weekly");
  });
});
