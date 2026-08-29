/**
 * `/admin/technologies/:slug`: the loader's resolve-name-then-fan-out order
 * (and its 404 on an unknown slug), plus the detail view's rendering -- full
 * metadata, the rollup-empty "not computed yet" state, and the live Swedish
 * companies list with its per-company technology links.
 */
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { TechnologyDetailView } from "~/components/admin/technology-detail";
import type {
  SeCompanyUsingTechnology,
  TechnologyAdoption,
  TechnologyDetail,
} from "~/lib/technologies.server";

const loadTechnologyDetail = vi.fn(
  async (): Promise<TechnologyDetail | null> => null,
);
const loadTechnologyAdoption = vi.fn(
  async (): Promise<TechnologyAdoption | null> => null,
);
const loadSeCompaniesUsingTechnology = vi.fn(
  async (): Promise<{ rows: SeCompanyUsingTechnology[]; error: string }> => ({
    rows: [],
    error: "",
  }),
);

vi.mock("~/lib/technologies.server", () => ({
  loadTechnologyDetail,
  loadTechnologyAdoption,
  loadSeCompaniesUsingTechnology,
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

function get(slug: string) {
  return loader({
    request: new Request(`http://backoffice/admin/technologies/${slug}`),
    params: { slug },
  } as unknown as Parameters<typeof loader>[0]);
}

describe("admin-technology-detail loader", () => {
  beforeEach(() => {
    loadTechnologyDetail.mockReset();
    loadTechnologyDetail.mockResolvedValue(null);
    loadTechnologyAdoption.mockClear();
    loadSeCompaniesUsingTechnology.mockClear();
  });

  it("404s an unknown slug via the standard not-found Response, before any heavy read", async () => {
    await expect(get("no-such-slug")).rejects.toSatisfy(
      (thrown) => thrown instanceof Response && thrown.status === 404,
    );
    expect(loadTechnologyDetail).toHaveBeenCalledWith("no-such-slug");
    expect(loadTechnologyAdoption).not.toHaveBeenCalled();
    expect(loadSeCompaniesUsingTechnology).not.toHaveBeenCalled();
  });

  it("resolves the exact detector NAME from the catalog and hands it (not the slug) to both follow-up reads", async () => {
    loadTechnologyDetail.mockResolvedValue(detail({}));
    const result = await get("wordpress");

    expect(loadTechnologyAdoption).toHaveBeenCalledWith("WordPress");
    expect(loadSeCompaniesUsingTechnology).toHaveBeenCalledWith("WordPress");
    expect(result).toEqual({
      technology: detail({}),
      adoption: null,
      companies: [],
      companiesError: "",
    });
  });
});

function render(
  technology: TechnologyDetail,
  adoption: TechnologyAdoption | null,
  companies: SeCompanyUsingTechnology[],
): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <TechnologyDetailView
            technology={technology}
            adoption={adoption}
            companies={companies}
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
    const html = render(detail({}), null, []);

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
      [],
    );
    expect(html).toContain("123,456");
    expect(html).toContain("2026-08-24 03:00:00");
    expect(html).not.toContain("not computed yet");
  });

  it("says the rollup has not computed yet while it is empty", () => {
    const html = render(detail({}), null, []);
    expect(html).toContain("Adoption not computed yet");
  });

  it("lists Swedish companies with links into each company's admin technology area", () => {
    const html = render(detail({}), null, [
      {
        company_id: "5560125220",
        root_domain: "example.se",
        legal_name: "Example AB",
      },
      { company_id: "5565200028", root_domain: "nameless.se", legal_name: "" },
    ]);

    expect(html).toContain('href="/admin/se/company/5560125220/technology"');
    expect(html).toContain("Example AB");
    expect(html).toContain("example.se");
    // A company se_companies has no name for still links by its org number.
    expect(html).toContain('href="/admin/se/company/5565200028/technology"');
    expect(html).toContain(">5565200028<");
  });

  it("says so when no Swedish domain carries a detection", () => {
    const html = render(detail({}), null, []);
    expect(html).toContain("No Swedish company domain has a detection");
  });

  it("degrades to a section notice when the live lookup failed, keeping the rest of the page", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <TechnologyDetailView
              technology={detail({})}
              adoption={null}
              companies={[]}
              companiesError="Timeout error."
            />
          ),
        },
      ],
      { initialEntries: ["/admin/technologies/wordpress"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("Live lookup unavailable");
    expect(html).toContain("WordPress");
    expect(html).toContain("Adoption not computed yet");
    expect(html).not.toContain("No Swedish company domain has a detection");
  });
});
