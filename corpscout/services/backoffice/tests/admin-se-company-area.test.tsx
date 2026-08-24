import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

// One hoisted ClickHouse mock covers every `.server` module reachable from the
// route modules imported below, so the loaders can be exercised end-to-end
// (loader -> component) without a live database.
const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import AdminSwedenCompanyLayout, {
  shouldRevalidate,
} from "~/routes/admin-se-company-layout";
import { loader as companyIndexLoader } from "~/routes/admin-se-company-index";
import { SeCompanyHeader } from "~/components/admin/se-company-header";
import { SeCompanyAddressTab } from "~/components/admin/se-company-address";
import { SeCompanyDomainsTab } from "~/components/admin/se-company-domains";
import { SeCompanyFinancialTab } from "~/components/admin/se-company-financial";
import { SeCompanyPeopleTab } from "~/components/admin/se-company-people";
import {
  loadSeCompanyAddresses,
  type SeCompanyAddressCorrectionRow,
  type SeCompanyAddressRow,
} from "~/lib/se-company-address.server";
import type { SeCompanyDomainRow } from "~/lib/se-company-domains.server";
import type { SeCompanyFinancialDetail } from "~/lib/se-company-financial.server";
import type { SeCompanyPersonRow } from "~/lib/se-company-people.server";
import type { SeCompanyShell } from "~/lib/se-company-shell.server";
import {
  SE_COMPANY_TABS,
  seCompanyTabFromPath,
  seCompanyTabPath,
  type SeCompanyTab,
} from "~/lib/se-company-tabs";

const COMPANY_ID = "5560125220";

const shell: SeCompanyShell = {
  company_id: COMPANY_ID,
  legal_name: "Beijer Byggmaterial Aktiebolag",
  legal_form_code: "AB-ORGFO",
  legal_form_label_en: "Limited company (aktiebolag)",
  legal_form_label_sv: "Aktiebolag",
  status: "active",
  incorporation_date: "1915-04-06",
  published: true,
  entity_type_label: "Company",
  is_public_sector: false,
};

/** Every tab component renders links, so each one needs a Router. */
function render(element: React.ReactNode, pathname: string): string {
  const router = createMemoryRouter([{ path: "*", element }], {
    initialEntries: [pathname],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

/** The opening `<a ...>` tag whose href is exactly `href`, so per-link
 * attribute assertions (aria-selected, data-active) stay scoped. */
function anchorWithHref(html: string, href: string): string {
  for (const part of html.split("<a ")) {
    const end = part.indexOf(">");
    const tag = end === -1 ? part : part.slice(0, end);
    if (tag.includes(`href="${href}"`)) return tag;
  }
  throw new Error(`no <a> with href ${href}`);
}

beforeEach(() => {
  clickhouse.query.mockReset();
  clickhouse.query.mockResolvedValue([]);
});

describe("se company tab paths", () => {
  it("reads the tab off the URL and falls back to info", () => {
    for (const tab of SE_COMPANY_TABS) {
      expect(seCompanyTabFromPath(seCompanyTabPath(COMPANY_ID, tab.value))).toBe(
        tab.value,
      );
    }
    // The area index redirects to /info, so a bare company path is Info.
    expect(seCompanyTabFromPath(`/admin/se/company/${COMPANY_ID}`)).toBe("info");
    expect(seCompanyTabFromPath(`/admin/se/company/${COMPANY_ID}/nope`)).toBe(
      "info",
    );
  });
});

describe("company area header", () => {
  it("names the company once, with its badges and both company-level links", () => {
    const html = render(
      <SeCompanyHeader shell={shell} tab="info" />,
      seCompanyTabPath(COMPANY_ID, "info"),
    );
    expect(html).toContain("Beijer Byggmaterial Aktiebolag");
    expect(html).toContain(COMPANY_ID);
    expect(html).toContain(">active<");
    // The legal form reads as its OFFICIAL Swedish name with the English gloss
    // beside it; the code itself is the badge's tooltip, since AB-ORGFO says
    // nothing on its own.
    expect(html).toContain('title="AB-ORGFO"');
    expect(html).toContain(">Aktiebolag<");
    expect(html).toContain(">Limited company (aktiebolag)<");
    expect(html).not.toContain(">AB-ORGFO<");
    expect(html).toContain(">Company<");
    expect(html).toContain("registered 1915-04-06");
    // Both links moved off the Info page in Task 18: they are about the
    // company, not about its description.
    expect(html).toContain(`href="/company/se/${COMPANY_ID}"`);
    expect(html).toContain(
      `href="/admin/se/company-info/corrections?companyId=${COMPANY_ID}"`,
    );
  });

  it("renders all five tabs and marks exactly the active one", () => {
    for (const active of SE_COMPANY_TABS) {
      const html = render(
        <SeCompanyHeader shell={shell} tab={active.value} />,
        seCompanyTabPath(COMPANY_ID, active.value),
      );
      for (const tab of SE_COMPANY_TABS) {
        const anchor = anchorWithHref(
          html,
          seCompanyTabPath(COMPANY_ID, tab.value),
        );
        expect(anchor, `${active.value} / ${tab.value}`).toContain(
          `aria-selected="${tab.value === active.value}"`,
        );
      }
      expect(html).toContain(active.label);
    }
  });

  it("says so when the company is in the register but not published yet", () => {
    const html = render(
      <SeCompanyHeader shell={{ ...shell, published: false }} tab="info" />,
      seCompanyTabPath(COMPANY_ID, "info"),
    );
    expect(html).toContain("Not published yet");
    expect(html).toContain("se_company_info");
    // The header still names the company: the register knows who it is.
    expect(html).toContain("Beijer Byggmaterial Aktiebolag");
  });
});

const address: SeCompanyAddressRow = {
  address_key: "f".repeat(64),
  address_type: "postal",
  care_of: "Nicklas",
  street_address: "Borgargatan 16, lgh 1302",
  normalized_address: "borgargatan 16 lgh 1302|11734|stockholm|se",
  postal_code: "11734",
  city: "STOCKHOLM",
  country_code: "SE",
  address_id: "9".repeat(64),
  latitude: "59.3167337",
  longitude: "18.0347148",
  geocode_status: "matched_exact",
  geocoded_at: "2026-08-17 20:14:13.671",
  sources: ["bolagsverket", "scb"],
  source_record_uids: ["bolagsverket:5560125220:postal"],
  evidence_set_hash: "a".repeat(64),
  correction_ids: [],
  resolved_at: "2026-08-24 09:12:00.000",
};

const correction: SeCompanyAddressCorrectionRow = {
  correction_id: "11111111-1111-4111-8111-111111111111",
  correction_kind: "reject_address",
  payload: JSON.stringify({ address_key: "e".repeat(64) }),
  address_key: "e".repeat(64),
  evidence_hash: "b".repeat(64),
  reason: "This is the accountant's address, not the company's.",
  decided_by: "backoffice",
  supersedes_correction_id: null,
  created_at: "2026-08-24 08:00:00.000",
  is_current: 1,
  is_stale: 0,
  is_applied: 1,
};

const emptyDetail = { addresses: [], removed: [], corrections: [] };

describe("address tab", () => {
  it("shows one card per published address, its sources, and its geocode with a map link", () => {
    const html = render(
      <SeCompanyAddressTab
        detail={{
          ...emptyDetail,
          addresses: [
            address,
            {
              ...address,
              address_key: "e".repeat(64),
              address_type: "visiting_or_postal",
              sources: ["scb"],
              latitude: "",
              longitude: "",
              geocode_status: "",
              geocoded_at: "",
              address_id: "",
            },
          ],
        }}
      />,
      seCompanyTabPath(COMPANY_ID, "address"),
    );
    expect(html).toContain("Postal address");
    expect(html).toContain("Visiting or postal address");
    // One badge per contributing source, in the order the row lists them:
    // precedence between sources is a fact worth seeing.
    expect(html).toContain(">bolagsverket<");
    expect(html).toContain(">scb<");
    expect(html).toContain("matched_exact");
    expect(html).toContain("11734");
    expect(html).toContain("STOCKHOLM");
    // A geocoded point is checkable on a map; one that never reached the
    // geocoder says so rather than showing a grid of em dashes or a link to 0,0.
    expect(html).toContain(
      'href="https://www.openstreetmap.org/?mlat=59.3167337&amp;mlon=18.0347148#map=18/59.3167337/18.0347148"',
    );
    expect(html.match(/openstreetmap\.org\/\?mlat/g)).toHaveLength(1);
    expect(html).toContain("This address has not been geocoded.");
    expect(html).not.toContain("1970");
  });

  it("says so when no source recorded an address", () => {
    const html = render(
      <SeCompanyAddressTab detail={emptyDetail} />,
      seCompanyTabPath(COMPANY_ID, "address"),
    );
    expect(html).toContain("No address recorded");
  });

  /**
   * Ruling A8: a rejected address is published is_current = false, and a page
   * that only listed live rows would hide it -- taking the correction that
   * rejected it, and the undo that would bring it back, with it.
   */
  it("keeps a rejected address visible in its own section, with the decision that removed it", () => {
    const html = render(
      <SeCompanyAddressTab
        detail={{
          addresses: [address],
          removed: [
            {
              ...address,
              address_key: "e".repeat(64),
              address_type: "visiting",
              correction_ids: [correction.correction_id],
            },
          ],
          corrections: [correction],
        }}
      />,
      seCompanyTabPath(COMPANY_ID, "address"),
    );
    expect(html).toContain("Removed / rejected");
    expect(html).toContain("Visiting address");
    expect(html).toContain("This is the accountant&#x27;s address, not the company&#x27;s.");
    expect(html).toContain(">applied<");
  });

  /**
   * Ruling A11: Dagster has no row to stamp a reject that names a key this
   * company does not publish, so it never appears in any row's correction_ids.
   * It is applied all the same -- and it still needs somewhere to be seen.
   */
  it("shows a correction whose address is gone rather than dropping it", () => {
    const html = render(
      <SeCompanyAddressTab
        detail={{ addresses: [address], removed: [], corrections: [correction] }}
      />,
      seCompanyTabPath(COMPANY_ID, "address"),
    );
    expect(html).toContain("Corrections without an address");
    expect(html).toContain(">applied<");
    expect(html).not.toContain(">pending<");
  });

  it("renders what the loader actually returns for a company with one address", async () => {
    clickhouse.query
      .mockResolvedValueOnce([address])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    const detail = await loadSeCompanyAddresses(COMPANY_ID);
    const html = render(
      <SeCompanyAddressTab detail={detail} />,
      seCompanyTabPath(COMPANY_ID, "address"),
    );
    expect(html).toContain("Postal address");
    expect(html).toContain("11734");
    expect(html).not.toContain("Removed / rejected");
  });
});

const financials: SeCompanyFinancialDetail = {
  latest: {
    fiscal_year: "2025",
    period_end_date: "2025-12-31",
    currency: "SEK",
    revenue_amount_original: "72852346",
    revenue_amount_usd: "7910318.028908",
    net_result_amount_original: "7219201",
    net_result_amount_usd: "783861.865266",
    total_assets_amount_original: "26884241",
    total_assets_amount_usd: "2919094.688808",
    equity_amount_original: "10823518",
    equity_amount_usd: "1175219.114723",
    employees: "18",
    years_count: "3",
    resolved_at: "2026-08-23 04:30:46.925",
  },
  sources: [
    {
      source_id: "bolagsverket-annual-accounts",
      view: "se_financials_bolagsverket_current",
      years: [
        {
          source_id: "bolagsverket-annual-accounts",
          accounting_scope: "standalone",
          source_document_id: "f132e16d",
          fiscal_year: "2025",
          report_period_start: "2025-01-01",
          report_period_end: "2025-12-31",
          currency: "SEK",
          revenue_amount_original: "72852346",
          operating_result_amount_original: "9117124",
          net_result_amount_original: "7219201",
          total_assets_amount_original: "26884241",
          equity_amount_original: "10823518",
          liabilities_amount_original: "16060723",
          cash_and_bank_amount_original: "6070053",
          current_assets_amount_original: "26788181",
          current_liabilities_amount_original: "14032036",
          personnel_expenses_amount_original: "15697959",
          wages_and_salaries_amount_original: "",
          employees: "18",
          revenue_amount_usd: "7910318.028908",
          net_result_amount_usd: "783861.865266",
          observation: "filed",
          source_fact_count: "172",
          mapped_fact_count: "14",
          mapping_version: "sweden-bolagsverket-observations-metrics-v3",
          fx_rate_to_usd: "0.108580141385",
          fx_rate_date: "2025-12-31",
          fx_source: "ECB EXR",
          source_url: "",
          viewer_url: "",
        },
      ],
    },
    { source_id: "esef", view: "se_financials_esef_current", years: [] },
  ],
  reports: [
    {
      source_slug: "sweden_financial",
      statement_key: "618cc95990c5",
      source_record_uid: "cb658443f00c",
      fiscal_year: "2025",
      report_period_start: "2025-01-01",
      report_period_end: "2025-12-31",
      reported_company_name: "Strive Stories AB",
      report_language: "",
      taxonomy_entrypoint: "http://xbrl.taxonomier.se/se/fr/ar/rar/2020-12-01/se-ar-rar-2020-12-01.xsd",
      source_archive_name: "25_9.zip",
      nested_zip_name: "5592990765_2025-12-31.zip",
      xhtml_object_key: "sweden_financial/report_xhtml/…",
      xhtml_sha256: "81b92c09",
      facts_count: "18",
      contexts_count: "1",
      units_count: "0",
      parser_version: "sweden-financial-ixbrl-v1",
      resolved_at: "2026-08-08 06:43:03.907",
    },
  ],
};

describe("financial tab", () => {
  it("shows the served row, the per-source years and the filed reports", () => {
    const html = render(
      <SeCompanyFinancialTab companyId={COMPANY_ID} detail={financials} />,
      seCompanyTabPath(COMPANY_ID, "financial"),
    );
    expect(html).toContain("Latest figures");
    expect(html).toContain("Bolagsverket annual accounts");
    expect(html).toContain("Filed reports");
    // A source with no year is not shown as an empty table.
    expect(html).not.toContain("ESEF consolidated IFRS");
    // Amounts are grouped but never rescaled: a reviewer checks them against a
    // filing, and the currency is whatever was stored.
    expect(html).toContain("72,852,346");
    expect(html).toContain(">SEK<");
    expect(html).toContain("tabular-nums");
    expect(html).toContain("filed");
    expect(html).toContain("Strive Stories AB");
    // The year links through to the tagged facts for that year.
    expect(html).toContain(`href="/company/se/${COMPANY_ID}/facts/2025"`);
  });

  it("says so when nothing has been filed, parsed or resolved", () => {
    const html = render(
      <SeCompanyFinancialTab
        companyId={COMPANY_ID}
        detail={{
          latest: null,
          sources: financials.sources.map((source) => ({
            ...source,
            years: [],
          })),
          reports: [],
        }}
      />,
      seCompanyTabPath(COMPANY_ID, "financial"),
    );
    expect(html).toContain("No financials recorded");
  });
});

const people: SeCompanyPersonRow[] = [
  {
    person_id: "43234b7d-0184-16b5-de47-dc086a2b0ed9",
    name: "Jens Lapidus",
    description: "",
    draft_count: 7,
    correction_count: 0,
    merged_into_person_id: "",
    updated_at: "2026-08-19 21:59:49.204",
    roles: [
      {
        person_id: "43234b7d-0184-16b5-de47-dc086a2b0ed9",
        role_code: "chief_executive_officer",
        role_label: "Chief executive officer",
        role_group: "executive",
        fiscal_year: "2025",
        sources: ["bolagsverket"],
        source_count: 1,
        is_current: 1,
        first_observed_at: "2026-08-18 12:46:49.000",
        last_observed_at: "2026-08-18 12:46:49.000",
      },
    ],
  },
  {
    person_id: "9b59d268-821c-acd8-1db7-166c6579cb02",
    name: "Thomas Kullman",
    description: "",
    draft_count: 6,
    correction_count: 0,
    merged_into_person_id: "",
    updated_at: "2026-08-19 21:59:49.204",
    roles: [],
  },
];

describe("people tab", () => {
  it("links every person to the existing per-company person review page", () => {
    const html = render(
      <SeCompanyPeopleTab companyId={COMPANY_ID} people={people} />,
      seCompanyTabPath(COMPANY_ID, "people"),
    );
    expect(html).toContain("Jens Lapidus");
    expect(html).toContain(
      `href="/admin/se/people/person/${COMPANY_ID}/43234b7d-0184-16b5-de47-dc086a2b0ed9"`,
    );
    // The catalog's wording, the year it was observed for and who observed it.
    expect(html).toContain("Chief executive officer 2025 · bolagsverket");
    expect(html).toContain("2 people · 1 without a role");
  });

  it("says so when Dagster has published no people", () => {
    const html = render(
      <SeCompanyPeopleTab companyId={COMPANY_ID} people={[]} />,
      seCompanyTabPath(COMPANY_ID, "people"),
    );
    expect(html).toContain("No people recorded");
  });
});

const domain: SeCompanyDomainRow = {
  root_domain: "beijerbygg.se",
  website_url: "https://www.beijerbygg.se",
  website_host: "beijerbygg.se",
  source_names: ["wikidata"],
  source_confidences: [1],
  source_urls: ["http://www.wikidata.org/entity/Q10427772"],
  confidence_bases: ["official_website_claim"],
  suggested_confidence: 1,
  suggested_primary: 1,
  review_status: "unreviewed",
  review_note: "",
  reviewed_by: "",
  reviewed_at: "",
  is_active: 1,
  first_seen_at: "2026-07-26 19:57:22.983",
  last_seen_at: "2026-07-26 19:57:22.983",
  resolved_at: "2026-08-19 14:04:35.263",
};

describe("domains tab", () => {
  it("shows each domain with its review state and zipped source evidence", () => {
    const html = render(
      <SeCompanyDomainsTab companyId={COMPANY_ID} domains={[domain]} />,
      seCompanyTabPath(COMPANY_ID, "domains"),
    );
    expect(html).toContain("beijerbygg.se");
    expect(html).toContain(">primary<");
    expect(html).toContain(">unreviewed<");
    expect(html).toContain("confidence 100%");
    expect(html).toContain(">wikidata<");
    expect(html).toContain("official_website_claim");
    expect(html).toContain("http://www.wikidata.org/entity/Q10427772");
    expect(html).toContain(
      `href="/countries/se/domain-suggestions?q=${COMPANY_ID}"`,
    );
  });

  it("says so when no source suggested a domain, and still offers the queue", () => {
    const html = render(
      <SeCompanyDomainsTab companyId={COMPANY_ID} domains={[]} />,
      seCompanyTabPath(COMPANY_ID, "domains"),
    );
    expect(html).toContain("No domains recorded");
    expect(html).toContain(
      `href="/countries/se/domain-suggestions?q=${COMPANY_ID}"`,
    );
  });
});

describe("tab labels", () => {
  it("is exactly Info, Address, Financial, People, Domains, in that order", () => {
    expect(SE_COMPANY_TABS.map((tab) => tab.label)).toEqual([
      "Info",
      "Address",
      "Financial",
      "People",
      "Domains",
    ]);
    const values: SeCompanyTab[] = SE_COMPANY_TABS.map((tab) => tab.value);
    expect(values).toEqual([
      "info",
      "address",
      "financial",
      "people",
      "domains",
    ]);
  });
});

/**
 * The layout is a route component, so its props are React Router's generated
 * `Route.ComponentProps` (loaderData, params, matches, ...). A test only ever
 * drives the two the component reads, so the rest is cast away here once
 * rather than at each call.
 */
type LayoutProps = Parameters<typeof AdminSwedenCompanyLayout>[0];

function layoutProps(
  loadedShell: SeCompanyShell | null,
  companyId: string,
): LayoutProps {
  return {
    loaderData: { shell: loadedShell },
    params: { companyId },
  } as unknown as LayoutProps;
}

describe("company area routes", () => {
  it("redirects a bare company path to Info", async () => {
    // The loader always throws, so the thrown Response IS the contract.
    let thrown: unknown;
    try {
      companyIndexLoader({ params: { companyId: COMPANY_ID } } as never);
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(Response);
    const response = thrown as Response;
    expect(response.status).toBe(302);
    expect(response.headers.get("Location")).toBe(
      `/admin/se/company/${COMPANY_ID}/info`,
    );
  });

  it("re-reads the shell only when the company changes", () => {
    // Tab switches keep :companyId, so the header must not be re-fetched.
    expect(
      shouldRevalidate({
        currentParams: { companyId: COMPANY_ID },
        nextParams: { companyId: COMPANY_ID },
      }),
    ).toBe(false);
    expect(
      shouldRevalidate({
        currentParams: { companyId: COMPANY_ID },
        nextParams: { companyId: "5592990765" },
      }),
    ).toBe(true);
  });

  it("renders the not-found view instead of the outlet for an unknown id", () => {
    const html = renderToStaticMarkup(
      <RouterProvider
        router={createMemoryRouter(
          [
            {
              path: "/admin/se/company/:companyId",
              element: (
                <AdminSwedenCompanyLayout
                  {...layoutProps(null, "0000000000")}
                />
              ),
              children: [{ path: "info", element: <p>TAB CONTENT</p> }],
            },
          ],
          { initialEntries: ["/admin/se/company/0000000000/info"] },
        )}
      />,
    );
    // An id in neither table never resolves itself, so it must not be told to
    // wait for the next Dagster run.
    expect(html).toContain("No company with this id in the register");
    expect(html).not.toContain("Not published yet");
    expect(html).not.toContain("TAB CONTENT");
    // No header and no sub-menu either: there is no company to head.
    expect(html).not.toContain("<h1");
    expect(html).not.toContain('role="tab"');
  });

  it("renders the header and the outlet for a company that resolves", () => {
    const html = renderToStaticMarkup(
      <RouterProvider
        router={createMemoryRouter(
          [
            {
              path: "/admin/se/company/:companyId",
              element: (
                <AdminSwedenCompanyLayout
                  {...layoutProps(shell, COMPANY_ID)}
                />
              ),
              children: [{ path: "info", element: <p>TAB CONTENT</p> }],
            },
          ],
          { initialEntries: [seCompanyTabPath(COMPANY_ID, "info")] },
        )}
      />,
    );
    expect(html).toContain("Beijer Byggmaterial Aktiebolag");
    expect(html).toContain("TAB CONTENT");
    expect(html).not.toContain("No company with this id in the register");
  });
});

describe("company area header legal form", () => {
  it("falls back to the bare code when the dictionary does not name the form", () => {
    // A register code the curation has not caught up with must still be
    // readable -- an empty badge would be worse than the code.
    const html = render(
      <SeCompanyHeader
        shell={{ ...shell, legal_form_label_sv: "", legal_form_label_en: "" }}
        tab="info"
      />,
      seCompanyTabPath(COMPANY_ID, "info"),
    );
    expect(html).toContain(">AB-ORGFO<");
  });

  it("shows no legal-form badge at all when the register recorded no code", () => {
    const html = render(
      <SeCompanyHeader
        shell={{
          ...shell,
          legal_form_code: "",
          legal_form_label_sv: "",
          legal_form_label_en: "",
        }}
        tab="info"
      />,
      seCompanyTabPath(COMPANY_ID, "info"),
    );
    // The badge is gone, tooltip and all -- the legal NAME still ends in
    // "Aktiebolag", so the absence is asserted on the code's own tooltip.
    expect(html).not.toContain('title="AB-ORGFO"');
    expect(html).not.toContain(">Limited company (aktiebolag)<");
  });
});
