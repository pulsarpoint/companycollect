import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

// One hoisted ClickHouse mock covers every `.server` module reachable from the
// route modules imported below, so the loaders can be exercised end-to-end
// (loader -> component) without a live database.
const clickhouse = vi.hoisted(() => ({ query: vi.fn(), insertAddressCorrections: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: clickhouse.query,
  // Mocking the module replaces every export, so the writer the address
  // module imports has to be here too -- otherwise it is silently undefined
  // for any test that ever reaches an append.
  chInsertSeCompanyAddressCorrections: clickhouse.insertAddressCorrections,
}));

import AdminSwedenCompanyLayout, {
  shouldRevalidate,
} from "~/routes/admin-se-company-layout";
import { loader as companyIndexLoader } from "~/routes/admin-se-company-index";
import { SeCompanyHeader } from "~/components/admin/se-company-header";
import { SeCompanyAddressTab } from "~/components/admin/se-company-address";
import { SeCompanyContractsTab } from "~/components/admin/se-company-contracts";
import { SeCompanyDomainsTab } from "~/components/admin/se-company-domains";
import { SeCompanyJobsTab } from "~/components/admin/se-company-jobs";
import { SeCompanyListedTab } from "~/components/admin/se-company-listed";
import { SeCompanyPeopleTab } from "~/components/admin/se-company-people";
import { SeFinancialsView } from "~/components/financials/se-financials-view";
import AdminSwedenCompanyTechnology from "~/routes/admin-se-company-technology";
import AdminSwedenCompanyTechnologyLayout from "~/routes/admin-se-company-technology-layout";
import AdminSwedenCompanyTechnologyInfrastructure from "~/routes/admin-se-company-technology-infrastructure";
import AdminSwedenCompanyTechnologyIpAddress from "~/routes/admin-se-company-technology-ip-address";
import AdminSwedenCompanyTechnologyIpAddresses from "~/routes/admin-se-company-technology-ip-addresses";
import AdminSwedenCompanyTechnologyWebIntelligence from "~/routes/admin-se-company-technology-web-intelligence";
import { buildWebTechnologyHistory } from "~/lib/web-technology-history";
import {
  loadSeCompanyAddresses,
  type SeCompanyAddressCorrectionRow,
  type SeCompanyAddressRow,
} from "~/lib/se-company-address.server";
import type {
  CompanyFinancialSource,
  CompanyTechnologyDetail,
  CompanyTechnologyIpInventory,
  DomainRow,
  FinancialSourceYearRow,
  PublicContractRow,
} from "~/lib/queries.server";
import type {
  SeCompanyJobAdDetail,
  SeCompanyJobRow,
} from "~/lib/se-company-jobs.server";
import type { SeCompanyListed } from "~/lib/se-company-listed.server";
import type { SeCompanyDomainRow } from "~/lib/se-company-domains.server";
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

  it("keeps Technology active on every nested technology sub-path", () => {
    const base = `/admin/se/company/${COMPANY_ID}/technology`;
    expect(seCompanyTabFromPath(base)).toBe("technology");
    expect(seCompanyTabFromPath(`${base}/web-intelligence`)).toBe("technology");
    expect(seCompanyTabFromPath(`${base}/infrastructure`)).toBe("technology");
    expect(seCompanyTabFromPath(`${base}/ip-addresses`)).toBe("technology");
    expect(seCompanyTabFromPath(`${base}/ip-addresses/193.235.32.10`)).toBe(
      "technology",
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

  it("renders all nine tabs and marks exactly the active one", () => {
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
        result={null}
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
      <SeCompanyAddressTab detail={emptyDetail} result={null} />,
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
        result={null}
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
        result={null}
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
      <SeCompanyAddressTab detail={detail} result={null} />,
      seCompanyTabPath(COMPANY_ID, "address"),
    );
    expect(html).toContain("Postal address");
    expect(html).toContain("11734");
    expect(html).not.toContain("Removed / rejected");
  });
});

const financialYear: FinancialSourceYearRow = {
  source_id: "bolagsverket-annual-accounts",
  accounting_scope: "standalone",
  source_document_id: "f132e16d",
  source_record_uids: [],
  source_url: "",
  viewer_url: "",
  fiscal_year: "2025",
  currency: "SEK",
  report_period_start: "2025-01-01",
  report_period_end: "2025-12-31",
  revenue_amount_original: 72852346,
  revenue_amount_usd: 7910318,
  operating_result_amount_original: 9117124,
  operating_result_amount_usd: null,
  net_result_amount_original: 7219201,
  net_result_amount_usd: 783861,
  total_assets_amount_original: 26884241,
  total_assets_amount_usd: 2919094,
  equity_amount_original: 10823518,
  equity_amount_usd: 1175219,
  employees: 18,
  source_fact_count: 172,
  mapped_fact_count: 14,
  observation: "filed",
  fx_rate_to_usd: 0.108580141385,
  fx_rate_date: "2025-12-31",
  fx_source: "ECB EXR",
};

const registrySource: CompanyFinancialSource = {
  id: "bolagsverket-annual-accounts",
  kind: "registry",
  title: "Bolagsverket annual accounts",
  description:
    "Standardized figures from the legal entity's digitally filed annual reports.",
  yearFacts: true,
  financials: [financialYear],
  documents: [
    {
      documentId: "doc-1",
      filingYear: 2025,
      sourceFileName: "arsredovisning-2025.pdf",
      sourceUrl: "https://example.invalid/arsredovisning-2025.pdf",
      factCount: 18,
      pageCount: 12,
      nativeTextPageCount: 12,
      ocrPageCount: 0,
      pdfSizeBytes: 100000,
      parseStatus: "loaded",
      parseWarnings: "",
      retrievedAt: "2026-08-08 06:43:03.907",
      resolvedAt: "2026-08-08 06:43:03.907",
      hasReportMetadata: true,
    },
  ],
};

const esefSource: CompanyFinancialSource = {
  id: "esef",
  kind: "esef",
  title: "ESEF consolidated IFRS",
  description:
    "Standardized group figures from filed ESEF annual financial reports.",
  financials: [
    {
      ...financialYear,
      source_id: "esef",
      accounting_scope: "consolidated",
      source_document_id: "esef-doc-9",
    },
  ],
};

describe("financial tab", () => {
  // The tab IS the public financials experience: the shared SeFinancialsView
  // the public /company/se/:id/financials page renders, deep-linking into the
  // public facts and report readers rather than duplicating them.
  const basePath = `/company/se/${COMPANY_ID}/financials`;
  const factsBase = `/company/se/${COMPANY_ID}/facts`;

  it("renders the source switcher and the selected registry source's overview", () => {
    const html = render(
      <SeFinancialsView
        financialSources={[registrySource, esefSource]}
        filingStatus={null}
        basePath={basePath}
        factsBase={factsBase}
      />,
      seCompanyTabPath(COMPANY_ID, "financial"),
    );
    // The switcher names both sources; the first one is the selected overview.
    expect(html).toContain("Financial source");
    expect(html).toContain(">Bolagsverket<");
    expect(html).toContain(">ESEF<");
    expect(html).toContain("Financial overview");
    expect(html).toContain("Financial year 2025");
    // Facts links point at the PUBLIC facts reader.
    expect(html).toContain(`href="${factsBase}/2025"`);
    // The registry source carries its document table, deep-linking to the
    // public report reader under the public financials base path.
    expect(html).toContain("Source documents");
    expect(html).toContain("arsredovisning-2025.pdf");
    expect(html).toContain(`href="${basePath}/doc-1"`);
  });

  it("points an ESEF source's year facts at the public ESEF document reader", () => {
    const html = render(
      <SeFinancialsView
        financialSources={[esefSource]}
        filingStatus={null}
        basePath={basePath}
        factsBase={factsBase}
      />,
      seCompanyTabPath(COMPANY_ID, "financial"),
    );
    expect(html).toContain(`href="${basePath}/esef/esef-doc-9"`);
    // An ESEF source never links to the registry facts pages.
    expect(html).not.toContain(`href="${factsBase}/2025"`);
  });

  it("says so when no source has anything filed", () => {
    const html = render(
      <SeFinancialsView
        financialSources={[]}
        filingStatus={null}
        basePath={basePath}
        factsBase={factsBase}
      />,
      seCompanyTabPath(COMPANY_ID, "financial"),
    );
    expect(html).toContain(
      "No digitally filed annual report found in our sources.",
    );
  });

  it("names the filing status when the register says the report is missing", () => {
    const html = render(
      <SeFinancialsView
        financialSources={[]}
        filingStatus={{
          status: "not_submitted",
          reportPeriodEnd: null,
          filingRegisteredOn: null,
          sourceFileFormat: null,
          bolagsverketDocumentId: null,
          sourceSlug: null,
          observedAt: null,
        }}
        basePath={basePath}
        factsBase={factsBase}
      />,
      seCompanyTabPath(COMPANY_ID, "financial"),
    );
    expect(html).toContain("Annual report not submitted.");
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
      <SeCompanyPeopleTab companyId={COMPANY_ID} people={people} evidence={[]} />,
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

  it("says so when neither sources nor Dagster know any people", () => {
    const html = render(
      <SeCompanyPeopleTab companyId={COMPANY_ID} people={[]} evidence={[]} />,
      seCompanyTabPath(COMPANY_ID, "people"),
    );
    expect(html).toContain("No people recorded");
  });

  it("shows verbatim source evidence with original roles even before any publish", () => {
    const html = render(
      <SeCompanyPeopleTab
        companyId={COMPANY_ID}
        people={[]}
        evidence={[
          {
            full_name: "Jens Lapidus",
            sources: ["bolagsverket", "esef"],
            entries: [
              {
                source: "bolagsverket",
                full_name: "Jens Lapidus",
                role: "Verkställande direktör",
                source_role_code: "ceo",
                mapped_role_label: "Chief executive officer",
                period: "2024",
              },
              {
                source: "esef",
                full_name: "Jens Lapidus",
                role: "Member of the Audit Committee",
                source_role_code: "other",
                mapped_role_label: "",
                period: "",
              },
            ],
          },
        ]}
      />,
      seCompanyTabPath(COMPANY_ID, "people"),
    );
    expect(html).toContain("1 person found in sources");
    expect(html).toContain("2 observations");
    // The source's own wording stays verbatim...
    expect(html).toContain("Verkställande direktör");
    expect(html).toContain("Member of the Audit Committee");
    // ...with our canonical mapping beside it when the static maps know the
    // source role code, and an em dash when they do not.
    expect(html).toContain("Chief executive officer");
    expect(html).toContain("2024");
    expect(html).toContain("seen by 2 sources");
    expect(html).toContain("bolagsverket");
    expect(html).toContain("esef");
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

/** The PUBLIC page's domain shape -- the technology tab reuses the public
 * technology detail, not the admin domain review rows. */
const techDomain: DomainRow = {
  domain: "beijerbygg.se",
  website_url: "https://www.beijerbygg.se",
  domain_source: "wikidata",
  confidence: 1,
  is_primary: 1,
  source_names: ["wikidata"],
  review_status: "confirmed_primary",
};

/** Route components take React Router's generated `Route.ComponentProps`; a
 * test drives only loaderData (and params where the component reads them). */
function routeProps<Component extends (props: never) => unknown>(
  loaderData: unknown,
): Parameters<Component>[0] {
  return {
    loaderData,
    params: { companyId: COMPANY_ID },
  } as unknown as Parameters<Component>[0];
}

const technologyProps = (detail: CompanyTechnologyDetail) =>
  routeProps<typeof AdminSwedenCompanyTechnology>(detail);

// The admin base path every technology sub-tab lives under since the area
// became a nested twin of the public one (no more public deep links).
const adminTechnologyBase = `/admin/se/company/${COMPANY_ID}/technology`;

describe("technology area layout", () => {
  // The layout IS the public technology layout's admin twin: the same domain
  // selector and the same four section tabs, but on the admin base path.
  const layoutProps = (domains: DomainRow[], selectedDomain: string) =>
    routeProps<typeof AdminSwedenCompanyTechnologyLayout>({
      domains,
      selectedDomain,
    });

  it("renders the four section tabs with admin hrefs threading ?domain=", () => {
    const html = render(
      <AdminSwedenCompanyTechnologyLayout
        {...layoutProps([techDomain], techDomain.domain)}
      />,
      adminTechnologyBase,
    );
    expect(html).toContain("Associated domain");
    expect(html).toContain(
      `href="${adminTechnologyBase}?domain=beijerbygg.se"`,
    );
    expect(html).toContain(
      `href="${adminTechnologyBase}/web-intelligence?domain=beijerbygg.se"`,
    );
    expect(html).toContain(
      `href="${adminTechnologyBase}/infrastructure?domain=beijerbygg.se"`,
    );
    expect(html).toContain(
      `href="${adminTechnologyBase}/ip-addresses?domain=beijerbygg.se"`,
    );
    // Nothing points at the public reader any more: the sections moved in.
    expect(html).not.toContain(`href="/company/se/${COMPANY_ID}/technology`);
  });

  it("marks exactly the section the URL is on as active", () => {
    const sections = [
      { path: adminTechnologyBase, value: "overview" },
      { path: `${adminTechnologyBase}/web-intelligence`, value: "web-intelligence" },
      { path: `${adminTechnologyBase}/infrastructure`, value: "infrastructure" },
      { path: `${adminTechnologyBase}/ip-addresses`, value: "ip-addresses" },
      // The per-IP detail page still belongs to the IP addresses section.
      {
        path: `${adminTechnologyBase}/ip-addresses/193.235.32.10`,
        value: "ip-addresses",
      },
    ];
    const hrefs: Record<string, string> = {
      overview: `${adminTechnologyBase}?domain=beijerbygg.se`,
      "web-intelligence": `${adminTechnologyBase}/web-intelligence?domain=beijerbygg.se`,
      infrastructure: `${adminTechnologyBase}/infrastructure?domain=beijerbygg.se`,
      "ip-addresses": `${adminTechnologyBase}/ip-addresses?domain=beijerbygg.se`,
    };
    for (const active of sections) {
      const html = render(
        <AdminSwedenCompanyTechnologyLayout
          {...layoutProps([techDomain], techDomain.domain)}
        />,
        active.path,
      );
      for (const [section, href] of Object.entries(hrefs)) {
        expect(anchorWithHref(html, href), `${active.path} / ${section}`)
          .toContain(`aria-selected="${section === active.value}"`);
      }
    }
  });

  // The public layout 404s a company with no domains; the admin shell must
  // keep rendering so every sub-tab can say "nothing yet" instead.
  it("keeps the tab shell without a domain selector when no domain resolved", () => {
    const html = render(
      <AdminSwedenCompanyTechnologyLayout {...layoutProps([], "")} />,
      adminTechnologyBase,
    );
    expect(html).not.toContain("Associated domain");
    // The section tabs still render, without a dangling ?domain=.
    expect(html).toContain(`href="${adminTechnologyBase}/infrastructure"`);
    expect(html).toContain(`href="${adminTechnologyBase}/ip-addresses"`);
  });
});

describe("technology tab", () => {
  // The index IS the public technology overview: the same shared sections the
  // public /company/se/:id/technology index renders; the domain selector and
  // the section tabs live in the parent layout.
  it("renders the shared domains section", () => {
    const html = render(
      <AdminSwedenCompanyTechnology
        {...technologyProps({
          domains: [techDomain],
          selectedDomain: techDomain.domain,
          webTechnologyHistory: null,
          technologyCatalog: {},
        })}
      />,
      adminTechnologyBase,
    );
    expect(html).toContain("Web presence");
    expect(html).toContain("beijerbygg.se");
    expect(html).toContain(">Selected<");
    expect(html).toContain(">wikidata<");
  });

  it("renders the crawl-history section when the selected domain has detections", () => {
    const history = buildWebTechnologyHistory(
      techDomain.domain,
      [
        {
          crawlId: "CC-MAIN-2026-26",
          observedPages: 12,
          processedAt: "2026-07-01 10:00:00.000",
        },
      ],
      [
        {
          crawlId: "CC-MAIN-2026-26",
          name: "WordPress",
          categories: ["CMS"],
          versions: ["6.5"],
          confidence: 100,
          detectedPages: 8,
          sampleUrls: ["https://www.beijerbygg.se/"],
        },
      ],
    );
    const html = render(
      <AdminSwedenCompanyTechnology
        {...technologyProps({
          domains: [techDomain],
          selectedDomain: techDomain.domain,
          webTechnologyHistory: history,
          technologyCatalog: {},
        })}
      />,
      adminTechnologyBase,
    );
    expect(html).toContain("WordPress");
    expect(html).toContain("CC-MAIN-2026-26");
  });

  // The public page 404s on an empty result; the admin tab must not -- a
  // reviewer needs the tab to say "nothing yet", not vanish.
  it("shows an empty state instead of throwing when no domain is known", () => {
    const html = render(
      <AdminSwedenCompanyTechnology
        {...technologyProps({
          domains: [],
          selectedDomain: "",
          webTechnologyHistory: null,
          technologyCatalog: {},
        })}
      />,
      adminTechnologyBase,
    );
    expect(html).toContain("No domains resolved for this company");
    expect(html).not.toContain("Web presence");
  });
});

describe("technology sub-tabs", () => {
  // Each sub-tab mounts the SAME section component as its public twin; what
  // is admin-specific is only the honest empty state where the public route
  // throws a 404.
  it("says no domains resolved instead of 404ing, on every sub-tab", () => {
    for (const [name, element] of [
      [
        "web-intelligence",
        <AdminSwedenCompanyTechnologyWebIntelligence
          {...routeProps<typeof AdminSwedenCompanyTechnologyWebIntelligence>(
            null,
          )}
        />,
      ],
      [
        "infrastructure",
        <AdminSwedenCompanyTechnologyInfrastructure
          {...routeProps<typeof AdminSwedenCompanyTechnologyInfrastructure>(
            null,
          )}
        />,
      ],
      [
        "ip-addresses",
        <AdminSwedenCompanyTechnologyIpAddresses
          {...routeProps<typeof AdminSwedenCompanyTechnologyIpAddresses>(null)}
        />,
      ],
    ] as const) {
      const html = render(element, `${adminTechnologyBase}/${name}`);
      expect(html, name).toContain("No domains resolved for this company");
    }
  });

  it("says so on the per-IP page when nothing connects the address", () => {
    const html = render(
      <AdminSwedenCompanyTechnologyIpAddress
        {...routeProps<typeof AdminSwedenCompanyTechnologyIpAddress>(null)}
      />,
      `${adminTechnologyBase}/ip-addresses/193.235.32.10`,
    );
    expect(html).toContain(
      "No evidence connects this IP address to the company",
    );
  });

  it("links each inventory row to the per-IP page under the ADMIN base path", () => {
    const inventory: CompanyTechnologyIpInventory = {
      domain: techDomain.domain,
      page: 1,
      pageSize: 25,
      summary: {
        totalAddresses: 1,
        ipv4Addresses: 1,
        ipv6Addresses: 0,
        rdapRegisteredAddressesOnPage: 0,
      },
      addresses: [
        {
          ip: "193.235.32.10",
          version: 4,
          networkSegment: "193.235.32.0/24",
          firstSeen: "2025-01-01 00:00:00.000",
          lastSeen: "2026-08-01 00:00:00.000",
          countryCode: "SE",
          countryName: "Sweden",
          cityName: "Stockholm",
          asn: 3301,
          asnOrganization: "Telia Company AB",
          rdapRegistration: null,
          hostnames: ["www.beijerbygg.se"],
        },
      ],
    };
    const html = render(
      <AdminSwedenCompanyTechnologyIpAddresses
        {...routeProps<typeof AdminSwedenCompanyTechnologyIpAddresses>(
          inventory,
        )}
      />,
      `${adminTechnologyBase}/ip-addresses`,
    );
    // The section links via the CURRENT pathname, so the same shared
    // component deep-links within the admin area, not out to the public one.
    expect(html).toContain(
      `href="${adminTechnologyBase}/ip-addresses/193.235.32.10?domain=beijerbygg.se"`,
    );
  });
});

const contract: PublicContractRow = {
  source: "sweden_uhm_procurement",
  notice_ref: "2025/S 001-000001",
  contract_date: "2025-03-14",
  buyer_name: "Trafikverket",
  title: "Road maintenance, region north",
  amount_original: null,
  amount_usd: null,
  currency: "",
  notice_amount_original: 12500000,
  notice_amount_usd: 1250000,
  notice_currency: "SEK",
  source_url: "",
};

describe("contracts tab", () => {
  it("renders the shared public contracts section for exact-matched awards", () => {
    const html = render(
      <SeCompanyContractsTab contracts={[contract]} />,
      seCompanyTabPath(COMPANY_ID, "contracts"),
    );
    // The SAME section the public company page renders, not an admin re-take.
    expect(html).toContain("Government contracts");
    expect(html).toContain("Trafikverket");
    expect(html).toContain("Road maintenance, region north");
    expect(html).toContain(">sweden_uhm_procurement<");
    // UHM publishes no per-winner figure, so the notice total is labelled.
    expect(html).toContain("whole notice");
  });

  it("says so when no award names this company as a winner", () => {
    const html = render(
      <SeCompanyContractsTab contracts={[]} />,
      seCompanyTabPath(COMPANY_ID, "contracts"),
    );
    expect(html).toContain("No contract awards");
    expect(html).toContain(
      "No exact-matched government contract awards name this company as a winner",
    );
  });
});

const job: SeCompanyJobRow = {
  source_system: "platsbanken",
  source_job_ad_id: "29112166",
  interval_number: 1,
  active_from: "2026-05-04 08:00:00.000",
  active_to: "2026-06-30 21:59:59.000",
  active_to_basis: "application_deadline",
  is_end_estimated: 1,
  publication_at: "2026-05-04 08:00:00.000",
  application_deadline: "2026-06-30 21:59:59.000",
  employer_name: "Beijer Byggmaterial AB",
  headline_original: "Säljare till Beijer Bygg i Luleå",
  occupation_label: "Utesäljare",
  municipality_name: "Luleå",
  region_name: "Norrbottens län",
  employment_type_label: "Tillsvidareanställning",
  working_hours_label: "Heltid",
  number_of_vacancies: 1,
  webpage_url: "https://arbetsformedlingen.se/platsbanken/annonser/29112166",
  is_open: 0,
};

const adDetail: SeCompanyJobAdDetail = {
  source_job_ad_id: "29112166",
  headline_original: "Säljare till Beijer Bygg i Luleå",
  description_text_original: "Om rollen\n\nDu säljer byggmaterial i butik.",
  detected_language: "sv",
  webpage_url: "https://arbetsformedlingen.se/platsbanken/annonser/29112166",
  extras: {
    salary_type_label: "Fast månads- vecko- eller timlön",
    salary_description: "Fast lön + provision",
    scope_min: 50,
    scope_max: 100,
    experience_required: 1,
    driving_license_required: 1,
    access_to_own_car: null,
    employer_workplace: "Beijer Luleå",
    street_address: "Storgatan 1",
    postcode: "97231",
    city: "Luleå",
    application_email: "jobb@beijer.se",
    application_url: "",
    application_information: "",
  },
  requirements: [
    { requirement_level: "must_have", requirement_type: "work_experience", label_original: "Säljare, dagligvaror", weight: 10 },
    { requirement_level: "must_have", requirement_type: "driving_license", label_original: "B", weight: 10 },
    { requirement_level: "nice_to_have", requirement_type: "language", label_original: "Engelska", weight: null },
  ],
  contacts: [
    { contact_index: 0, name: "Anna Ek", description: "", email: "anna.ek@beijer.se", telephone: "+46 70 000 00 00", contact_type: "Rekryterande chef" },
  ],
};

describe("jobs tab", () => {
  it("shows headline, active period with estimated end, deadline and source", () => {
    const html = render(
      <SeCompanyJobsTab
        jobs={[
          job,
          { ...job, source_job_ad_id: "29112167", active_to: "", is_open: 1 },
        ]}
      />,
      seCompanyTabPath(COMPANY_ID, "jobs"),
    );
    expect(html).toContain("Säljare till Beijer Bygg i Luleå");
    expect(html).toContain("2026-05-04");
    expect(html).toContain("2026-06-30");
    // An estimated end is marked, not passed off as stated by the source.
    expect(html).toContain("(est.)");
    expect(html).toContain(">platsbanken<");
    // The open interval wears the badge and says it has no recorded end.
    expect(html).toContain(">open<");
    expect(html).toContain("open-ended");
    expect(html).toContain("2 ad intervals · 1 currently open");
  });

  it("shows occupation with type/hours, location with region tooltip, and the ad links", () => {
    const html = render(
      <SeCompanyJobsTab jobs={[job]} />,
      seCompanyTabPath(COMPANY_ID, "jobs"),
    );
    expect(html).toContain("Utesäljare");
    // Employment type and working hours share one muted secondary line.
    expect(html).toContain("Tillsvidareanställning · Heltid");
    expect(html).toContain("Luleå");
    expect(html).toContain('title="Norrbottens län"');
    // The headline selects the ad via ?ad= (resolved against the tab path),
    // the icon opens Platsbanken.
    expect(html).toContain('?ad=29112166"');
    expect(html).toContain(
      'href="https://arbetsformedlingen.se/platsbanken/annonser/29112166"',
    );
    // One vacancy is the norm, so no badge for it.
    expect(html).not.toContain("1 vacancies");
  });

  it("wears a vacancies badge only when an ad seeks more than one person", () => {
    const html = render(
      <SeCompanyJobsTab jobs={[{ ...job, number_of_vacancies: 5 }]} />,
      seCompanyTabPath(COMPANY_ID, "jobs"),
    );
    expect(html).toContain("5 vacancies");
  });

  it("renders the ?ad= detail card: facts, grouped requirements, contacts, text", () => {
    const path = seCompanyTabPath(COMPANY_ID, "jobs");
    const html = render(
      <SeCompanyJobsTab jobs={[job]} adDetail={adDetail} />,
      `${path}?ad=29112166`,
    );
    // Latest-version facts.
    expect(html).toContain(
      "Fast månads- vecko- eller timlön · Fast lön + provision",
    );
    expect(html).toContain("50–100%");
    expect(html).toContain("Beijer Luleå");
    expect(html).toContain("Storgatan 1, 97231 Luleå");
    expect(html).toContain("jobb@beijer.se");
    // Requirements grouped by level, each with its type badge.
    expect(html).toContain("Must have");
    expect(html).toContain("Nice to have");
    expect(html).toContain("Säljare, dagligvaror");
    expect(html).toContain(">experience<");
    expect(html).toContain(">driving licence<");
    expect(html).toContain("Engelska");
    // Contacts with their free-text role.
    expect(html).toContain("Anna Ek");
    expect(html).toContain("Rekryterande chef");
    expect(html).toContain("mailto:anna.ek@beijer.se");
    // The ad text renders as readable text, and the close link drops ?ad=.
    expect(html).toContain("Du säljer byggmaterial i butik.");
    expect(html).toContain("whitespace-pre-line");
    expect(html).toContain(`href="${path}"`);
    // The selected row is marked.
    expect(html).toContain('data-state="selected"');
  });

  it("renders the plain list when the ?ad= id was not this company's (detail null)", () => {
    const html = render(
      <SeCompanyJobsTab jobs={[job]} adDetail={null} />,
      `${seCompanyTabPath(COMPANY_ID, "jobs")}?ad=999999`,
    );
    expect(html).toContain("Säljare till Beijer Bygg i Luleå");
    expect(html).not.toContain("Must have");
    expect(html).not.toContain('data-state="selected"');
  });

  it("says the description-only detail is missing its raw version when extras are null", () => {
    const html = render(
      <SeCompanyJobsTab
        jobs={[job]}
        adDetail={{ ...adDetail, extras: null, requirements: [], contacts: [] }}
      />,
      `${seCompanyTabPath(COMPANY_ID, "jobs")}?ad=29112166`,
    );
    expect(html).toContain("No raw ad version found");
    expect(html).toContain("Du säljer byggmaterial i butik.");
    expect(html).not.toContain("Must have");
  });

  // The pipeline HAS landed data (21k companies), so an empty tab means this
  // company was not matched -- the copy must not blame the pipeline anymore.
  it("says no ads matched this company when the table is empty", () => {
    const html = render(
      <SeCompanyJobsTab jobs={[]} />,
      seCompanyTabPath(COMPANY_ID, "jobs"),
    );
    expect(html).toContain("No job ads recorded for this company");
    // JSX collapses the source's line breaks to single spaces.
    expect(html).toContain(
      "neither the historical archives nor the live JobStream feed matched any to this one",
    );
  });
});

/** Two traded years, newest first — summaries[0] doubles as the headline
 * summary. */
const SUMMARIES_FIXTURE: SeCompanyListed["summaries"] = [
  {
    year: 2025,
    venues: 4,
    lead_venue: "ST",
    lead_currency: "SEK",
    last_close: 122.15,
    last_day: "2025-12-30",
    traded_usd: 31_500_000_000,
  },
  {
    year: 2024,
    venues: 4,
    lead_venue: "ST",
    lead_currency: "SEK",
    last_close: 101.4,
    last_day: "2024-12-30",
    traded_usd: 18_500_000_000,
  },
];

/** Handelsbanken-shaped: a Stockholm home line plus an LSE cross-listing, the
 * quote led by the Stockholm line. */
const listedTraded: SeCompanyListed = {
  leis: [
    {
      lei: "NHBDILHZTYCNBV5UYZ31",
      entity_status: "ACTIVE",
      registration_status: "ISSUED",
    },
  ],
  symbols: [
    // The cross-listing carries the enrichment worth testing: its own quote
    // currency, a non-common instrument type, and a delisting flag.
    {
      isin: "SE0007100599",
      eodhd_symbol_key: "0R7S.LSE",
      ticker: "0R7S",
      exchange_code: "LSE",
      symbol_name: "Svenska Handelsbanken AB",
      instrument_type: "Preferred Stock",
      quote_currency: "GBP",
      is_delisted: 1,
    },
    {
      isin: "SE0007100599",
      eodhd_symbol_key: "SHB-A.ST",
      ticker: "SHB-A",
      exchange_code: "ST",
      symbol_name: "Svenska Handelsbanken AB (publ)",
      instrument_type: "Common Stock",
      quote_currency: "SEK",
      is_delisted: 0,
    },
  ],
  summary: {
    year: 2025,
    venues: 4,
    lead_venue: "ST",
    lead_currency: "SEK",
    last_close: 122.15,
    last_day: "2025-12-30",
    traded_usd: 31_500_000_000,
  },
  summaries: SUMMARIES_FIXTURE,
  leadSymbolKey: "SHB-A.ST",
  chartSymbolKey: "SHB-A.ST",
  prices: [
    { price_date: "2025-09-01", close: 118.4, high: 119.2, low: 117.5, adjusted_close: 117.9, volume: 4_800_000 },
    { price_date: "2025-12-30", close: 122.15, high: 122.9, low: 121.1, adjusted_close: 122.15, volume: 5_400_000 },
  ],
  // SHB-A.ST-shaped stats, precomputed by the loader — the component only
  // renders them.
  stats: {
    high52w: 149.7,
    low52w: 116.8,
    avgVolume: 5_170_000,
    returns: [
      { label: "1M", value: 0.021 },
      { label: "YTD", value: -0.034 },
      { label: "1Y", value: 0.089 },
      { label: "5Y", value: 0.42 },
    ],
  },
};

describe("listed tab", () => {
  it("charts the selected line and badges it, keeping the lead badge separate", () => {
    const html = render(
      <SeCompanyListedTab
        companyId={COMPANY_ID}
        listed={{
          ...listedTraded,
          chartSymbolKey: "0R7S.LSE",
        }}
      />,
      seCompanyTabPath(COMPANY_ID, "listed"),
    );
    // The chart card names the SELECTED line, not the lead.
    expect(html).toContain("0R7S · London Stock Exchange");
    expect(html).toContain(">charted<");
    // The lead badge stays on the lead row; ticker cells are selection links.
    expect(html).toContain(">lead<");
    expect(html).toContain('?line=SHB-A.ST');
    expect(html).toContain('?line=0R7S.LSE');
  });


  it("says publicly traded from the EODHD resolve, quotes the lead line, and charts a year of closes", () => {
    const html = render(
      <SeCompanyListedTab companyId={COMPANY_ID} listed={listedTraded} />,
      seCompanyTabPath(COMPANY_ID, "listed"),
    );
    expect(html).toContain("Publicly traded");
    expect(html).not.toContain("Not publicly traded");
    // The quote is the lead line's: ticker, venue, close in the lead
    // currency on its day, and the turnover figure labelled for what it is.
    expect(html).toContain("SHB-A");
    expect(html).toContain(">ST<");
    expect(html).toContain("122.15");
    expect(html).toContain("SEK");
    expect(html).toContain("2025-12-30");
    expect(html).toContain("Traded value");
    expect(html).toContain("$31.5B");
    // Turnover is NOT market capitalisation, and the label must never say so.
    expect(html).not.toContain("Market cap");
    // The chart rendered (ChartContainer's slot) for the lead symbol.
    expect(html).toContain('data-slot="chart"');
    // The stat strip: 52-week range in the lead currency, average volume,
    // and the four returns -- gains green, losses red, both theme-aware.
    expect(html).toContain("52-week range");
    expect(html).toContain("116.80 – 149.70 SEK");
    expect(html).toContain("Avg volume (1Y)");
    expect(html).toContain("5.17M");
    expect(html).toContain("+2.1%");
    expect(html).toContain("text-emerald-600");
    expect(html).toContain("-3.4%");
    expect(html).toContain("text-red-600");
    expect(html).toContain(">1M<");
    expect(html).toContain(">YTD<");
    // Both listed lines are rows: the LSE cross-listing is real, with ISIN.
    expect(html).toContain("0R7S");
    expect(html).toContain(">LSE<");
    expect(html).toContain("SE0007100599");
    // The enrichment from eodhd_symbols: the official name (lead line's also
    // sits in the verdict card), each line's own quote currency, a type
    // badge only when the instrument is not common stock, and the delisted
    // mark on the dead cross-listing.
    expect(html).toContain("Svenska Handelsbanken AB (publ)");
    expect(html).toContain(">SEK<");
    expect(html).toContain(">GBP<");
    expect(html).toContain(">Preferred Stock<");
    expect(html).not.toContain(">Common Stock<");
    expect(html).toContain(">delisted<");
    // The LEI still shows as identity context.
    expect(html).toContain("NHBDILHZTYCNBV5UYZ31");
    // ESEF is not trading information: no filings table, ever.
    expect(html).not.toContain("ESEF");
  });

  it("skips the chart and blanks the quote when the symbol has no prices or summary yet", () => {
    const html = render(
      <SeCompanyListedTab
        companyId={COMPANY_ID}
        listed={{
          ...listedTraded,
          summary: null,
          summaries: [],
          leadSymbolKey: "0R7S.LSE",
          chartSymbolKey: "0R7S.LSE",
          prices: [],
          stats: null,
        }}
      />,
      seCompanyTabPath(COMPANY_ID, "listed"),
    );
    // Still traded: the verdict is the symbol resolve, not the summary.
    expect(html).toContain("Publicly traded");
    expect(html).not.toContain('data-slot="chart"');
    // The summary lag is said out loud rather than rendered as zeros.
    expect(html).toContain("No market summary row yet");
    expect(html).not.toContain("$31.5B");
  });

  it("says not publicly traded when no EODHD symbol resolves, naming the detection", () => {
    const html = render(
      <SeCompanyListedTab
        companyId={COMPANY_ID}
        listed={{
          leis: listedTraded.leis,
          symbols: [],
          summary: null,
          summaries: [],
          leadSymbolKey: "",
          chartSymbolKey: "",
          prices: [],
          stats: null,
        }}
      />,
      seCompanyTabPath(COMPANY_ID, "listed"),
    );
    expect(html).toContain("Not publicly traded");
    expect(html).toContain("No EODHD symbol resolves to this company");
    // The LEI still shows: holding one is a fact, just not a listing.
    expect(html).toContain("NHBDILHZTYCNBV5UYZ31");
    expect(html).not.toContain('data-slot="chart"');
  });
});

describe("the Sources strip every tab opens with", () => {
  // Task 20: the list page says 'BSEW' in letters; each tab has to say the
  // same thing in words, derived from what that tab ALREADY loaded -- no tab
  // gained a query for this. One register, one name, across all five.
  it("names the registers behind the addresses -- including a tombstoned one's", () => {
    const html = render(
      <SeCompanyAddressTab
        result={null}
        detail={{
          ...emptyDetail,
          addresses: [{ ...address, sources: ["scb"] }],
          // A rejected address is still evidence Bolagsverket held one, and
          // its card is still on the page -- so the strip must name it.
          removed: [
            { ...address, address_key: "e".repeat(64), sources: ["bolagsverket"] },
          ],
        }}
      />,
      seCompanyTabPath(COMPANY_ID, "address"),
    );
    expect(html).toContain('data-source-strip="Bolagsverket,SCB"');
  });

  // The Financial tab has no strip since it became the shared public
  // financials view: the source switcher already names each register.

  it("names the registers behind the people's ROLES, and says nothing when no role resolved", () => {
    const html = render(
      <SeCompanyPeopleTab companyId={COMPANY_ID} people={people} evidence={[]} />,
      seCompanyTabPath(COMPANY_ID, "people"),
    );
    expect(html).toContain('data-source-strip="Bolagsverket"');

    // se_company_person carries no source column of its own: a company whose
    // published people have no resolved role has nothing to name, and says so
    // with the em dash rather than inventing a register.
    const roleless = render(
      <SeCompanyPeopleTab
        companyId={COMPANY_ID}
        people={people.map((person) => ({ ...person, roles: [] }))}
        evidence={[]}
      />,
      seCompanyTabPath(COMPANY_ID, "people"),
    );
    expect(roleless).toContain('data-source-strip=""');
  });

  it("folds the domain register's own spelling onto the catalog's name", () => {
    const html = render(
      <SeCompanyDomainsTab
        companyId={COMPANY_ID}
        domains={[
          { ...domain, source_names: ["esef_filing", "common_crawl_identity"] },
          domain,
        ]}
      />,
      seCompanyTabPath(COMPANY_ID, "domains"),
    );
    // 'esef_filing' is ESEF; 'common_crawl_identity' has no letter but is
    // still named in prose; and the catalog's order wins over arrival order.
    expect(html).toContain('data-source-strip="ESEF,Wikidata,Common Crawl"');
    // The per-row evidence still shows the RAW token -- the strip summarises,
    // it does not rewrite what the register recorded.
    expect(html).toContain(">esef_filing<");
  });
});

describe("tab labels", () => {
  it("is exactly Info, Address, Financial, People, Domains, Technology, Contracts, Jobs, Listed, in that order", () => {
    expect(SE_COMPANY_TABS.map((tab) => tab.label)).toEqual([
      "Info",
      "Address",
      "Financial",
      "People",
      "Domains",
      "Technology",
      "Contracts",
      "Jobs",
      "Publicly traded",
    ]);
    const values: SeCompanyTab[] = SE_COMPANY_TABS.map((tab) => tab.value);
    expect(values).toEqual([
      "info",
      "address",
      "financial",
      "people",
      "domains",
      "technology",
      "contracts",
      "jobs",
      "listed",
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
