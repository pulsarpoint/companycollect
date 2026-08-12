import { describe, expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import {
  placedKeys,
  restKeys,
} from "~/components/detail/countries/no-financials";
import { isLineageKey } from "~/components/detail/fields";
import { COUNTRIES, getCountry } from "~/lib/countries";
import { getFacetOptions } from "~/lib/facets.server";
import { filterableFacetKeys } from "~/lib/filters";
import {
  PAGE_SIZES,
  companyHasFlag,
  getCompanyDetail,
  getCompanyShell,
  getCompanyTechnologyIpDetail,
  getCompanyTechnologyIpInventory,
  getCompanyTechnologyInfrastructure,
  getCompanyTechnologyDetail,
  getCountryStats,
  getTechnologyIpDetail,
  searchCompanies,
} from "~/lib/queries.server";

// Integration tests against the real ClickHouse. Estonia is the smallest
// dataset (~373k rows), so queries stay fast.
const ee = getCountry("ee")!;
const se = getCountry("se")!;

describe("getCountryStats", () => {
  it("returns positive totals with active <= total", async () => {
    const stats = await getCountryStats(ee);
    expect(stats.total).toBeGreaterThan(100_000);
    expect(stats.active).toBeGreaterThan(0);
    expect(stats.active).toBeLessThanOrEqual(stats.total);
  });
});

describe("searchCompanies", () => {
  it("returns a first page of rows with id and name", async () => {
    const result = await searchCompanies(ee, { page: 1, pageSize: 25 });
    expect(result.rows).toHaveLength(25);
    expect(result.total).toBeGreaterThan(100_000);
    for (const row of result.rows) {
      expect(row.id).toBeTruthy();
      expect(row.name).toBeTruthy();
    }
  });

  it("filters by case-insensitive name substring", async () => {
    const result = await searchCompanies(ee, { q: "grupp", pageSize: 25 });
    expect(result.rows.length).toBeGreaterThan(0);
    expect(result.total).toBeLessThan(400_000);
    for (const row of result.rows) {
      expect(String(row.name).toLowerCase()).toContain("grupp");
    }
  });

  it("falls back to sane defaults on non-finite page inputs", async () => {
    const result = await searchCompanies(ee, {
      page: Number("abc"),
      pageSize: Number.POSITIVE_INFINITY,
    });
    expect(result.page).toBe(1);
    expect(result.pageSize).toBe(50);
    expect(result.rows.length).toBeGreaterThan(0);
  });

  it("paginates without overlap", async () => {
    const p1 = await searchCompanies(ee, { page: 1, pageSize: 25 });
    const p2 = await searchCompanies(ee, { page: 2, pageSize: 25 });
    const ids1 = new Set(p1.rows.map((r) => r.id));
    expect(p2.rows.some((r) => ids1.has(r.id))).toBe(false);
  });

  it("clamps out-of-range pages to the last page", async () => {
    const result = await searchCompanies(ee, {
      page: Number.MAX_SAFE_INTEGER,
      pageSize: 25,
    });
    const lastPage = Math.max(1, Math.ceil(result.total / result.pageSize));
    expect(result.page).toBe(lastPage);
    expect(result.rows.length).toBeGreaterThan(0);
  });
});

describe("searchCompanies sorting and columns", () => {
  it("returns all declared column keys plus active and industry fields", async () => {
    const result = await searchCompanies(ee, { pageSize: 25 });
    expect(result.pageSize).toBe(25);
    const row = result.rows[0];
    for (const col of ee.columns) {
      expect(row, `column ${col.key}`).toHaveProperty(col.key);
    }
    expect(row).toHaveProperty("active");
    expect(row).toHaveProperty("industry_code");
    expect(row).toHaveProperty("industry_label");
    expect(row).not.toHaveProperty("__industry_key");
  });

  it("merges a primary industry for most companies on a page", async () => {
    const result = await searchCompanies(ee, { pageSize: 50 });
    const withIndustry = result.rows.filter((r) => r.industry_label);
    // ~96% of EE companies have a primary industry; a 50-row page having zero would mean the merge is broken.
    expect(withIndustry.length).toBeGreaterThan(0);
  });

  it("sorts by a whitelisted column in both directions", async () => {
    const asc = await searchCompanies(ee, {
      sort: "id",
      dir: "asc",
      pageSize: 25,
    });
    const desc = await searchCompanies(ee, {
      sort: "id",
      dir: "desc",
      pageSize: 25,
    });
    expect(asc.sort).toBe("id");
    expect(asc.dir).toBe("asc");
    expect(desc.dir).toBe("desc");
    expect(asc.rows[0].id).not.toEqual(desc.rows[0].id);
    const ascIds = asc.rows.map((r) => String(r.id));
    expect([...ascIds].sort()).toEqual(ascIds);
  });

  it("falls back to name asc on unknown sort keys and directions", async () => {
    const result = await searchCompanies(ee, {
      sort: "industry_label; DROP TABLE x",
      dir: "sideways",
      pageSize: 25,
    });
    expect(result.sort).toBe("name");
    expect(result.dir).toBe("asc");
    expect(result.rows).toHaveLength(25);
  });

  it("accepts only whitelisted page sizes", async () => {
    const result = await searchCompanies(ee, { pageSize: 37 });
    expect(result.pageSize).toBe(50);
    expect(PAGE_SIZES).toEqual([25, 50, 100]);
  });
});

describe("searchCompanies across all countries", () => {
  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: default first page executes registry SQL and merges industry",
    async (_code, country) => {
      const result = await searchCompanies(country, { pageSize: 25 });
      expect(result.rows.length).toBeGreaterThan(0);
      expect(result.total).toBeGreaterThan(0);
      const row = result.rows[0];
      expect(row).toHaveProperty("id");
      expect(row).toHaveProperty("name");
      expect(row).toHaveProperty("industry_code");
      expect(row).not.toHaveProperty("__industry_key");
    },
    30_000,
  );

  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: first facet loads and filters companies",
    async (_code, country) => {
      const keys = filterableFacetKeys(country).filter((k) => k !== "industry");
      const facetKey = keys[0];
      const options = await getFacetOptions(country, facetKey);
      expect(options.length).toBeGreaterThan(0);
      const filtered = await searchCompanies(country, {
        pageSize: 25,
        filters: { [facetKey]: [options[0].value] },
      });
      expect(filtered.total).toBeGreaterThan(0);
    },
    60_000,
  );

  it.each(
    COUNTRIES.filter((c) => c.industryFilterExpr).map(
      (c) => [c.code, c] as const,
    ),
  )(
    "%s: industry facet loads and filters companies",
    async (_code, country) => {
      const options = await getFacetOptions(country, "industry");
      expect(options.length).toBeGreaterThan(0);
      const filtered = await searchCompanies(country, {
        pageSize: 25,
        filters: { industry: [options[0].value] },
      });
      expect(filtered.total).toBeGreaterThan(0);
    },
    120_000,
  );

  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: company detail loads for a first-page id",
    async (_code, country) => {
      const page = await searchCompanies(country, { pageSize: 1 });
      const id = String(page.rows[0].id);
      const detail = await getCompanyDetail(country, id);
      expect(detail).not.toBeNull();
      expect(String(detail!.company.id)).toBe(id);
    },
    60_000,
  );

  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: shared company shell loads header and record data",
    async (_code, country) => {
      const [seed] = await chQuery<{ id: string }>(
        `SELECT toString(${country.idColumn}) AS id
         FROM ${country.companiesTable}
         LIMIT 1`,
      );
      const shell = await getCompanyShell(country, seed.id);
      expect(shell).not.toBeNull();
      expect(String(shell!.company.id)).toBe(seed.id);
      expect(shell!.record).toBeTruthy();
      expect(shell!.company).not.toHaveProperty("__industry_key");
    },
    60_000,
  );

  it.each(
    COUNTRIES.filter((c) => c.detail?.financialsQuery).map(
      (c) => [c.code, c] as const,
    ),
  )(
    "%s: financials registry SQL is valid against live schema",
    async (_code, country) => {
      // Execute the registry SQL directly with a synthetic id: zero rows expected,
      // but a schema/SQL error surfaces as a ClickHouse exception → test failure.
      const rows = await chQuery(country.detail!.financialsQuery!, { id: "0" });
      expect(Array.isArray(rows)).toBe(true);
    },
    60_000,
  );

  it("Sweden financial facts join the versioned taxonomy dictionary exactly", () => {
    const query = getCountry("se")!.detail!.factsQuery!;

    expect(query).toContain(
      "labels.taxonomy_entrypoint = ifNull(report.taxonomy_entrypoint, '')",
    );
    expect(query).toContain("labels.concept_namespace = f.concept_namespace");
    expect(query).toContain("labels.label_sv, fallback_labels.label_sv");
    expect(query).toContain(
      "labels.description_en, fallback_labels.description_en",
    );
    expect(query).toContain("concept_description_en_source");
    expect(query).toContain("concept_taxonomy_entrypoint");
  });

  it.each(
    COUNTRIES.filter(
      (c) => c.detail?.contactsQuery || c.detail?.domainsQuery,
    ).map((c) => [c.code, c] as const),
  )(
    "%s: contacts/domains registry SQL is valid against live schema",
    async (_code, country) => {
      for (const q of [
        country.detail?.contactsQuery,
        country.detail?.domainsQuery,
      ]) {
        if (!q) continue;
        const rows = await chQuery(q, { id: "0" });
        expect(Array.isArray(rows)).toBe(true);
      }
    },
    60_000,
  );

  it.each(
    COUNTRIES.filter((c) => c.detail?.industriesQuery).map(
      (c) => [c.code, c] as const,
    ),
  )(
    "%s: industriesQuery SQL is valid against live schema",
    async (_code, country) => {
      const rows = await chQuery(country.detail!.industriesQuery!, { id: "0" });
      expect(Array.isArray(rows)).toBe(true);
    },
    60_000,
  );

  it.each(
    COUNTRIES.filter((c) => c.detail?.addressQuery).map(
      (c) => [c.code, c] as const,
    ),
  )(
    "%s: addressQuery SQL is valid against live schema",
    async (_code, country) => {
      const rows = await chQuery(country.detail!.addressQuery!, { id: "0" });
      expect(Array.isArray(rows)).toBe(true);
    },
    60_000,
  );

  it.each(COUNTRIES.map((c) => [c.code, c] as const))(
    "%s: idColumn is a String column (unified merge-order invariant)",
    async (_code, country) => {
      const [row] = await chQuery<{ type: string }>(
        `SELECT type FROM system.columns
         WHERE database = 'corpscout' AND table = {table:String} AND name = {name:String}`,
        { table: country.companiesTable, name: country.idColumn },
      );
      expect(row.type).toBe("String");
    },
    30_000,
  );

  it.each(
    COUNTRIES.filter((c) => c.financialsLatest).map(
      (c) => [c.code, c] as const,
    ),
  )(
    "%s: financialsLatest summary table exists and companyKeyExpr joins to it",
    async (_code, country) => {
      const { table, companyKeyExpr } = country.financialsLatest!;
      // Summary table exists and company_id is queryable.
      const rows = await chQuery(`SELECT company_id FROM ${table} LIMIT 1`);
      expect(Array.isArray(rows)).toBe(true);

      // companyKeyExpr on companiesTable is queryable and semi-joins against
      // the summary table's company_id — mirrors the real join in
      // unified.server.ts (`LEFT JOIN ... ON fin.company_id =
      // toString(${companyKeyExpr})`), so this catches a stale/renamed
      // companyKeyExpr the plain LIMIT 1 alone would miss.
      const joined = await chQuery(
        `SELECT ${companyKeyExpr} AS key
         FROM ${country.companiesTable}
         WHERE toString(${companyKeyExpr}) IN (SELECT company_id FROM ${table})
         LIMIT 1`,
      );
      expect(Array.isArray(joined)).toBe(true);
    },
    30_000,
  );
});

describe("searchCompanies with filters", () => {
  it("applies a single-value filter to rows and total", async () => {
    const unfiltered = await searchCompanies(ee, { pageSize: 25 });
    const statusOptions = await getFacetOptions(ee, "status");
    const top = statusOptions[0].value;
    const filtered = await searchCompanies(ee, {
      pageSize: 25,
      filters: { status: [top] },
    });
    expect(filtered.total).toBeGreaterThan(0);
    expect(filtered.total).toBeLessThanOrEqual(unfiltered.total);
    for (const row of filtered.rows) {
      expect(String(row.status)).toBe(top);
    }
  });

  it("multi-value filter is a union (IN)", async () => {
    const statusOptions = await getFacetOptions(ee, "status");
    if (statusOptions.length < 2) return; // data-dependent guard
    const [a, b] = statusOptions;
    const fa = await searchCompanies(ee, { filters: { status: [a.value] } });
    const fb = await searchCompanies(ee, { filters: { status: [b.value] } });
    const both = await searchCompanies(ee, {
      filters: { status: [a.value, b.value] },
    });
    expect(both.total).toBe(fa.total + fb.total);
  });

  it("filters compose with q search", async () => {
    const statusOptions = await getFacetOptions(ee, "status");
    const result = await searchCompanies(ee, {
      q: "grupp",
      filters: { status: [statusOptions[0].value] },
    });
    for (const row of result.rows) {
      expect(String(row.name).toLowerCase()).toContain("grupp");
      expect(String(row.status)).toBe(statusOptions[0].value);
    }
  });

  it("ignores filter keys not in the registry whitelist", async () => {
    const result = await searchCompanies(ee, {
      pageSize: 25,
      filters: { "bogus; DROP": ["x"], name: ["y"] } as never,
    });
    expect(result.rows.length).toBe(25); // filters silently ignored
  });
});

describe("Sweden technical-information filter", () => {
  it("returns only companies connected to a current website domain", async () => {
    const result = await searchCompanies(se, {
      pageSize: 25,
      filters: { flag_domain: ["yes"] },
    });

    expect(result.total).toBeGreaterThan(0);
    expect(result.total).toBeLessThan(10_000);
    expect(result.rows).toHaveLength(25);
    for (const row of result.rows) {
      expect(row.flags).toContain("domain");
    }
  });

  it("checks domain availability for company navigation", async () => {
    const [withTechnology, withoutTechnology] = await Promise.all([
      companyHasFlag(se, "5594643297", "domain"),
      companyHasFlag(se, "8024123872", "domain"),
    ]);

    expect(withTechnology).toBe(true);
    expect(withoutTechnology).toBe(false);
  }, 30_000);

  it("combines certificate and DNS evidence for a company's primary domain", async () => {
    const infrastructure = await getCompanyTechnologyInfrastructure(
      se,
      "5594643297",
      { page: 1, pageSize: 25 },
    );

    expect(infrastructure).not.toBeNull();
    expect(infrastructure?.domain).toBe("100.se");
    expect(infrastructure?.summary.totalHostnames).toBeGreaterThanOrEqual(10);
    expect(infrastructure?.summary.certificateHostnames).toBeGreaterThan(0);
    expect(infrastructure?.summary.dnsHostnames).toBeGreaterThan(0);
    expect(infrastructure?.summary.resolvedIpAddressesOnPage).toBeGreaterThan(
      0,
    );

    const api = infrastructure?.hostnames.find(
      (hostname) => hostname.hostname === "api.100.se",
    );
    expect(api?.evidence).toEqual(
      expect.arrayContaining(["certificate", "dns"]),
    );
    expect(api?.records.some((record) => record.type === "A")).toBe(true);
    expect(api?.ipAddresses.length).toBeGreaterThan(0);
    expect(
      infrastructure?.summary.rdapRegisteredIpAddressesOnPage,
    ).toBeGreaterThan(0);
    expect(
      infrastructure?.hostnames
        .flatMap((hostname) => hostname.ipAddresses)
        .some((address) => address.rdapRegistration !== null),
    ).toBe(true);
    expect(
      infrastructure?.hostnames
        .flatMap((hostname) => hostname.ipAddresses)
        .some(
          (address) =>
            address.rdapRegistration?.matchedCidr === "0.0.0.0/0" ||
            address.rdapRegistration?.matchedCidr === "::/0",
        ),
    ).toBe(false);
    expect(api?.records[0]?.firstSeen).toBeTruthy();
    expect(api?.records[0]?.lastSeen).toBeTruthy();
    expect(infrastructure?.scan?.resolvedAt).toBeTruthy();
  }, 30_000);

  it("lists distinct company IPs and opens an address detail safely", async () => {
    const inventory = await getCompanyTechnologyIpInventory(se, "5594643297", {
      page: 1,
      pageSize: 25,
    });

    expect(inventory?.domain).toBe("100.se");
    expect(inventory?.summary.totalAddresses).toBeGreaterThan(0);
    expect(inventory?.addresses.length).toBeGreaterThan(0);
    expect(inventory?.addresses[0]?.networkSegment).toMatch(/\/(24|48)$/);
    expect(
      inventory?.addresses.every((address) => address.hostnames.length > 0),
    ).toBe(true);

    const selected = inventory!.addresses.find(
      (address) => address.rdapRegistration,
    )!;
    expect(selected).toBeTruthy();
    const detail = await getCompanyTechnologyIpDetail(
      se,
      "5594643297",
      selected.ip,
    );

    expect(detail?.address.ip).toBe(selected.ip);
    expect(detail?.companyDomain).toBe("100.se");
    expect(detail?.companyHostnames.length).toBeGreaterThan(0);
    expect(
      detail?.exactConnections.connections.some(
        (connection) => connection.domain === "100.se",
      ),
    ).toBe(true);
    expect(detail?.segmentConnections).not.toBeNull();

    const canonicalDetail = await getTechnologyIpDetail(selected.ip);
    expect(canonicalDetail?.address.ip).toBe(selected.ip);
    expect(canonicalDetail?.exactConnections.total).toBeGreaterThan(0);
    expect(canonicalDetail?.address.networkSegment).toBe(
      selected.networkSegment,
    );
  }, 30_000);

  it("builds Common Crawl web-technology change history", async () => {
    const technology = await getCompanyTechnologyDetail(se, "5594643297");

    expect(technology.webTechnologyHistory?.domain).toBe("100.se");
    expect(technology.webTechnologyHistory?.latestCrawlId).toBe(
      "CC-MAIN-2026-30",
    );
    expect(
      technology.webTechnologyHistory?.technologies.find(
        (item) => item.name === "Next.js",
      )?.state,
    ).toBe("detected_latest");
    expect(
      technology.webTechnologyHistory?.technologies.find(
        (item) => item.name === "C3.js",
      )?.state,
    ).toBe("not_detected_latest");
    expect(technology.webTechnologyHistory?.snapshots[0]).toEqual(
      expect.objectContaining({
        crawlId: "CC-MAIN-2026-30",
        noLongerDetected: ["YouTube"],
      }),
    );
  }, 30_000);
});

describe("industry filter (Estonia)", () => {
  it("filters companies by primary industry code", async () => {
    const options = await getFacetOptions(ee, "industry");
    const top = options[0];
    const filtered = await searchCompanies(ee, {
      filters: { industry: [top.value] },
    });
    expect(filtered.total).toBeGreaterThan(0);
    expect(filtered.total).toBeLessThan(400_000);
    for (const row of filtered.rows) {
      expect(row.industry_code).toBe(top.value);
    }
  });
});

describe("getCompanyDetail (Estonia)", () => {
  it("returns null for an unknown id", async () => {
    const detail = await getCompanyDetail(ee, "does-not-exist-000");
    expect(detail).toBeNull();
  });

  it("returns company row with industry for a real id", async () => {
    const page = await searchCompanies(ee, { pageSize: 1 });
    const id = String(page.rows[0].id);
    const detail = await getCompanyDetail(ee, id);
    expect(detail).not.toBeNull();
    expect(String(detail!.company.id)).toBe(id);
    expect(detail!.company).toHaveProperty("name");
    expect(detail!.company).toHaveProperty("active");
    expect(detail!.company).toHaveProperty("industry_code");
    expect(detail!.company).not.toHaveProperty("__industry_key");
  });

  it("returns canonical financials for a company that has them", async () => {
    const [row] = await chQuery<{ id: string }>(
      `SELECT reg_code AS id FROM ee_financial_metrics
       WHERE revenue_amount_usd IS NOT NULL
         AND reg_code IN (SELECT reg_code FROM ee_companies)
       ORDER BY reg_code
       LIMIT 1`,
    );
    const detail = await getCompanyDetail(ee, row.id);
    expect(detail!.financials.length).toBeGreaterThan(0);
    expect(typeof detail!.financials[0].fiscal_year).toBe("string");
    expect(
      detail!.financials.some((f) => typeof f.revenue_amount_usd === "number"),
    ).toBe(true); // toFloat64 → JSON number, not string
    const years = detail!.financials.map((f) => f.fiscal_year);
    expect([...years].sort().reverse()).toEqual(years); // newest first
  });

  it("sweden returns canonical financials preferring the complete duplicate row", async () => {
    const se = getCountry("se")!;
    // Known large filer with a duplicate all-NULL 2023 metrics row alongside
    // the complete one — the isNull tiebreak must surface the complete row.
    const detail = await getCompanyDetail(se, "5561632232");
    expect(detail).not.toBeNull();
    const y2023 = detail!.financials.find((f) => f.fiscal_year === "2023");
    expect(y2023).toBeDefined();
    expect(y2023!.revenue_amount_usd).toBeGreaterThan(1e9); // ≈ $4.16bn
    expect(y2023!.employees).toBeGreaterThan(1000);
    // Dual-currency contract: original AND usd both present for money fields.
    expect(y2023!.total_assets_amount_original).toBeGreaterThan(1e9); // SEK
    expect(y2023!.equity_amount_original).not.toBeNull();
  });

  it("returns contacts and domains for companies that have them", async () => {
    const [c] = await chQuery<{ id: string }>(
      `SELECT registry_id AS id FROM ee_company_contacts
       WHERE is_current = 1
         AND registry_id IN (SELECT reg_code FROM ee_companies)
       ORDER BY registry_id
       LIMIT 1`,
    );
    const withContacts = await getCompanyDetail(ee, c.id);
    expect(withContacts!.contacts.length).toBeGreaterThan(0);
    expect(withContacts!.contacts[0]).toHaveProperty("contact_type");
    expect(withContacts!.contacts[0]).toHaveProperty("contact_value");

    const [d] = await chQuery<{ id: string }>(
      `SELECT registry_id AS id FROM ee_company_domains
       WHERE is_current = 1
         AND registry_id IN (SELECT reg_code FROM ee_companies)
       ORDER BY registry_id
       LIMIT 1`,
    );
    const withDomains = await getCompanyDetail(ee, d.id);
    expect(withDomains!.domains.length).toBeGreaterThan(0);
    expect(withDomains!.domains[0].domain).toBeTruthy();
  });

  it("returns the full company record with every table column", async () => {
    const page = await searchCompanies(ee, { pageSize: 1 });
    const detail = await getCompanyDetail(ee, String(page.rows[0].id));
    const keys = Object.keys(detail!.record);
    // ee_companies has 24 columns — the record must carry them all,
    // including columns the list config never shows:
    expect(keys.length).toBeGreaterThanOrEqual(20);
    expect(keys).toContain("address");
    expect(keys).toContain("company_url");
    expect(keys).toContain("source_url");
    expect(detail!.record.reg_code).toBe(String(page.rows[0].id));
  });
});

describe("getCompanyDetail statements (Norway)", () => {
  it("returns full statement rows for a company with filings", async () => {
    const no = getCountry("no")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT org_number AS id FROM no_financial_statements
       WHERE operating_costs_amount_original IS NOT NULL
         AND org_number IN (SELECT org_number FROM no_companies)
       ORDER BY org_number LIMIT 1`,
    );
    const detail = await getCompanyDetail(no, row.id);
    expect(detail!.statements.length).toBeGreaterThan(0);
    const stmt = detail!.statements[0];
    // full-fidelity: raw table columns present, not the canonical subset
    expect(stmt).toHaveProperty("operating_costs_amount_original");
    expect(stmt).toHaveProperty("accounts_type");
    expect(stmt).toHaveProperty("is_parent_company");
  }, 30_000);

  it("countries without statementsQuery return an empty array", async () => {
    const page = await searchCompanies(ee, { pageSize: 1 });
    const detail = await getCompanyDetail(ee, String(page.rows[0].id));
    expect(detail!.statements).toEqual([]);
  });

  it("every statement column is placed in a group, lineage, or rest (fidelity guarantee)", async () => {
    const no = getCountry("no")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT org_number AS id FROM no_financial_statements
       WHERE org_number IN (SELECT org_number FROM no_companies)
       ORDER BY org_number LIMIT 1`,
    );
    const detail = await getCompanyDetail(no, row.id);
    const row0 = detail!.statements[0];
    const rest = restKeys(row0);
    // Today every column is explicitly grouped; a future migration adding a
    // column makes it appear in "Other fields" automatically — never dropped.
    expect(rest).toEqual([]);

    // Every fetched column must be accounted for: placed in a visible group,
    // rendered as lineage, or surfaced in "Other fields" — nothing silently
    // dropped from rendering.
    const placed = new Set(placedKeys());
    for (const key of Object.keys(row0)) {
      expect(
        placed.has(key) || isLineageKey(key) || rest.includes(key),
        `column ${key} must be placed, lineage, or rest`,
      ).toBe(true);
    }
  }, 30_000);
});

describe("getCompanyDetail (Brazil)", () => {
  it("returns deterministic pivoted financials for a BR CVM company", async () => {
    const br = getCountry("br")!;
    // Note: joined in this direction (br_companies outer, financials subquery
    // inner) so ClickHouse hashes the smaller ~900k-row financials view
    // instead of the 68.6M-row br_companies table — the other direction
    // reliably takes ~38s against live data and trips the client's 30s
    // per-request HTTP timeout. Same semantics, same result set.
    const [row] = await chQuery<{ id: string }>(
      `SELECT cnpj_basico AS id FROM br_companies
       WHERE cnpj_basico IN (
         SELECT cnpj_basico FROM br_cvm_financial_metrics
         WHERE metric_name = 'revenue' AND amount_usd IS NOT NULL AND period_type = 'annual'
       )
       ORDER BY cnpj_basico LIMIT 1`,
    );
    const first = await getCompanyDetail(br, row.id);
    const second = await getCompanyDetail(br, row.id);
    expect(first!.financials.length).toBeGreaterThan(0);
    expect(first!.financials).toEqual(second!.financials); // deterministic across calls
    expect(
      first!.financials.some((f) => typeof f.revenue_amount_usd === "number"),
    ).toBe(true);
  }, 120_000);
});

describe("translated record cards", () => {
  it("norway record carries _en fields AND base-only fields (fidelity both ways)", async () => {
    const no = getCountry("no")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT org_number AS id FROM no_companies_translated
       WHERE articles_purpose_en IS NOT NULL AND articles_purpose_en != ''
       ORDER BY org_number LIMIT 1`,
    );
    const detail = await getCompanyDetail(no, row.id);
    expect(detail!.record).toHaveProperty("articles_purpose_en");
    expect(detail!.record).toHaveProperty("activity_text_en");
    expect(detail!.record).toHaveProperty("legal_form_description_en");
    expect(detail!.record).toHaveProperty("last_submitted_accounts_year"); // base-only column survives
    expect(String(detail!.record.articles_purpose_en)).not.toBe("");
  }, 30_000);

  it("latvia record carries activity_text_en and the history-backed current address", async () => {
    const lv = getCountry("lv")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT regcode AS id FROM lv_companies_translated
       WHERE activity_text_en IS NOT NULL AND activity_text_en != ''
       ORDER BY regcode LIMIT 1`,
    );
    const detail = await getCompanyDetail(lv, row.id);
    expect(detail!.record).toHaveProperty("activity_text_en");
    expect(String(detail!.record.activity_text_en)).not.toBe("");
    expect(detail!.record).toHaveProperty("address_city_name");
  }, 30_000);
});

describe("industries section", () => {
  it("estonia returns all industry rows with english labels, primary first", async () => {
    const [row] = await chQuery<{ id: string }>(
      `SELECT reg_code AS id FROM ee_industries
       WHERE is_primary = 1 AND reg_code IN (SELECT reg_code FROM ee_companies)
       ORDER BY reg_code LIMIT 1`,
    );
    const detail = await getCompanyDetail(ee, row.id);
    expect(detail!.industries.length).toBeGreaterThan(0);
    expect(detail!.industries[0].is_primary).toBe(1);
    expect(detail!.industries[0].industry_label).toBeTruthy();
  });

  it("latvia returns an empty industries array", async () => {
    const lv = getCountry("lv")!;
    const page = await searchCompanies(lv, { pageSize: 1 });
    const detail = await getCompanyDetail(lv, String(page.rows[0].id));
    expect(detail!.industries).toEqual([]);
  }, 30_000);
});

describe("addresses", () => {
  it("estonia composes a full address from embedded columns", async () => {
    const [row] = await chQuery<{ id: string }>(
      `SELECT reg_code AS id FROM ee_companies
       WHERE address != '' AND postal_code != ''
       ORDER BY reg_code LIMIT 1`,
    );
    const detail = await getCompanyDetail(ee, row.id);
    expect(detail!.addresses.length).toBeGreaterThan(0);
    expect(detail!.addresses[0].full_address).toContain(",");
    expect(detail!.addresses[0].full_address).not.toMatch(/, ,|^,|,$/);
  });

  it("sweden reads its addresses table", async () => {
    const se = getCountry("se")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT registration_number AS id FROM se_companies
       WHERE company_id IN (
         SELECT company_id
         FROM se_company_addresses_current
         WHERE has_address = 1 AND street_address != ''
       )
       ORDER BY registration_number LIMIT 1`,
    );
    const detail = await getCompanyDetail(se, row.id);
    expect(detail!.addresses.length).toBeGreaterThan(0);
    expect(detail!.addresses[0].full_address).toBeTruthy();
  }, 30_000);

  it("sweden exposes a registration date and geocoder-safe address without an unverified VAT number", async () => {
    const se = getCountry("se")!;
    const detail = await getCompanyDetail(se, "8024887013");

    expect(detail!.record.registration_date).toBe("2014-05-15");
    expect(detail!.record).not.toHaveProperty("incorporation_date");
    expect(detail!.record).not.toHaveProperty("vat_number");
    expect(detail!.addresses[0].full_address).toBe(
      "c/o AZIZ MANNANOV, PRÄSTGÅRDSLIDEN 4 C, 595 42 MJÖLBY",
    );
    expect(detail!.addresses[0].geocode_address).toBe(
      "PRÄSTGÅRDSLIDEN 4 C, 595 42 MJÖLBY",
    );
    expect(detail!.addresses[0].geocode_street).toBe("PRÄSTGÅRDSLIDEN 4 C");
    expect(detail!.addresses[0].geocode_postal_code).toBe("59542");
    expect(detail!.addresses[0].address_country_code).toBe("SE");
    expect(detail!.addresses[0].address_is_foreign).toBe(0);
  }, 30_000);

  it("sweden identifies and cleans a Polish address stored as foreign by SCB", async () => {
    const se = getCountry("se")!;
    const detail = await getCompanyDetail(se, "5020640487");

    expect(detail!.addresses[0].full_address).toBe("GLINKI 146, BYDGOSZCZ, PL");
    expect(detail!.addresses[0].geocode_address).toBe("GLINKI 146, BYDGOSZCZ");
    expect(detail!.addresses[0].address_country_code).toBe("PL");
    expect(detail!.addresses[0].address_is_foreign).toBe(1);
  }, 30_000);

  it("sweden separates care-of and floor details from its geocoding address", async () => {
    const se = getCountry("se")!;
    const detail = await getCompanyDetail(se, "8024123872");

    expect(detail!.addresses[0].full_address).toBe(
      "c/o R TORO, BERGENGATAN 13, 3 TR, 164 37 KISTA",
    );
    expect(detail!.addresses[0].geocode_address).toBe(
      "BERGENGATAN 13, 164 37 KISTA",
    );
    expect(detail!.addresses[0].geocode_street).toBe("BERGENGATAN 13");
    expect(detail!.addresses[0].geocode_postal_code).toBe("16437");
    expect(detail!.addresses[0].address_country_code).toBe("SE");
    expect(detail!.addresses[0].address_is_foreign).toBe(0);
  }, 30_000);

  it("sweden reconstructs company source records from keyed evidence lookups", async () => {
    const detail = await getCompanyDetail(se, "5594643297");

    expect(detail).not.toBeNull();
    expect(detail!.sourceRecords.length).toBeGreaterThan(0);
    expect(
      detail!.sourceRecords.every(
        (record) =>
          record.sourceRecordUid.length === 64 &&
          record.evidence.some((evidence) => evidence.origins.length > 0),
      ),
    ).toBe(true);
  }, 30_000);

  it("finland reads its YTJ address table", async () => {
    const fi = getCountry("fi")!;
    const [row] = await chQuery<{ id: string }>(
      `SELECT business_id AS id FROM fi_companies
       WHERE business_id IN (SELECT registry_id FROM fi_company_addresses WHERE address_lines IS NOT NULL)
       ORDER BY business_id LIMIT 1`,
    );
    const detail = await getCompanyDetail(fi, row.id);
    expect(detail!.addresses.length).toBeGreaterThan(0);
    expect(detail!.addresses[0].full_address).toBeTruthy();
  }, 30_000);
});

describe("empties-last sorting (Finland has 1,205 nameless registry stubs)", () => {
  const fi = getCountry("fi")!;

  it("default name-asc page 1 starts with real names, not empty stubs", async () => {
    const result = await searchCompanies(fi, { pageSize: 25 });
    for (const row of result.rows) {
      expect(String(row.name)).not.toBe("");
    }
  }, 30_000);

  it("empties stay last under desc too", async () => {
    const result = await searchCompanies(fi, {
      sort: "name",
      dir: "desc",
      pageSize: 25,
    });
    for (const row of result.rows) {
      expect(String(row.name)).not.toBe("");
    }
  }, 30_000);

  it("total still counts the stubs (ordering only, no filtering)", async () => {
    const result = await searchCompanies(fi, { pageSize: 25 });
    expect(result.total).toBeGreaterThan(460_000); // 460,200 incl. the 1,205 stubs
  }, 30_000);
});
