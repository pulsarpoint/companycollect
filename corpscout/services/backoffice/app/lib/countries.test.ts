import { describe, expect, it } from "vitest";
import { COUNTRIES, getCountry, getSortColumn } from "~/lib/countries";

describe("country registry", () => {
  it("contains all ten countries with unique lowercase ISO2 codes", () => {
    const codes = COUNTRIES.map((c) => c.code);
    expect(codes).toEqual([...new Set(codes)]);
    expect(codes.every((c) => /^[a-z]{2}$/.test(c))).toBe(true);
    expect(codes.sort()).toEqual(
      ["br", "cz", "ee", "fi", "fr", "gb", "lv", "no", "se", "sk"].sort(),
    );
  });

  it("resolves countries case-insensitively", () => {
    expect(getCountry("no")?.name).toBe("Norway");
    expect(getCountry("NO")?.name).toBe("Norway");
    expect(getCountry("xx")).toBeUndefined();
  });

  it("maps Sweden to its status-based active expression", () => {
    const se = getCountry("se");
    expect(se?.companiesTable).toBe("se_companies");
    expect(se?.idColumn).toBe("company_id");
    expect(se?.nameColumn).toBe("legal_name");
    expect(se?.activeExpr).toBe("status = 'active'");
    expect(
      se?.columns.find((column) => column.key === "registered")?.label,
    ).toBe("Registered");
  });
});

describe("company columns", () => {
  it("every country declares id and name columns with unique keys", () => {
    for (const c of COUNTRIES) {
      const keys = c.columns.map((col) => col.key);
      expect(keys, c.code).toEqual([...new Set(keys)]);
      expect(keys, c.code).toContain("id");
      expect(keys, c.code).toContain("name");
      expect(keys, c.code).not.toContain("industry"); // industry is virtual, merged post-query
      expect(keys, c.code).not.toContain("active"); // reserved, always selected
    }
  });

  it("every country has a sortable status column and a sortable name", () => {
    for (const c of COUNTRIES) {
      const status = c.columns.find((col) => col.kind === "status");
      expect(status, c.code).toBeDefined();
      expect(status?.sortable, c.code).toBe(true);
      expect(
        c.columns.find((col) => col.key === "name")?.sortable,
        c.code,
      ).toBe(true);
    }
  });

  it("every industry query is parameterized and returns the merge contract", () => {
    for (const c of COUNTRIES) {
      expect(c.industryQuery, c.code).toBeDefined();
      expect(c.industryQuery, c.code).toContain("{ids:Array(String)}");
      expect(c.industryQuery, c.code).toContain("AS company_id");
      expect(c.industryQuery, c.code).toContain("AS industry_code");
      expect(c.industryQuery, c.code).toContain("AS industry_label");
    }
  });

  it("getSortColumn whitelists: unknown or unsortable keys fall back to name", () => {
    const ee = getCountry("ee")!;
    expect(getSortColumn(ee, "status").key).toBe("status");
    expect(getSortColumn(ee, "id; DROP TABLE x").key).toBe("name");
    expect(getSortColumn(ee, null).key).toBe("name");
  });

  it("every country has a filterable status; only text/status kinds are filterable", () => {
    for (const c of COUNTRIES) {
      const filterable = c.columns.filter((col) => col.filterable);
      expect(filterable.length, c.code).toBeGreaterThan(0);
      expect(
        c.columns.find((col) => col.kind === "status")?.filterable,
        c.code,
      ).toBe(true);
      for (const col of filterable) {
        expect(["text", "status"], `${c.code}:${col.key}`).toContain(col.kind);
        expect(col.key, `${c.code}:${col.key}`).toMatch(/^[a-z_]+$/);
      }
    }
  });

  it("id, name, registered are never filterable", () => {
    for (const c of COUNTRIES) {
      for (const key of ["id", "name", "registered"]) {
        expect(
          c.columns.find((col) => col.key === key)?.filterable,
          `${c.code}:${key}`,
        ).toBeFalsy();
      }
    }
  });
});

describe("industry facet and filter", () => {
  it("every country except lv has industryFacetQuery and industryFilterExpr", () => {
    for (const c of COUNTRIES) {
      if (c.code === "lv") {
        expect(c.industryFacetQuery, c.code).toBeUndefined();
        expect(c.industryFilterExpr, c.code).toBeUndefined();
        continue;
      }
      expect(c.industryFacetQuery, c.code).toContain(" AS value");
      expect(c.industryFacetQuery, c.code).toContain(" AS label");
      expect(c.industryFacetQuery, c.code).toContain(" AS cnt");
      expect(c.industryFilterExpr, c.code).toContain(
        "{f_industry:Array(String)}",
      );
    }
  });

  it("display industryQuery prefers canonical nace english labels", () => {
    // Every non-lv, non-br industryQuery joins nace_categories for the label.
    for (const c of COUNTRIES) {
      if (c.code === "lv" || c.code === "br") continue;
      expect(c.industryQuery, c.code).toContain("nace_categories");
    }
  });
});

describe("detail config", () => {
  // se joined 2026-07-18: sweden_financial (XBRL) landed after the detail pages were built.
  const FIN = ["no", "fi", "ee", "lv", "gb", "br", "se"];
  const CONTACTS = ["no", "fi", "ee", "lv", "cz", "br"];
  const DOMAINS = ["no", "fi", "se", "ee", "lv", "cz", "br"];
  const NONE = ["sk", "fr"];

  it("declares detail sections exactly per data availability", () => {
    for (const c of COUNTRIES) {
      if (NONE.includes(c.code)) {
        // These countries now carry a `detail` block for industriesQuery
        // (Task 2), but still have no financials/contacts/domains sections.
        expect(c.detail?.financialsQuery, c.code).toBeUndefined();
        expect(c.detail?.contactsQuery, c.code).toBeUndefined();
        expect(c.detail?.domainsQuery, c.code).toBeUndefined();
        continue;
      }
      expect(!!c.detail?.financialsQuery, c.code).toBe(FIN.includes(c.code));
      expect(!!c.detail?.contactsQuery, c.code).toBe(CONTACTS.includes(c.code));
      expect(!!c.detail?.domainsQuery, c.code).toBe(DOMAINS.includes(c.code));
    }
  });

  it("every detail query is parameterized and canonical", () => {
    for (const c of COUNTRIES) {
      for (const q of [
        c.detail?.financialsQuery,
        c.detail?.contactsQuery,
        c.detail?.domainsQuery,
      ]) {
        if (!q) continue;
        expect(q, c.code).toContain("{id:String}");
      }
      if (c.detail?.financialsQuery) {
        for (const col of [
          "AS fiscal_year",
          "AS currency",
          "AS revenue_amount_original",
          "AS revenue_amount_usd",
          "AS net_result_amount_original",
          "AS net_result_amount_usd",
          "AS total_assets_amount_original",
          "AS total_assets_amount_usd",
          "AS equity_amount_original",
          "AS equity_amount_usd",
          "AS employees",
        ]) {
          expect(c.detail.financialsQuery, `${c.code}: ${col}`).toContain(col);
        }
      }
      if (c.detail?.contactsQuery) {
        expect(c.detail.contactsQuery, c.code).toContain("AS contact_type");
        expect(c.detail.contactsQuery, c.code).toContain("AS contact_value");
      }
      if (c.detail?.domainsQuery) {
        for (const col of [
          "AS domain",
          "AS website_url",
          "AS domain_source",
          "AS confidence",
          "AS is_primary",
        ]) {
          expect(c.detail.domainsQuery, `${c.code}: ${col}`).toContain(col);
        }
      }
    }
  });

  it("reads every Swedish financial year from unified metrics", () => {
    const query = COUNTRIES.find((country) => country.code === "se")?.detail
      ?.financialsQuery;

    expect(query).toContain("FROM se_bolagsverket_financial_metrics");
    expect(query).toContain("observation_kind = 'reported'");
    expect(query).not.toContain("se_financial_history");
  });

  it("keeps Finland registry and consolidated financial sources separate", () => {
    const detail = getCountry("fi")?.detail;

    expect(detail?.financialSources?.map((source) => source.kind)).toEqual([
      "registry",
      "esef",
    ]);
    expect(detail?.financialSources?.[0]).toMatchObject({
      id: "prh-digital-annual-accounts",
      yearFacts: true,
    });
    expect(detail?.financialsQuery).toContain("FROM fi_financial_metrics");
    expect(detail?.factsQuery).toContain(
      "FROM fi_financial_facts_with_source AS f",
    );
    expect(detail?.factsQuery).toContain("AS concept_label_original");
    expect(detail?.factsDocumentQuery).toContain(
      "xml_source_uri AS source_uri",
    );
    expect(detail?.factsDocumentQuery).toContain(
      "'application/xml; charset=utf-8' AS content_type",
    );
  });

  it("norway declares a full statements query; others do not yet", () => {
    for (const c of COUNTRIES) {
      if (c.code === "no") {
        expect(c.detail?.statementsQuery).toContain("{id:String}");
        expect(c.detail?.statementsQuery).toContain("no_financial_statements");
      } else {
        expect(c.detail?.statementsQuery, c.code).toBeUndefined();
      }
    }
  });

  it("country record queries keep source translation work bounded", () => {
    for (const c of COUNTRIES) {
      if (c.code === "no") {
        expect(c.detail?.recordQuery).toContain("no_companies_translated");
        expect(c.detail?.recordQuery).toContain("{id:String}");
        expect(c.detail?.recordQuery).toContain("c.*");
      } else if (c.code === "lv") {
        expect(c.detail?.recordQuery).toContain("lv_companies_translated");
        expect(c.detail?.recordQuery).toContain("{id:String}");
        expect(c.detail?.recordQuery).toContain("c.*");
      } else if (c.code === "se") {
        expect(c.detail?.companyShellQuery).toContain("FROM se_companies AS c");
        expect(c.detail?.companyShellQuery).toContain("{id:String}");
        expect(c.detail?.companyShellQuery).toContain(
          "activity_description AS activity_description_original",
        );
        expect(c.detail?.companyShellQuery).toContain(
          "incorporation_date AS registration_date",
        );
        expect(c.detail?.companyShellQuery).not.toContain("splitByChar");
        expect(c.detail?.companyShellQuery).not.toContain(
          "$FORETAGSNAMN-ORGNAM$",
        );
        expect(c.detail?.companyShellQuery).not.toContain("vat_number");
        expect(c.detail?.companyShellQuery).not.toContain(
          "se_companies_translated",
        );
        // Enrichment identity resolution belongs to the offline serving build,
        // never to the shell request.
        expect(c.detail?.companyShellQuery).not.toContain("gleif_lei_records");
        expect(c.detail?.companyShellQuery).not.toContain("replaceRegexpAll");
        expect(c.detail?.companyShellQuery).toContain(
          "PREWHERE c.company_id = {id:String}",
        );
        expect(c.detail?.companyShellQuery).not.toContain(
          "WHERE c.registration_number = {id:String}",
        );
      } else {
        expect(c.detail?.recordQuery, c.code).toBeUndefined();
      }
    }
  });

  it("every country except lv declares industriesQuery with the canonical aliases", () => {
    for (const c of COUNTRIES) {
      if (c.code === "lv") {
        expect(c.detail?.industriesQuery).toBeUndefined();
        continue;
      }
      for (const alias of [
        "AS industry_code",
        "AS description_original",
        "AS industry_label",
        "AS is_primary",
      ]) {
        expect(c.detail?.industriesQuery, `${c.code}: ${alias}`).toContain(
          alias,
        );
      }
      expect(c.detail?.industriesQuery, c.code).toContain("{id:String}");
    }
  });

  it("every country declares addressQuery with canonical aliases", () => {
    for (const c of COUNTRIES) {
      expect(c.detail?.addressQuery, c.code).toContain("AS address_type");
      expect(c.detail?.addressQuery, c.code).toContain("AS full_address");
      expect(c.detail?.addressQuery, c.code).toContain("{id:String}");
    }
  });

  it("sweden address query exposes foreign-country geocoding metadata", () => {
    const se = getCountry("se")!;
    expect(se.placeQuery).toContain("se_company_addresses_current");
    expect(se.detail?.addressQuery).toContain("se_company_addresses_current");
    expect(se.detail?.addressQuery).toContain("has_address = 1");
    expect(se.detail?.addressQuery).toContain("AS geocode_address");
    expect(se.detail?.addressQuery).toContain("AS geocode_street");
    expect(se.detail?.addressQuery).toContain("AS geocode_postal_code");
    expect(se.detail?.addressQuery).toContain("AS address_country_code");
    expect(se.detail?.addressQuery).toContain("AS address_is_foreign");
  });

  it("latvia reads current addresses from the history-backed views", () => {
    const lv = getCountry("lv")!;
    expect(lv.companiesTable).toBe("lv_companies_current");
    expect(lv.detail?.recordQuery).toContain("lv_companies_current AS c");
    expect(lv.detail?.addressQuery).toContain("lv_company_addresses_current");
  });
});

describe("Brazil industry labels prefer CNAE over the NACE division", () => {
  const br = COUNTRIES.find((c) => c.code === "br")!;
  const queries = [
    ["industryQuery", br.industryQuery],
    ["industryFacetQuery", br.industryFacetQuery],
    ["industriesQuery", br.detail?.industriesQuery],
  ] as const;

  it.each(queries)("%s reads the CNAE subclass name", (_name, sql) => {
    expect(sql).toContain("br_cnae_categories_translated");
    expect(sql).toContain("c.level = 'subclass'");
  });

  it.each(queries)("%s puts CNAE ahead of the NACE division", (_name, sql) => {
    // The bridge is division level: CNAE and NACE agree on the two-digit
    // division and nothing below it. Preferring the division labelled a
    // clothing shop "Retail trade, except of motor vehicles and motorcycles".
    const cnae = sql!.indexOf("c.description_en");
    const nace = sql!.indexOf("m.nace_description_en");
    expect(cnae).toBeGreaterThan(-1);
    expect(nace).toBeGreaterThan(-1);
    expect(cnae).toBeLessThan(nace);
  });

  it("pairs the English label with IBGE's own Portuguese", () => {
    // CONCLA publishes Portuguese only, so the English is a translation and the
    // original has to stay visible beside it.
    expect(br.detail?.industriesQuery).toContain("c.description_pt");
    expect(br.detail?.industriesQuery).not.toContain(
      "'' AS description_original",
    );
  });
});
