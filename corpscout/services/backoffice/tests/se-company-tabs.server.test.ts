import { beforeEach, describe, expect, it, vi } from "vitest";

// One hoisted chQuery for every tab loader: the SQL each one sends is the
// contract these tests pin, so the mock records the calls rather than
// standing in for a live ClickHouse.
const clickhouse = vi.hoisted(() => ({ insert: vi.fn(), query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({
  chInsertSeCompanyAddressCorrections: clickhouse.insert,
  chQuery: clickhouse.query,
}));

import {
  ADDRESS_STATUS_INPUTS_SQL,
  ADDRESSES_SQL,
  CORRECTIONS_SQL as ADDRESS_CORRECTIONS_SQL,
  REMOVED_SQL,
  loadSeCompanyAddresses,
} from "~/lib/se-company-address.server";
import {
  COMPANY_DOMAINS_SQL,
  loadSeCompanyDomains,
} from "~/lib/se-company-domains.server";
import {
  PEOPLE_ROLES_SQL,
  PEOPLE_SQL,
  loadSeCompanyPeople,
} from "~/lib/se-company-people.server";
import {
  SHELL_ENTITY_TYPE_SQL,
  SHELL_INFO_SQL,
  SHELL_LEGAL_FORM_LABEL_SQL,
  SHELL_REGISTER_SQL,
  loadSeCompanyShell,
} from "~/lib/se-company-shell.server";
import { loadSeCompanyContracts } from "~/lib/se-company-contracts.server";
import {
  COMPANY_JOB_AD_CONTACTS_SQL,
  COMPANY_JOB_AD_REQUIREMENTS_SQL,
  COMPANY_JOB_AD_SQL,
  COMPANY_JOB_AD_VERSION_SQL,
  COMPANY_JOBS_CURRENT_SQL,
  COMPANY_JOBS_SQL,
  loadSeCompanyJobAdDetail,
  loadSeCompanyJobs,
} from "~/lib/se-company-jobs.server";
import {
  COMPANY_LEAD_PRICES_SQL,
  COMPANY_LEI_SQL,
  COMPANY_MARKET_SUMMARY_SQL,
  COMPANY_TRADED_SYMBOLS_SQL,
  computeMarketStats,
  loadSeCompanyListed,
  pickLeadSymbol,
  type SeCompanyPricePoint,
} from "~/lib/se-company-listed.server";
import { getCountry } from "~/lib/countries";

const COMPANY = "5560125220";

/** Every statement the company area's own tab loaders send. The Financial
 * tab is not here: it renders the public financials experience through
 * `getCompanyFinancialDetail` (queries.server), whose SQL has its own tests. */
const ALL_SQL: Array<[string, string]> = [
  ["SHELL_INFO_SQL", SHELL_INFO_SQL],
  ["SHELL_REGISTER_SQL", SHELL_REGISTER_SQL],
  ["SHELL_ENTITY_TYPE_SQL", SHELL_ENTITY_TYPE_SQL],
  ["SHELL_LEGAL_FORM_LABEL_SQL", SHELL_LEGAL_FORM_LABEL_SQL],
  ["ADDRESSES_SQL", ADDRESSES_SQL],
  ["REMOVED_SQL", REMOVED_SQL],
  ["ADDRESS_CORRECTIONS_SQL", ADDRESS_CORRECTIONS_SQL],
  ["ADDRESS_STATUS_INPUTS_SQL", ADDRESS_STATUS_INPUTS_SQL],
  ["PEOPLE_SQL", PEOPLE_SQL],
  ["PEOPLE_ROLES_SQL", PEOPLE_ROLES_SQL],
  ["COMPANY_DOMAINS_SQL", COMPANY_DOMAINS_SQL],
  ["COMPANY_JOBS_SQL", COMPANY_JOBS_SQL],
  ["COMPANY_JOBS_CURRENT_SQL", COMPANY_JOBS_CURRENT_SQL],
  ["COMPANY_JOB_AD_SQL", COMPANY_JOB_AD_SQL],
  ["COMPANY_JOB_AD_VERSION_SQL", COMPANY_JOB_AD_VERSION_SQL],
  ["COMPANY_JOB_AD_REQUIREMENTS_SQL", COMPANY_JOB_AD_REQUIREMENTS_SQL],
  ["COMPANY_JOB_AD_CONTACTS_SQL", COMPANY_JOB_AD_CONTACTS_SQL],
  ["COMPANY_LEI_SQL", COMPANY_LEI_SQL],
  ["COMPANY_TRADED_SYMBOLS_SQL", COMPANY_TRADED_SYMBOLS_SQL],
  ["COMPANY_MARKET_SUMMARY_SQL", COMPANY_MARKET_SUMMARY_SQL],
  ["COMPANY_LEAD_PRICES_SQL", COMPANY_LEAD_PRICES_SQL],
];

beforeEach(() => {
  clickhouse.query.mockReset();
  clickhouse.query.mockResolvedValue([]);
});

describe("company area SQL", () => {
  it("passes every value as a named parameter, never interpolated", () => {
    for (const [name, sql] of ALL_SQL) {
      expect(sql, name).not.toContain(COMPANY);
      // A `${` surviving into a shipped statement means a value was
      // interpolated, so nothing here may carry one.
      expect(sql, name).not.toContain("${");
    }
  });

  /**
   * Every statement that returns ROWS is bounded. The one exception is named
   * rather than skipped: ADDRESS_STATUS_INPUTS_SQL is a per-company aggregate
   * with no GROUP BY, so it returns exactly one row whatever it reads, and
   * bounding it is the very bug review T7-m5 fixed -- a LIMIT there caps the
   * applied-correction ids and flips an applied decision to stale.
   */
  it("bounds every statement that returns rows", () => {
    for (const [name, sql] of ALL_SQL) {
      if (sql === ADDRESS_STATUS_INPUTS_SQL) continue;
      expect(sql, name).toMatch(/LIMIT \d+/);
      const limit = Number(/LIMIT (\d+)/.exec(sql)?.[1] ?? "0");
      // The lead-price series is the one deliberately larger read: five years
      // of ~260 sessions, keyed on one symbol -- still a bounded primary-key
      // scan, not a table walk.
      expect(limit, name).toBeLessThanOrEqual(
        sql === COMPANY_LEAD_PRICES_SQL ? 1500 : 900,
      );
    }
    expect(ADDRESS_STATUS_INPUTS_SQL).not.toMatch(/LIMIT/);
    expect(ADDRESS_STATUS_INPUTS_SQL).not.toContain("GROUP BY");
  });

  // FINAL is not decoration: without it a ReplacingMergeTree hands back every
  // version of a row, so a re-reviewed domain or a re-parsed report shows
  // twice. The MergeTree snapshots below are rebuilt whole per run, where
  // FINAL would be a full dedup pass for no change in the result.
  it("reads FINAL from every Replacing engine and from none of the snapshots", () => {
    for (const table of [
      "corpscout.se_company_info AS i FINAL",
      "corpscout.se_companies AS c FINAL",
      "corpscout.company_entity_types AS t FINAL",
    ]) {
      expect([SHELL_INFO_SQL, SHELL_REGISTER_SQL, SHELL_ENTITY_TYPE_SQL].join("\n"))
        .toContain(table);
    }
    expect(PEOPLE_SQL).toContain("corpscout.se_company_person AS p FINAL");
    expect(PEOPLE_ROLES_SQL).toContain("corpscout.se_company_person_role FINAL");
    expect(PEOPLE_ROLES_SQL).toContain(
      "corpscout.company_person_role_type AS t FINAL",
    );
    expect(COMPANY_DOMAINS_SQL).toContain("corpscout.company_domains AS d FINAL");
    // eodhd_eod_prices is a ReplacingMergeTree on retrieved_at: a re-fetched
    // trading day must show once, in its newest state.
    expect(COMPANY_LEAD_PRICES_SQL).toContain(
      "corpscout.eodhd_eod_prices AS p FINAL",
    );
    // The job tables, company_identifier and the market fact tables are plain
    // MergeTree snapshots rebuilt whole per pipeline run: FINAL there is a
    // dedup pass for nothing.
    for (const sql of [
      COMPANY_JOBS_SQL,
      COMPANY_JOBS_CURRENT_SQL,
      COMPANY_JOB_AD_SQL,
      COMPANY_LEI_SQL,
      COMPANY_MARKET_SUMMARY_SQL,
    ]) {
      expect(sql).not.toContain("FINAL");
    }
    // The raw Platsbanken requirement/contact tables ARE
    // ReplacingMergeTree(ingested_at) and return many rows per ad, so a
    // re-ingested row would show twice without FINAL.
    expect(COMPANY_JOB_AD_REQUIREMENTS_SQL).toContain(
      "corpscout.se_platsbanken_job_ad_requirement_versions AS r FINAL",
    );
    expect(COMPANY_JOB_AD_CONTACTS_SQL).toContain(
      "corpscout.se_platsbanken_job_ad_contact_versions AS c FINAL",
    );
    // The version table is Replacing too, but its sorting key includes
    // version_at, so FINAL cannot fold versions into "the latest" -- that
    // read orders by version_at (ingested_at tiebreak) and takes one row,
    // which makes FINAL a dedup pass for nothing there as well.
    expect(COMPANY_JOB_AD_VERSION_SQL).not.toContain("FINAL");
    expect(COMPANY_JOB_AD_VERSION_SQL).toContain(
      "ORDER BY v.version_at DESC, v.ingested_at DESC",
    );
    expect(COMPANY_JOB_AD_VERSION_SQL).toContain("LIMIT 1");
    // company_traded_symbols itself is a rebuilt-whole snapshot (no FINAL),
    // but the eodhd_symbols dimension it joins IS a ReplacingMergeTree on
    // retrieved_at, so the joined side alone takes FINAL.
    expect(COMPANY_TRADED_SYMBOLS_SQL).not.toContain(
      "company_traded_symbols AS s FINAL",
    );
    expect(COMPANY_TRADED_SYMBOLS_SQL).toContain(
      "corpscout.eodhd_symbols AS es FINAL",
    );
    // The address final is a ReplacingMergeTree on resolved_at, so both of its
    // reads take FINAL; the ledger it joins nothing to is a plain MergeTree.
    for (const sql of [ADDRESSES_SQL, REMOVED_SQL, ADDRESS_STATUS_INPUTS_SQL]) {
      expect(sql).toContain("corpscout.se_company_address AS a FINAL");
    }
    expect(ADDRESS_CORRECTIONS_SQL).not.toContain("FINAL");
  });

  /**
   * The address tab used to read a six-table LEFT JOIN chain
   * (se_company_addresses_current -> display -> members -> links ->
   * se_addresses_current -> geocodes), and ClickHouse's habit of filling a
   * LEFT JOIN miss with each column's *type default* rather than NULL put a
   * geocode confidence of 0 taken on 1970-01-01 on the page. The datatype
   * removed the reason for the chain: the geocode is resolved once and stored
   * on the published row. Pin that, so nobody re-adds a join here.
   */
  it("reads the address final on its own, with no join chain behind it", () => {
    for (const sql of [ADDRESSES_SQL, REMOVED_SQL]) {
      expect(sql).not.toContain("JOIN");
      expect(sql).not.toContain("se_company_addresses_current");
      expect(sql).not.toContain("se_address_geocodes_current");
      expect(sql).toContain("toString(a.geocode_status) AS geocode_status");
    }
  });

  it("filters the role join in a subquery, not in an outer WHERE", () => {
    // ClickHouse 26.5 loses `company_id` from the block when FINAL sits on the
    // left of a LEFT JOIN and the predicate is pushed down
    // (NOT_FOUND_COLUMN_IN_BLOCK), so the filter has to happen first.
    const beforeJoin = PEOPLE_ROLES_SQL.split("LEFT JOIN")[0];
    expect(beforeJoin).toContain("WHERE company_id = {companyId:String}");
  });
});

describe("loadSeCompanyShell", () => {
  it("prefers the published row and marks it published", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === SHELL_INFO_SQL) {
        return [
          {
            company_id: COMPANY,
            legal_name: "Published AB",
            legal_form_code: "AB-ORGFO",
            status: "active",
            incorporation_date: "1915-04-06",
          },
        ];
      }
      if (sql === SHELL_REGISTER_SQL) {
        return [
          {
            company_id: COMPANY,
            legal_name: "Register AB",
            legal_form_code: "AB-ORGFO",
            status: "active",
            incorporation_date: "1915-04-06",
          },
        ];
      }
      if (sql === SHELL_LEGAL_FORM_LABEL_SQL) {
        return [
          {
            label_en: "Limited company (aktiebolag)",
            label_sv: "Aktiebolag",
          },
        ];
      }
      return [{ entity_type_label: "Company", is_public_sector: 0 }];
    });
    const shell = await loadSeCompanyShell(COMPANY);
    expect(shell?.legal_name).toBe("Published AB");
    expect(shell?.published).toBe(true);
    expect(shell?.entity_type_label).toBe("Company");
    expect(shell?.is_public_sector).toBe(false);
    // Both labels come from the curated dictionary, keyed by the code -- the
    // header renders for unpublished companies too, and se_companies carries
    // the code only, so reading the published row's own copies would leave
    // half the company area unlabelled.
    expect(shell?.legal_form_label_sv).toBe("Aktiebolag");
    expect(shell?.legal_form_label_en).toBe("Limited company (aktiebolag)");
    expect(
      clickhouse.query.mock.calls.find(
        ([sql]) => sql === SHELL_LEGAL_FORM_LABEL_SQL,
      )?.[1],
    ).toEqual({ legalFormCode: "AB-ORGFO" });
  });

  it("leaves both labels empty when the dictionary does not name the code", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === SHELL_INFO_SQL) {
        return [
          {
            company_id: COMPANY,
            legal_name: "Published AB",
            legal_form_code: "ZZZ",
            status: "active",
            incorporation_date: "",
          },
        ];
      }
      return [];
    });
    const shell = await loadSeCompanyShell(COMPANY);
    expect(shell?.legal_form_code).toBe("ZZZ");
    expect(shell?.legal_form_label_sv).toBe("");
    expect(shell?.legal_form_label_en).toBe("");
  });

  it("falls back to the register and says the company is not published", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === SHELL_INFO_SQL) return [];
      if (sql === SHELL_REGISTER_SQL) {
        return [
          {
            company_id: COMPANY,
            legal_name: "Register AB",
            legal_form_code: "",
            status: "active",
            incorporation_date: "",
          },
        ];
      }
      return [];
    });
    const shell = await loadSeCompanyShell(COMPANY);
    expect(shell?.legal_name).toBe("Register AB");
    expect(shell?.published).toBe(false);
    // No legal form means nothing to look it up BY, so neither by-code query
    // is sent: not the classification and not the label dictionary.
    for (const sql of [SHELL_ENTITY_TYPE_SQL, SHELL_LEGAL_FORM_LABEL_SQL]) {
      expect(
        clickhouse.query.mock.calls.some(([sent]) => sent === sql),
      ).toBe(false);
    }
    expect(shell?.legal_form_label_sv).toBe("");
  });

  it("is null when neither table knows the id", async () => {
    expect(await loadSeCompanyShell("0000000000")).toBeNull();
  });
});

describe("tab loaders", () => {
  it("reads the live rows, the tombstones and the ledger of one company", async () => {
    await loadSeCompanyAddresses(COMPANY);
    const sent = clickhouse.query.mock.calls.map(([sql]) => sql as string);
    expect(sent).toEqual([
      ADDRESSES_SQL,
      REMOVED_SQL,
      ADDRESS_STATUS_INPUTS_SQL,
      ADDRESS_CORRECTIONS_SQL,
    ]);
    for (const [, params] of clickhouse.query.mock.calls) {
      expect(params).toMatchObject({ companyId: COMPANY });
    }
  });

  it("reads domains with the company id as a parameter", async () => {
    await loadSeCompanyDomains(COMPANY);
    expect(clickhouse.query).toHaveBeenCalledWith(COMPANY_DOMAINS_SQL, {
      companyId: COMPANY,
    });
  });

  it("hangs each person's roles off that person, and leaves roleless ones empty", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === PEOPLE_SQL) {
        return [
          { person_id: "p1", name: "Anna", description: "", draft_count: 3, correction_count: 0, merged_into_person_id: "", updated_at: "2026-08-19 00:00:00.000" },
          { person_id: "p2", name: "Bo", description: "", draft_count: 1, correction_count: 0, merged_into_person_id: "", updated_at: "2026-08-19 00:00:00.000" },
        ];
      }
      return [
        { person_id: "p1", role_code: "board_chair", role_label: "Board chair", role_group: "governance", fiscal_year: "2024", sources: ["esef"], source_count: 1, is_current: 1, first_observed_at: "", last_observed_at: "" },
        { person_id: "p1", role_code: "board_member", role_label: "Board member", role_group: "governance", fiscal_year: "2023", sources: ["esef"], source_count: 1, is_current: 0, first_observed_at: "", last_observed_at: "" },
      ];
    });
    const people = await loadSeCompanyPeople(COMPANY);
    expect(people.map((person) => person.roles.length)).toEqual([2, 0]);
    expect(people[0].roles[0].role_label).toBe("Board chair");
  });

  /**
   * The Contracts tab deliberately owns NO SQL: it sends the exact
   * publicContractsQuery the SE country config declares, with the same `id`
   * parameter the public page binds, so the two can never drift apart.
   */
  it("sends the SE country config's own public contracts query", async () => {
    const seQuery = getCountry("se")!.detail!.publicContractsQuery!;
    // The shape this tab depends on, pinned where the tab reads it.
    expect(seQuery).toContain("FROM se_government_contracts");
    expect(seQuery).toContain("WHERE company_id = {id:String}");
    expect(seQuery).toContain("LIMIT 100");
    await loadSeCompanyContracts(COMPANY);
    expect(clickhouse.query).toHaveBeenCalledWith(seQuery, { id: COMPANY });
  });

  it("marks a job open when the current table still lists it or its interval has no end", async () => {
    const history = {
      source_system: "platsbanken",
      source_job_ad_id: "1",
      interval_number: 1,
      active_from: "2026-05-04 08:00:00.000",
      active_to: "2026-06-30 21:59:59.000",
      active_to_basis: "application_deadline",
      is_end_estimated: 1,
      publication_at: "",
      application_deadline: "",
      employer_name: "AB",
      headline_original: "Säljare",
    };
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === COMPANY_JOBS_SQL) {
        return [
          history,
          { ...history, source_job_ad_id: "2" },
          { ...history, source_job_ad_id: "3", active_to: "" },
        ];
      }
      return [{ source_system: "platsbanken", source_job_ad_id: "2" }];
    });
    const jobs = await loadSeCompanyJobs(COMPANY);
    expect(jobs.map((job) => job.is_open)).toEqual([0, 1, 1]);
    for (const [, params] of clickhouse.query.mock.calls) {
      expect(params).toEqual({ companyId: COMPANY });
    }
  });

  it("enriches the job list without ever shipping the full ad text in it", () => {
    // description_text_original is the FULL ad body (large, ZSTD(6) on disk):
    // it belongs to the one-ad detail read only, never the 200-row list.
    expect(COMPANY_JOBS_SQL).not.toContain("description_text_original");
    expect(COMPANY_JOBS_CURRENT_SQL).not.toContain("description_text_original");
    for (const column of [
      "occupation_label",
      "municipality_name",
      "region_name",
      "employment_type_label",
      "working_hours_label",
      "number_of_vacancies",
      "webpage_url",
    ]) {
      expect(COMPANY_JOBS_SQL).toContain(column);
    }
    // Archive eras differ in which taxonomy level they filled in, so the
    // occupation column falls back to the GROUP label.
    expect(COMPANY_JOBS_SQL).toContain("occupation_group_label_original");
  });

  const AD_ID = "29112166";
  const VERSION_UID = "a".repeat(64);
  const adRow = {
    source_job_ad_id: AD_ID,
    headline_original: "Säljare till Beijer Bygg i Luleå",
    description_text_original: "Om rollen\n\nDu säljer byggmaterial.",
    detected_language: "sv",
    webpage_url: `https://arbetsformedlingen.se/platsbanken/annonser/${AD_ID}`,
  };
  const versionRow = {
    version_uid: VERSION_UID,
    salary_type_label: "Fast månads- vecko- eller timlön",
    salary_description: "",
    scope_min: 50,
    scope_max: 100,
    experience_required: 1,
    driving_license_required: 0,
    access_to_own_car: null,
    employer_workplace: "Beijer Luleå",
    street_address: "Storgatan 1",
    postcode: "97231",
    city: "Luleå",
    application_email: "",
    application_url: "https://example.com/apply",
    application_information: "",
  };

  it("keys every read by the validated ad and takes the LATEST version's rows", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === COMPANY_JOB_AD_SQL) return [adRow];
      if (sql === COMPANY_JOB_AD_VERSION_SQL) return [versionRow];
      if (sql === COMPANY_JOB_AD_REQUIREMENTS_SQL) {
        return [
          { requirement_level: "must_have", requirement_type: "work_experience", label_original: "Säljare", weight: 10 },
          { requirement_level: "nice_to_have", requirement_type: "language", label_original: "Engelska", weight: null },
        ];
      }
      return [
        { contact_index: 0, name: "Anna Ek", description: "", email: "anna@beijer.se", telephone: "+46 70 000 00 00", contact_type: "Rekryterande chef" },
      ];
    });
    const detail = await loadSeCompanyJobAdDetail(COMPANY, AD_ID);
    // The ownership gate runs FIRST; the raw per-ad tables are consulted only
    // after the keyed history read vouched for the id, and the requirement
    // and contact reads are pinned to the version the version read returned.
    expect(clickhouse.query.mock.calls.map(([sql]) => sql)).toEqual([
      COMPANY_JOB_AD_SQL,
      COMPANY_JOB_AD_VERSION_SQL,
      COMPANY_JOB_AD_REQUIREMENTS_SQL,
      COMPANY_JOB_AD_CONTACTS_SQL,
    ]);
    expect(clickhouse.query).toHaveBeenCalledWith(COMPANY_JOB_AD_SQL, {
      companyId: COMPANY,
      adId: AD_ID,
    });
    expect(clickhouse.query).toHaveBeenCalledWith(
      COMPANY_JOB_AD_REQUIREMENTS_SQL,
      { adId: AD_ID, versionUid: VERSION_UID },
    );
    expect(clickhouse.query).toHaveBeenCalledWith(COMPANY_JOB_AD_CONTACTS_SQL, {
      adId: AD_ID,
      versionUid: VERSION_UID,
    });
    expect(detail?.description_text_original).toBe(adRow.description_text_original);
    expect(detail?.extras).not.toBeNull();
    expect(detail?.extras).not.toHaveProperty("version_uid");
    expect(detail?.extras?.scope_min).toBe(50);
    expect(detail?.requirements).toHaveLength(2);
    expect(detail?.contacts[0].name).toBe("Anna Ek");
  });

  it("is null for an ad this company does not own, and stops at the gate", async () => {
    const detail = await loadSeCompanyJobAdDetail(COMPANY, "999999");
    expect(detail).toBeNull();
    // No history row means NO raw-table read happens for the untrusted id.
    expect(clickhouse.query.mock.calls.map(([sql]) => sql)).toEqual([
      COMPANY_JOB_AD_SQL,
    ]);
  });

  it("still returns the description when the ad has no raw version row", async () => {
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === COMPANY_JOB_AD_SQL ? [adRow] : [],
    );
    const detail = await loadSeCompanyJobAdDetail(COMPANY, AD_ID);
    expect(detail).toEqual({
      ...adRow,
      extras: null,
      requirements: [],
      contacts: [],
    });
    // With no version there is no version_uid to key on, so neither the
    // requirement nor the contact read is sent.
    expect(clickhouse.query.mock.calls.map(([sql]) => sql)).toEqual([
      COMPANY_JOB_AD_SQL,
      COMPANY_JOB_AD_VERSION_SQL,
    ]);
  });

  it("keys the gate by company AND ad, and the raw reads by ad and version", () => {
    expect(COMPANY_JOB_AD_SQL).toContain("h.country_code = 'SE'");
    expect(COMPANY_JOB_AD_SQL).toContain("h.company_id = {companyId:String}");
    expect(COMPANY_JOB_AD_SQL).toContain(
      "h.source_job_ad_id = {adId:String}",
    );
    // A republished ad has several intervals; the newest one's text wins.
    expect(COMPANY_JOB_AD_SQL).toContain("ORDER BY h.interval_number DESC");
    expect(COMPANY_JOB_AD_SQL).toContain("LIMIT 1");
    for (const sql of [
      COMPANY_JOB_AD_REQUIREMENTS_SQL,
      COMPANY_JOB_AD_CONTACTS_SQL,
    ]) {
      expect(sql).toContain("source_job_ad_id = {adId:String}");
      expect(sql).toContain("version_uid = {versionUid:String}");
    }
  });

  /**
   * The Publicly traded tab reads the EODHD market facts, not ESEF: a filing
   * is a reporting fact, not trading information. The identity reads are
   * scoped by (country_code, company_id) -- a bare company_id is not unique
   * across registers -- and the price read is keyed on the price table's own
   * primary key (eodhd_symbol_key), never a scan.
   */
  it("reads symbols, summary and the LEIs, then prices for the LEAD symbol only", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === COMPANY_TRADED_SYMBOLS_SQL) {
        return [
          { isin: "SE0007100599", eodhd_symbol_key: "0R7S.LSE", ticker: "0R7S", exchange_code: "LSE", symbol_name: "Svenska Handelsbanken AB (publ)", instrument_type: "Common Stock", quote_currency: "SEK", is_delisted: 0 },
          { isin: "SE0007100599", eodhd_symbol_key: "SHB-A.ST", ticker: "SHB-A", exchange_code: "ST", symbol_name: "Svenska Handelsbanken AB (publ)", instrument_type: "Common Stock", quote_currency: "SEK", is_delisted: 0 },
        ];
      }
      if (sql === COMPANY_MARKET_SUMMARY_SQL) {
        return [
          { year: "2025", venues: "4", lead_venue: "ST", lead_currency: "SEK", last_close: "122.15", last_day: "2025-12-30", traded_usd: "31500000000.00" },
        ];
      }
      if (sql === COMPANY_LEAD_PRICES_SQL) {
        return [{ price_date: "2025-12-30", close: 122.15, high: 123.1, low: 121.0, adjusted_close: 122.15, volume: 5_000_000 }];
      }
      return [];
    });
    const listed = await loadSeCompanyListed(COMPANY);
    const sent = clickhouse.query.mock.calls.map(([sql]) => sql as string);
    expect(sent).toEqual([
      COMPANY_LEI_SQL,
      COMPANY_TRADED_SYMBOLS_SQL,
      COMPANY_MARKET_SUMMARY_SQL,
      COMPANY_LEAD_PRICES_SQL,
    ]);
    for (const [sql, params] of clickhouse.query.mock.calls) {
      if (sql === COMPANY_LEAD_PRICES_SQL) {
        // The chart follows the QUOTE: SHB-A.ST is read because the summary's
        // lead venue is ST, even though the LSE line sorts first.
        expect(params).toEqual({ symbolKey: "SHB-A.ST" });
      } else {
        expect(params).toEqual({ companyId: COMPANY });
      }
    }
    expect(listed.leadSymbolKey).toBe("SHB-A.ST");
    expect(listed.summary).toEqual({
      year: 2025,
      venues: 4,
      lead_venue: "ST",
      lead_currency: "SEK",
      last_close: 122.15,
      last_day: "2025-12-30",
      traded_usd: 31500000000,
    });
    expect(listed.prices).toEqual([
      { price_date: "2025-12-30", close: 122.15, high: 123.1, low: 121.0, adjusted_close: 122.15, volume: 5_000_000 },
    ]);
    // Stats are derived from the loaded series, never a fifth query; their
    // values are pinned in the computeMarketStats suite (the loader's
    // reference date is the wall clock, so only the shape is asserted here).
    expect(listed.stats?.returns).toHaveLength(4);

    // The shape this tab depends on: EODHD market tables only -- ESEF is not
    // trading information and must never come back to this loader.
    expect(COMPANY_TRADED_SYMBOLS_SQL).toContain("corpscout.company_traded_symbols");
    expect(COMPANY_MARKET_SUMMARY_SQL).toContain("corpscout.company_market_summary");
    expect(COMPANY_LEAD_PRICES_SQL).toContain("corpscout.eodhd_eod_prices");
    for (const [name, sql] of ALL_SQL) {
      expect(sql, name).not.toContain("esef_filings");
    }
    for (const sql of [COMPANY_TRADED_SYMBOLS_SQL, COMPANY_MARKET_SUMMARY_SQL]) {
      expect(sql).toContain("country_code = 'SE'");
      expect(sql).toContain("company_id = {companyId:String}");
    }
    // The summary reads ONE row -- the most recent year -- and the price read
    // is keyed and bounded to a trading year.
    expect(COMPANY_MARKET_SUMMARY_SQL).toContain("ORDER BY m.year DESC");
    // Every traded year plus headroom -- the per-year table needs them all.
    expect(COMPANY_MARKET_SUMMARY_SQL).toContain("LIMIT 50");
    expect(COMPANY_LEAD_PRICES_SQL).toContain(
      "eodhd_symbol_key = {symbolKey:String}",
    );
    expect(COMPANY_LEAD_PRICES_SQL).toContain(
      "price_date >= today() - INTERVAL 5 YEAR",
    );
    // The stat strip needs TRUE intraday high/low, the adjusted close for
    // returns and the volume — all Nullable, all passed through as null.
    for (const column of [
      "toFloat64(p.high) AS high",
      "toFloat64(p.low) AS low",
      "toFloat64(p.adjusted_close) AS adjusted_close",
      "toFloat64(p.volume) AS volume",
    ]) {
      expect(COMPANY_LEAD_PRICES_SQL).toContain(column);
    }
    // The listings enrichment joins the EODHD symbol dimension; every joined
    // column is ifNull-folded so a join miss reads '' / 0 under BOTH
    // join_use_nulls settings.
    expect(COMPANY_TRADED_SYMBOLS_SQL).toContain(
      "LEFT JOIN corpscout.eodhd_symbols AS es FINAL",
    );
    expect(COMPANY_TRADED_SYMBOLS_SQL).toContain(
      "ON es.eodhd_symbol_key = s.eodhd_symbol_key",
    );
    for (const column of [
      "ifNull(es.symbol_name, '') AS symbol_name",
      "ifNull(toString(es.instrument_type), '') AS instrument_type",
      "ifNull(toString(es.currency), '') AS quote_currency",
      "toUInt8(ifNull(es.is_delisted, 0)) AS is_delisted",
    ]) {
      expect(COMPANY_TRADED_SYMBOLS_SQL).toContain(column);
    }
    expect(COMPANY_LEI_SQL).toContain("is_current = 1");
  });

  it("skips the price read entirely when no symbol resolves", async () => {
    const listed = await loadSeCompanyListed(COMPANY);
    expect(
      clickhouse.query.mock.calls.some(([sql]) => sql === COMPANY_LEAD_PRICES_SQL),
    ).toBe(false);
    expect(listed.symbols).toEqual([]);
    expect(listed.summary).toBeNull();
    expect(listed.leadSymbolKey).toBe("");
    expect(listed.prices).toEqual([]);
    expect(listed.stats).toBeNull();
  });

  it("falls back to the first symbol when no line sits on the lead venue", () => {
    const enrichment = { symbol_name: "", instrument_type: "", quote_currency: "", is_delisted: 0 };
    const lse = { isin: "SE1", eodhd_symbol_key: "0R7S.LSE", ticker: "0R7S", exchange_code: "LSE", ...enrichment };
    const st = { isin: "SE1", eodhd_symbol_key: "SHB-A.ST", ticker: "SHB-A", exchange_code: "ST", ...enrichment };
    const summary = {
      year: 2025, venues: 2, lead_venue: "XETRA", lead_currency: "EUR",
      last_close: 10, last_day: "2025-12-30", traded_usd: 1,
    };
    expect(pickLeadSymbol([lse, st], summary)).toBe(lse);
    expect(pickLeadSymbol([lse, st], { ...summary, lead_venue: "ST" })).toBe(st);
    expect(pickLeadSymbol([lse, st], null)).toBe(lse);
    expect(pickLeadSymbol([], summary)).toBeNull();
  });
});

/** A session with every nullable field defaulted null, overridable. */
function pricePoint(
  price_date: string,
  close: number,
  extra: Partial<SeCompanyPricePoint> = {},
): SeCompanyPricePoint {
  return {
    price_date,
    close,
    high: null,
    low: null,
    adjusted_close: null,
    volume: null,
    ...extra,
  };
}

describe("computeMarketStats", () => {
  const TODAY = "2026-08-28";

  /** Handelsbanken-shaped five-year series with sparse sessions at each
   * window boundary. Returns use adjusted_close (deliberately far from the
   * raw close so a close-based computation would fail loudly). */
  const series: SeCompanyPricePoint[] = [
    pricePoint("2021-09-01", 50, { adjusted_close: 40, high: 52, low: 48, volume: 1_000_000 }),
    // Last session on or before the 1Y cutoff (2025-08-28) — and OUTSIDE the
    // trailing-365d window, so its extreme volume must not enter the average.
    pricePoint("2025-08-27", 100, { adjusted_close: 90, high: 101, low: 99, volume: 9_000_000 }),
    // First session of the current calendar year: the YTD baseline.
    pricePoint("2026-01-02", 110, { adjusted_close: 100, high: 112, low: 108, volume: 1000 }),
    // Last session on or before the 1M cutoff (2026-07-29); a null-volume day.
    pricePoint("2026-07-28", 120, { adjusted_close: 115, high: 125, low: 118, volume: null }),
    pricePoint("2026-08-27", 121, { adjusted_close: 120, high: 122, low: 119.5, volume: 3000 }),
  ];

  it("computes the 52-week range from TRUE highs/lows inside the trailing year", () => {
    const stats = computeMarketStats(series, TODAY)!;
    expect(stats.high52w).toBe(125);
    expect(stats.low52w).toBe(108);
  });

  it("averages volume over the trailing year, ignoring null-volume days", () => {
    const stats = computeMarketStats(series, TODAY)!;
    // (1000 + 3000) / 2 -- the 2025-08-27 session is outside the window and
    // the 2026-07-28 null-volume day is not a divisor.
    expect(stats.avgVolume).toBe(2000);
  });

  it("computes 1M, YTD, 1Y and 5Y returns from the adjusted close", () => {
    const stats = computeMarketStats(series, TODAY)!;
    const byLabel = Object.fromEntries(
      stats.returns.map((entry) => [entry.label, entry.value]),
    );
    expect(byLabel["1M"]).toBeCloseTo(120 / 115 - 1, 10);
    expect(byLabel["YTD"]).toBeCloseTo(0.2, 10);
    expect(byLabel["1Y"]).toBeCloseTo(120 / 90 - 1, 10);
    // The span really is ~5 years, so the label earns "5Y".
    expect(stats.returns[3].label).toBe("5Y");
    expect(byLabel["5Y"]).toBeCloseTo(2, 10);
  });

  it("falls back to the raw close on endpoints whose adjusted close is null", () => {
    const stats = computeMarketStats(
      [
        pricePoint("2026-07-01", 100),
        pricePoint("2026-08-27", 110),
      ],
      TODAY,
    )!;
    const oneMonth = stats.returns.find((entry) => entry.label === "1M")!;
    expect(oneMonth.value).toBeCloseTo(0.1, 10);
  });

  it("is 0 on the first session of the year and null for windows the series does not reach", () => {
    const stats = computeMarketStats(
      [
        pricePoint("2025-12-30", 100, { adjusted_close: 100 }),
        pricePoint("2026-01-02", 105, { adjusted_close: 105 }),
      ],
      "2026-01-02",
    )!;
    const byLabel = Object.fromEntries(
      stats.returns.map((entry) => [entry.label, entry.value]),
    );
    // YTD measures from the first session of the CURRENT calendar year.
    expect(byLabel["YTD"]).toBe(0);
    // The series starts after both cutoffs, so neither window has a baseline.
    expect(byLabel["1M"]).toBeNull();
    expect(byLabel["1Y"]).toBeNull();
  });

  it("labels a short series 'Since {year}' instead of claiming 5Y", () => {
    const stats = computeMarketStats(
      [
        pricePoint("2024-03-01", 80, { adjusted_close: 80 }),
        pricePoint("2026-08-27", 100, { adjusted_close: 100 }),
      ],
      TODAY,
    )!;
    expect(stats.returns[3].label).toBe("Since 2024");
    expect(stats.returns[3].value).toBeCloseTo(0.25, 10);
  });

  it("reports null volume and range when the window has nothing to say", () => {
    // Every session lacks volume -> no average, not an average of zeros.
    const noVolume = computeMarketStats(
      [pricePoint("2026-08-01", 10), pricePoint("2026-08-27", 11)],
      TODAY,
    )!;
    expect(noVolume.avgVolume).toBeNull();
    // A day without high/low still bounds the range through its close.
    expect(noVolume.high52w).toBe(11);
    expect(noVolume.low52w).toBe(10);
    // A stale series -- nothing inside the trailing year -- has no range.
    const stale = computeMarketStats(
      [pricePoint("2024-01-02", 10, { volume: 500 })],
      TODAY,
    )!;
    expect(stale.high52w).toBeNull();
    expect(stale.low52w).toBeNull();
    expect(stale.avgVolume).toBeNull();
  });

  it("is null for an empty series", () => {
    expect(computeMarketStats([], TODAY)).toBeNull();
  });
});
