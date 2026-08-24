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
  FINANCIALS_LATEST_SQL,
  FINANCIAL_REPORTS_SQL,
  SOURCE_VIEWS,
  financialSourceYearsSql,
  loadSeCompanyFinancialDetail,
} from "~/lib/se-company-financial.server";
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

const COMPANY = "5560125220";

/** Every statement the company area sends, with the two view-built ones
 * expanded from their allowlist. */
const ALL_SQL: Array<[string, string]> = [
  ["SHELL_INFO_SQL", SHELL_INFO_SQL],
  ["SHELL_REGISTER_SQL", SHELL_REGISTER_SQL],
  ["SHELL_ENTITY_TYPE_SQL", SHELL_ENTITY_TYPE_SQL],
  ["SHELL_LEGAL_FORM_LABEL_SQL", SHELL_LEGAL_FORM_LABEL_SQL],
  ["ADDRESSES_SQL", ADDRESSES_SQL],
  ["REMOVED_SQL", REMOVED_SQL],
  ["ADDRESS_CORRECTIONS_SQL", ADDRESS_CORRECTIONS_SQL],
  ["ADDRESS_STATUS_INPUTS_SQL", ADDRESS_STATUS_INPUTS_SQL],
  ["FINANCIALS_LATEST_SQL", FINANCIALS_LATEST_SQL],
  ["FINANCIAL_REPORTS_SQL", FINANCIAL_REPORTS_SQL],
  ["PEOPLE_SQL", PEOPLE_SQL],
  ["PEOPLE_ROLES_SQL", PEOPLE_ROLES_SQL],
  ["COMPANY_DOMAINS_SQL", COMPANY_DOMAINS_SQL],
  ...SOURCE_VIEWS.map(
    (source) =>
      [`years(${source.view})`, financialSourceYearsSql(source.view)] as [
        string,
        string,
      ],
  ),
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
      // interpolated; the view builder's own `${view}` is expanded above, so
      // nothing here may carry one.
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
      expect(limit, name).toBeLessThanOrEqual(900);
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
    expect(FINANCIAL_REPORTS_SQL).toContain(
      "corpscout.se_financial_reports AS r FINAL",
    );
    expect(PEOPLE_SQL).toContain("corpscout.se_company_person AS p FINAL");
    expect(PEOPLE_ROLES_SQL).toContain("corpscout.se_company_person_role FINAL");
    expect(PEOPLE_ROLES_SQL).toContain(
      "corpscout.company_person_role_type AS t FINAL",
    );
    expect(COMPANY_DOMAINS_SQL).toContain("corpscout.company_domains AS d FINAL");
    // The address final is a ReplacingMergeTree on resolved_at, so both of its
    // reads take FINAL; the ledger it joins nothing to is a plain MergeTree.
    for (const sql of [ADDRESSES_SQL, REMOVED_SQL, ADDRESS_STATUS_INPUTS_SQL]) {
      expect(sql).toContain("corpscout.se_company_address AS a FINAL");
    }
    expect(ADDRESS_CORRECTIONS_SQL).not.toContain("FINAL");

    // Snapshot tables: no FINAL anywhere in the statements that read them.
    expect(FINANCIALS_LATEST_SQL).not.toContain("FINAL");
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

  it("reads only the two allowlisted financial views", async () => {
    await loadSeCompanyFinancialDetail(COMPANY);
    const sent = clickhouse.query.mock.calls.map(([sql]) => sql as string);
    expect(sent).toContain(FINANCIALS_LATEST_SQL);
    expect(sent).toContain(FINANCIAL_REPORTS_SQL);
    for (const source of SOURCE_VIEWS) {
      expect(sent).toContain(financialSourceYearsSql(source.view));
    }
    expect(sent).toHaveLength(2 + SOURCE_VIEWS.length);
  });

  it("keeps a source's years under its own id, empty ones included", async () => {
    clickhouse.query.mockImplementation(async (sql: string) => {
      if (sql === financialSourceYearsSql("se_financials_bolagsverket_current")) {
        return [{ source_id: "bolagsverket-annual-accounts", fiscal_year: "2025" }];
      }
      return [];
    });
    const detail = await loadSeCompanyFinancialDetail(COMPANY);
    expect(detail.latest).toBeNull();
    expect(detail.sources.map((source) => source.source_id)).toEqual([
      "bolagsverket-annual-accounts",
      "esef",
    ]);
    expect(detail.sources[0].years).toHaveLength(1);
    expect(detail.sources[1].years).toEqual([]);
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
});
