import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  countForFilter,
  GEOCODE_LIST_FILTER_SQL,
  GEOCODING_COUNTS_SQL,
  GEOCODING_LIST_SELECT_SQL,
  listSeCompanyGeocodingPage,
  loadSeCompanyGeocodingCounts,
  PRIMARY_GEOCODE_CLASS_COLUMN,
  SE_COMPANIES_CURRENT_TABLE,
} from "~/lib/se-company-geocoding-list.server";
import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import { GEOCODE_LIST_FILTERS } from "~/lib/se-company-geocoding-filters";

describe("the serving view: read directly, NO FINAL, NO join", () => {
  it("the list SELECT reads corpscout.se_companies_current as a plain table", () => {
    // The whole point of Task 3: the tab used to recompute FINAL merges on
    // se_company_address/se_company_info + the served-overlay join + a
    // primary-address pick on every request (~20s). It now scans the
    // per-company serving MV (migration 000326), which paid all of that once at
    // refresh time. Pinned whole so no FINAL/join can creep back in.
    expect(SE_COMPANIES_CURRENT_TABLE).toBe("corpscout.se_companies_current");
    expect(GEOCODING_LIST_SELECT_SQL).toBe(`SELECT
  company_id AS company_id,
  legal_name AS legal_name,
  primary_street_address AS street_address,
  primary_postal_code AS postal_code,
  primary_city AS city,
  primary_geocode_status AS geocode_status,
  primary_geocode_precision AS geocode_precision,
  primary_geocode_provider AS geocode_provider,
  primary_geocode_class AS geocode_class
FROM corpscout.se_companies_current`);
  });

  it("neither the list nor the counts query FINALs anything or joins any table", () => {
    for (const sql of [GEOCODING_LIST_SELECT_SQL, GEOCODING_COUNTS_SQL]) {
      expect(sql).toContain("FROM corpscout.se_companies_current");
      expect(sql).not.toContain("FINAL");
      expect(sql).not.toContain("JOIN");
      // The pre-repoint inputs the MV subsumed -- none may be read here anymore.
      expect(sql).not.toContain("se_company_address");
      expect(sql).not.toContain("se_company_info");
      expect(sql).not.toContain("se_address_geocodes_served");
      expect(sql).not.toContain("se_address_geocodes_current");
    }
  });

  it("the list projects the primary address's display fields aliased to the row's own names", () => {
    expect(GEOCODING_LIST_SELECT_SQL).toContain("legal_name AS legal_name");
    expect(GEOCODING_LIST_SELECT_SQL).toContain(
      "primary_street_address AS street_address",
    );
    expect(GEOCODING_LIST_SELECT_SQL).toContain("primary_postal_code AS postal_code");
    expect(GEOCODING_LIST_SELECT_SQL).toContain("primary_city AS city");
    // The raw status the badge tooltip still shows -- off the view's primary column.
    expect(GEOCODING_LIST_SELECT_SQL).toContain(
      "primary_geocode_status AS geocode_status",
    );
    // The two columns that tell a coarse row apart from an exact one.
    expect(GEOCODING_LIST_SELECT_SQL).toContain(
      "primary_geocode_precision AS geocode_precision",
    );
    expect(GEOCODING_LIST_SELECT_SQL).toContain(
      "primary_geocode_provider AS geocode_provider",
    );
    // The precomputed class -- read, never re-derived here.
    expect(GEOCODING_LIST_SELECT_SQL).toContain(
      "primary_geocode_class AS geocode_class",
    );
  });
});

describe("GEOCODE_LIST_FILTER_SQL", () => {
  it("builds every toggle predicate from the precomputed primary_geocode_class column", () => {
    expect(PRIMARY_GEOCODE_CLASS_COLUMN).toBe("primary_geocode_class");
    expect(GEOCODE_LIST_FILTER_SQL).toEqual({
      needs_attention: `primary_geocode_class != 'geocoded'`,
      all: "1",
      geocoded: `primary_geocode_class = 'geocoded'`,
      coarse: `primary_geocode_class = 'coarse'`,
      ambiguous: `primary_geocode_class = 'ambiguous'`,
      unmatched: `primary_geocode_class = 'unmatched'`,
      no_outcome: `primary_geocode_class = 'no_outcome'`,
    });
    // Every catalog filter has a predicate, and vice versa: a filter added to
    // the URL-facing catalog without a predicate here would page a class the
    // toggle can select but the server can never actually filter to.
    expect(Object.keys(GEOCODE_LIST_FILTER_SQL).sort()).toEqual(
      [...GEOCODE_LIST_FILTERS].sort(),
    );
  });

  it("keeps 'coarse' a distinct class from 'geocoded' -- the coarse-vs-geocoded distinction the MV computes", () => {
    // The MV's primary_geocode_class already ran the coarse-before-geocoded
    // check (centroid_fallback provider -> 'coarse' BEFORE the geocoded-status
    // membership check), so a coarse centroid can never carry the value
    // 'geocoded'. These two predicates therefore select disjoint sets, and
    // 'coarse' is NOT a flavour of 'geocoded'.
    expect(GEOCODE_LIST_FILTER_SQL.coarse).toBe(`primary_geocode_class = 'coarse'`);
    expect(GEOCODE_LIST_FILTER_SQL.geocoded).toBe(`primary_geocode_class = 'geocoded'`);
    expect(GEOCODE_LIST_FILTER_SQL.coarse).not.toBe(GEOCODE_LIST_FILTER_SQL.geocoded);
  });

  it("'needs_attention' is `!= 'geocoded'`, never spelled as the coarse/ambiguous/unmatched/no_outcome OR directly", () => {
    // If it were spelled as an OR of the narrow predicates instead of
    // `!= 'geocoded'`, adding a sixth class later would silently leave
    // "needs attention" not counting it. Asserting the literal `!=` form
    // guards that -- and confirms a coarse row DOES need attention (it is not
    // 'geocoded').
    expect(GEOCODE_LIST_FILTER_SQL.needs_attention).toBe(
      `primary_geocode_class != 'geocoded'`,
    );
  });
});

describe("listSeCompanyGeocodingPage", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("defaults to needs_attention, orders by company_id, and pages with named LIMIT/OFFSET params", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    const page = await listSeCompanyGeocodingPage({
      filter: "needs_attention",
      page: 1,
      pageSize: 50,
    });

    expect(page).toEqual({ rows: [] });
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain(GEOCODING_LIST_SELECT_SQL);
    expect(sql).toContain(`WHERE ${GEOCODE_LIST_FILTER_SQL.needs_attention}`);
    expect(sql).toContain("ORDER BY company_id ASC");
    expect(sql).toContain(PAGE_LIMIT_OFFSET_SQL);
    expect(params).toEqual({ limit: 50, offset: 0 });
  });

  it("threads every catalog filter into its own WHERE clause", async () => {
    for (const filter of GEOCODE_LIST_FILTERS) {
      clickhouse.query.mockReset();
      clickhouse.query.mockResolvedValueOnce([]);
      await listSeCompanyGeocodingPage({ filter, page: 1, pageSize: 50 });
      const [sql] = clickhouse.query.mock.calls[0];
      expect(sql).toContain(`WHERE ${GEOCODE_LIST_FILTER_SQL[filter]}`);
    }
  });

  it("clamps pageSize to [10, 200] and computes offset from page", async () => {
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyGeocodingPage({ filter: "all", page: 3, pageSize: 5 });
    expect(clickhouse.query.mock.calls[0][1]).toEqual({ limit: 10, offset: 20 });

    clickhouse.query.mockClear();
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyGeocodingPage({ filter: "all", page: 2, pageSize: 500 });
    expect(clickhouse.query.mock.calls[0][1]).toEqual({ limit: 200, offset: 200 });
  });
});

describe("loadSeCompanyGeocodingCounts", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("counts total and every class in ONE scan of the serving view -- no GROUP BY", async () => {
    clickhouse.query.mockResolvedValueOnce([
      {
        total: "3523532",
        needs_attention: "1658184",
        geocoded: "1865348",
        coarse: "204817",
        ambiguous: "287000",
        unmatched: "1166366",
        no_outcome: "1",
      },
    ]);

    const counts = await loadSeCompanyGeocodingCounts();

    expect(counts).toEqual({
      total: 3523532,
      needsAttention: 1658184,
      geocoded: 1865348,
      coarse: 204817,
      ambiguous: 287000,
      unmatched: 1166366,
      noOutcome: 1,
    });
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    expect(clickhouse.query.mock.calls[0][0]).toBe(GEOCODING_COUNTS_SQL);
    expect(GEOCODING_COUNTS_SQL).not.toContain("GROUP BY");
    // The buckets read the precomputed class column, not a re-derivation.
    expect(GEOCODING_COUNTS_SQL).toContain(`countIf(${GEOCODE_LIST_FILTER_SQL.coarse})`);
    expect(GEOCODING_COUNTS_SQL).toContain(PRIMARY_GEOCODE_CLASS_COLUMN);
  });

  it("returns zeroes (never throws) when the view has no rows", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await loadSeCompanyGeocodingCounts()).toEqual({
      total: 0,
      needsAttention: 0,
      geocoded: 0,
      coarse: 0,
      ambiguous: 0,
      unmatched: 0,
      noOutcome: 0,
    });
  });
});

describe("countForFilter", () => {
  it("picks the counts field matching each catalog filter, so the table never runs a second count() query", () => {
    const counts = {
      total: 10,
      needsAttention: 7,
      geocoded: 3,
      coarse: 2,
      ambiguous: 1,
      unmatched: 4,
      noOutcome: 1,
    };
    expect(countForFilter(counts, "all")).toBe(10);
    expect(countForFilter(counts, "needs_attention")).toBe(7);
    expect(countForFilter(counts, "geocoded")).toBe(3);
    expect(countForFilter(counts, "coarse")).toBe(2);
    expect(countForFilter(counts, "ambiguous")).toBe(1);
    expect(countForFilter(counts, "unmatched")).toBe(4);
    expect(countForFilter(counts, "no_outcome")).toBe(1);
  });
});
