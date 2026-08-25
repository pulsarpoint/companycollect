import { readFileSync } from "node:fs";
import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  countForFilter,
  GEOCODE_LIST_FILTER_SQL,
  GEOCODE_STATUS_CLASS_EXPR,
  geocodeClassExpr,
  GEOCODED_MATCH_STATUSES,
  GEOCODING_COUNTS_SQL,
  GEOCODING_LIST_SELECT_SQL,
  GEOCODING_PUBLISHED_ADDRESS_SQL,
  GEOCODING_PUBLISHED_CTE_SQL,
  listSeCompanyGeocodingPage,
  loadSeCompanyGeocodingCounts,
} from "~/lib/se-company-geocoding-list.server";
import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";
import { GEOCODE_LIST_FILTERS } from "~/lib/se-company-geocoding-filters";

describe("GEOCODING_PUBLISHED_ADDRESS_SQL: the per-company primary-address pick", () => {
  it("reads se_company_address FINAL, live rows only, and NOTHING else", () => {
    // Pinned whole: this is the one join path the task requires -- reusing
    // se-company-address.server.ts's own table and its `geocode_status`
    // column, never a fresh join to se_address_geocodes_current.
    expect(GEOCODING_PUBLISHED_ADDRESS_SQL).toBe(`SELECT
    a.company_id AS company_id,
    ifNull(a.street_address, '') AS street_address,
    ifNull(a.postal_code, '') AS postal_code,
    ifNull(a.city, '') AS city,
    toString(a.geocode_status) AS geocode_status
  FROM corpscout.se_company_address AS a FINAL
  WHERE a.is_current
  ORDER BY
    a.company_id,
    a.address_type = 'visiting_or_postal' DESC,
    a.address_type = 'visiting' DESC,
    a.address_key ASC
  LIMIT 1 BY a.company_id`);
    expect(GEOCODING_PUBLISHED_ADDRESS_SQL).not.toContain(
      "se_address_geocodes_current",
    );
    expect(GEOCODING_PUBLISHED_ADDRESS_SQL).not.toContain(
      "se_company_address_links_current",
    );
  });

  it("is reused verbatim by the shared CTE both the row list and the counts query start from", () => {
    // Two CTEs, not one: `published` is the raw per-company address pick,
    // `published_companies` folds in the se_company_info join. Pinned whole
    // so the join can never be re-added to one query's FROM without the
    // other -- see the fix below.
    expect(GEOCODING_PUBLISHED_CTE_SQL).toBe(`WITH published AS (
${GEOCODING_PUBLISHED_ADDRESS_SQL}
),
published_companies AS (
  SELECT
    p.company_id AS company_id,
    i.legal_name AS legal_name,
    p.street_address AS street_address,
    p.postal_code AS postal_code,
    p.city AS city,
    p.geocode_status AS geocode_status
  FROM published AS p
  INNER JOIN corpscout.se_company_info AS i FINAL ON i.company_id = p.company_id
)`);
    expect(GEOCODING_LIST_SELECT_SQL).toContain(GEOCODING_PUBLISHED_CTE_SQL);
    expect(GEOCODING_COUNTS_SQL).toContain(GEOCODING_PUBLISHED_CTE_SQL);
  });

  it("gives the row list and the counts query the exact same FROM -- neither adds a join of its own", () => {
    // The fix this pins: GEOCODING_COUNTS_SQL used to count `published` alone
    // (pre-join) while GEOCODING_LIST_SELECT_SQL additionally INNER JOINed
    // se_company_info, so an orphaned address (0 today, unguarded) would have
    // counted in the header strip and the pagination total while never
    // appearing as a row. Both queries must FROM `published_companies` --
    // the CTE that already carries the join -- and neither may spell
    // "INNER JOIN" a second time outside GEOCODING_PUBLISHED_CTE_SQL.
    expect(GEOCODING_LIST_SELECT_SQL).toContain("FROM published_companies AS p");
    expect(GEOCODING_COUNTS_SQL).toContain("FROM published_companies AS p");
    for (const sql of [GEOCODING_LIST_SELECT_SQL, GEOCODING_COUNTS_SQL]) {
      const afterCte = sql.slice(GEOCODING_PUBLISHED_CTE_SQL.length);
      expect(afterCte).not.toContain("JOIN");
      expect(afterCte).not.toContain("FROM published AS p");
    }
  });
});

describe("geocodeClassExpr / GEOCODE_STATUS_CLASS_EXPR", () => {
  it("classifies '' as no_outcome before checking membership, matched_* as geocoded, 'ambiguous' on its own, and everything else as unmatched", () => {
    // Pinned whole: postal_box, invalid_address, foreign_address and
    // property_identifier are all real, distinct outcomes -- none of them
    // geocoded per Dagster's own GEOCODED_STATUSES (geocode_store.py) -- so
    // this multiIf must fall through to 'unmatched' for every one of them via
    // the trailing default, not an explicit branch that could omit one.
    expect(geocodeClassExpr("x")).toBe(`multiIf(
    x = '', 'no_outcome',
    x IN ('matched_exact', 'matched_corrected', 'matched_site', 'matched_area', 'matched_street'), 'geocoded',
    x = 'ambiguous', 'ambiguous',
    'unmatched'
  )`);
    expect(GEOCODE_STATUS_CLASS_EXPR).toBe(geocodeClassExpr("p.geocode_status"));
  });

  it("puts the empty-string branch before the IN check (branch order the multiIf itself depends on)", () => {
    const emptyIdx = GEOCODE_STATUS_CLASS_EXPR.indexOf("'no_outcome'");
    const geocodedIdx = GEOCODE_STATUS_CLASS_EXPR.indexOf("'geocoded'");
    const ambiguousIdx = GEOCODE_STATUS_CLASS_EXPR.indexOf("'ambiguous'");
    const unmatchedIdx = GEOCODE_STATUS_CLASS_EXPR.indexOf("'unmatched'");
    expect(emptyIdx).toBeGreaterThan(-1);
    expect(emptyIdx).toBeLessThan(geocodedIdx);
    expect(geocodedIdx).toBeLessThan(ambiguousIdx);
    expect(ambiguousIdx).toBeLessThan(unmatchedIdx);
  });
});

describe("GEOCODE_LIST_FILTER_SQL", () => {
  it("builds every toggle predicate from the SAME class expression the row list and counts strip project", () => {
    expect(GEOCODE_LIST_FILTER_SQL).toEqual({
      needs_attention: `(${GEOCODE_STATUS_CLASS_EXPR}) != 'geocoded'`,
      all: "1",
      geocoded: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'geocoded'`,
      ambiguous: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'ambiguous'`,
      unmatched: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'unmatched'`,
      no_outcome: `(${GEOCODE_STATUS_CLASS_EXPR}) = 'no_outcome'`,
    });
    // Every catalog filter has a predicate, and vice versa: a filter added to
    // the URL-facing catalog without a predicate here would page a class the
    // toggle can select but the server can never actually filter to.
    expect(Object.keys(GEOCODE_LIST_FILTER_SQL).sort()).toEqual(
      [...GEOCODE_LIST_FILTERS].sort(),
    );
  });

  it("'needs_attention' is never spelled as the ambiguous/unmatched/no_outcome OR directly", () => {
    // If it were spelled as an OR of the three narrow predicates instead of
    // `!= 'geocoded'`, adding a fifth class later would silently leave
    // "needs attention" not counting it. Asserting the literal `!=` form
    // guards that.
    expect(GEOCODE_LIST_FILTER_SQL.needs_attention).toBe(
      `(${GEOCODE_STATUS_CLASS_EXPR}) != 'geocoded'`,
    );
  });
});

describe("GEOCODING_LIST_SELECT_SQL", () => {
  it("reads legal_name and every address column off published_companies (the se_company_info join lives in the shared CTE, not here) and projects the computed class alongside the raw status", () => {
    expect(GEOCODING_LIST_SELECT_SQL).toContain("p.legal_name AS legal_name");
    expect(GEOCODING_LIST_SELECT_SQL).toContain(
      `${GEOCODE_STATUS_CLASS_EXPR} AS geocode_class`,
    );
    expect(GEOCODING_LIST_SELECT_SQL).toContain(
      "p.geocode_status AS geocode_status",
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
    expect(sql).toContain("ORDER BY p.company_id ASC");
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

  it("counts total and every class in ONE scan of `published_companies`", async () => {
    clickhouse.query.mockResolvedValueOnce([
      {
        total: "3523532",
        needs_attention: "1658184",
        geocoded: "1865348",
        ambiguous: "491817",
        unmatched: "1166366",
        no_outcome: "1",
      },
    ]);

    const counts = await loadSeCompanyGeocodingCounts();

    expect(counts).toEqual({
      total: 3523532,
      needsAttention: 1658184,
      geocoded: 1865348,
      ambiguous: 491817,
      unmatched: 1166366,
      noOutcome: 1,
    });
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    expect(clickhouse.query.mock.calls[0][0]).toBe(GEOCODING_COUNTS_SQL);
    expect(GEOCODING_COUNTS_SQL).not.toContain("GROUP BY");
  });

  it("returns zeroes (never throws) when `published` has no rows", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await loadSeCompanyGeocodingCounts()).toEqual({
      total: 0,
      needsAttention: 0,
      geocoded: 0,
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
      ambiguous: 2,
      unmatched: 4,
      noOutcome: 1,
    };
    expect(countForFilter(counts, "all")).toBe(10);
    expect(countForFilter(counts, "needs_attention")).toBe(7);
    expect(countForFilter(counts, "geocoded")).toBe(3);
    expect(countForFilter(counts, "ambiguous")).toBe(2);
    expect(countForFilter(counts, "unmatched")).toBe(4);
    expect(countForFilter(counts, "no_outcome")).toBe(1);
  });
});

describe("GEOCODED_MATCH_STATUSES drift pin (cross-language)", () => {
  // Reads Dagster's own source off disk, the same idiom
  // company-serving-sections.test.ts already uses for pinning against a
  // sibling module's source text. GEOCODED_MATCH_STATUSES is a hand-copy of
  // geocode_store.py's GEOCODED_STATUSES tuple (dagster_v3/defs/
  // sweden_company/geocode_store.py); a Python-side status added to that
  // tuple without a matching TS-side update would silently classify as
  // 'unmatched' here instead of 'geocoded'. This test is what catches that --
  // not the doc comment.
  const geocodeStorePy = readFileSync(
    new URL(
      "../../dagster_v3/src/dagster_v3/defs/sweden_company/geocode_store.py",
      import.meta.url,
    ),
    "utf8",
  );

  /**
   * Extracts the quoted string literals inside `GEOCODED_STATUSES = ( ... )`.
   * Anti-vacuous by construction: a regex that fails to match the
   * declaration, or matches it but finds no quoted values inside, THROWS --
   * it never returns `[]` and lets a subsequent `toEqual([])` pass for the
   * wrong reason (the extraction breaking, not the two sides agreeing).
   */
  function extractGeocodedStatuses(source: string): string[] {
    const declaration = source.match(/GEOCODED_STATUSES\s*=\s*\(([^)]*)\)/);
    if (!declaration) {
      throw new Error(
        "Could not find `GEOCODED_STATUSES = ( ... )` in geocode_store.py -- " +
          "the drift-pin extraction regex needs updating (the Python source " +
          "changed shape), not a silent pass.",
      );
    }
    const values = [...declaration[1].matchAll(/["']([A-Za-z0-9_]+)["']/g)].map(
      (m) => m[1],
    );
    if (values.length === 0) {
      throw new Error(
        "`GEOCODED_STATUSES = ( ... )` matched but no quoted values were " +
          "extracted from it -- the drift-pin extraction regex needs " +
          "updating, not a silent pass.",
      );
    }
    return values;
  }

  it("extracts a non-empty list from geocode_store.py before comparing anything (extraction itself is proven, not assumed)", () => {
    const extracted = extractGeocodedStatuses(geocodeStorePy);
    expect(extracted.length).toBeGreaterThan(0);
  });

  it("extraction throws on a declaration shape it cannot find (the anti-vacuous guard actually guards)", () => {
    expect(() => extractGeocodedStatuses("# no GEOCODED_STATUSES here")).toThrow(
      /Could not find/,
    );
    expect(() =>
      extractGeocodedStatuses("GEOCODED_STATUSES = (\n    # nothing quoted\n)"),
    ).toThrow(/no quoted values/);
  });

  it("matches Dagster's GEOCODED_STATUSES exactly -- same set, same size, no TS-side status Python doesn't have or vice versa", () => {
    const fromPython = extractGeocodedStatuses(geocodeStorePy);
    expect(new Set(fromPython)).toEqual(new Set(GEOCODED_MATCH_STATUSES));
    // Set equality alone would not catch a duplicate on either side (e.g. a
    // status listed twice) hiding a real count mismatch, so pin the length
    // too -- as unlikely as that dedup gap is, it's what set equality alone
    // cannot see.
    expect(fromPython.length).toBe(GEOCODED_MATCH_STATUSES.length);
  });
});
