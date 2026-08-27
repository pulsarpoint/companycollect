/**
 * The Simple Sync preview: a SQL-shape pin on the hand-ported normalization.py
 * `_company_status_ctes`/`build_pending_companies_sql` (`source_count = 1`
 * branch) semantics, and a mapping test for `loadSimpleSyncPreview` over a
 * faked ClickHouse read -- mirrors se-company-info-pipeline.server.test.ts's
 * `toContain` convention for pinning hand-ported SQL by name.
 */
import { describe, expect, it, vi } from "vitest";

const chQuery = vi.fn();

vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: (...args: unknown[]) => chQuery(...args),
}));

const {
  SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL,
  SIMPLE_SYNC_STATS_SQL,
  SIMPLE_SYNC_SAMPLE_SQL,
  SIMPLE_SYNC_SAMPLE_SIZE,
  loadSimpleSyncPreview,
} = await import("~/lib/se-people-simple-sync.server");

describe("SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL", () => {
  it("reads all three source views, unlike the production draft_id read it otherwise mirrors", () => {
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain(
      "FROM corpscout.se_company_person_bolagsverket",
    );
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain(
      "FROM corpscout.se_company_person_esef",
    );
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain(
      "FROM corpscout.se_company_person_wikidata",
    );
  });

  it("computes draft_id with source_views.py's exact hash domain and per-branch disambiguator", () => {
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain(
      "se-company-person-source-observation-v2",
    );
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain("toString(signatory_uid)");
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain("toString(candidate_uid)");
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain("toString(company_wikidata_id)");
  });

  it("compares is_unchanged the same way normalization.py's company_status CTE does", () => {
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain(
      "published.company_id != ''\n            AND published.draft_ids = drafts.draft_ids\n            AND published.correction_ids = corrections.correction_ids AS is_unchanged",
    );
  });

  it("applies only the person-level correction kinds apply_person_corrections actually applies", () => {
    for (const kind of [
      "merge_persons",
      "reassign_draft",
      "split_person",
      "approve_suggestion",
      "reject_suggestion",
      "override_field",
    ]) {
      expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain(`'${kind}'`);
    }
    // Role kinds and keep_separate/undo never gate se_company_person_clickhouse.
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).not.toContain("'set_role'");
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).not.toContain("'keep_separate'");
  });

  it("is the single-source pending definition, not every changed company", () => {
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).toContain(
      "WHERE NOT is_unchanged AND source_count = 1",
    );
  });

  it("is unscoped -- no company_ids parameter, matching the clean-copy launch's default scope", () => {
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).not.toContain("company_ids");
    expect(SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL).not.toContain("all_companies");
  });
});

describe("SIMPLE_SYNC_STATS_SQL", () => {
  it("totals companies/people and breaks both down per source", () => {
    expect(SIMPLE_SYNC_STATS_SQL).toContain("toString(count()) AS company_count");
    expect(SIMPLE_SYNC_STATS_SQL).toContain("toString(sum(observation_count)) AS person_count");
    for (const source of ["bolagsverket", "esef", "wikidata"]) {
      expect(SIMPLE_SYNC_STATS_SQL).toContain(`countIf(source = '${source}')`);
      expect(SIMPLE_SYNC_STATS_SQL).toContain(`sumIf(observation_count, source = '${source}')`);
    }
    expect(SIMPLE_SYNC_STATS_SQL).toContain("FROM pending_single_source_companies");
  });
});

describe("SIMPLE_SYNC_SAMPLE_SQL", () => {
  it("is bounded by a named LIMIT parameter -- never the full pending set", () => {
    expect(SIMPLE_SYNC_SAMPLE_SQL).toContain("LIMIT {sampleSize:UInt32}");
    expect(SIMPLE_SYNC_SAMPLE_SIZE).toBe(20);
  });

  it("joins back to the pending set so the sample only ever shows in-scope people", () => {
    expect(SIMPLE_SYNC_SAMPLE_SQL).toContain(
      "INNER JOIN pending_single_source_companies USING (company_id)",
    );
  });
});

describe("loadSimpleSyncPreview", () => {
  it("maps the stats row and sample rows, and asks for exactly SIMPLE_SYNC_SAMPLE_SIZE", async () => {
    chQuery.mockReset();
    chQuery.mockImplementation(async (sql: string) => {
      if (sql === SIMPLE_SYNC_STATS_SQL) {
        return [
          {
            company_count: "42",
            person_count: "42",
            bolagsverket_company_count: "30",
            bolagsverket_person_count: "30",
            esef_company_count: "10",
            esef_person_count: "10",
            wikidata_company_count: "2",
            wikidata_person_count: "2",
          },
        ];
      }
      if (sql === SIMPLE_SYNC_SAMPLE_SQL) {
        return [{ name: "Anna Svensson", company_id: "5560125220", source: "bolagsverket" }];
      }
      throw new Error(`unexpected query: ${sql}`);
    });

    const preview = await loadSimpleSyncPreview();

    expect(preview.companyCount).toBe(42);
    expect(preview.personCount).toBe(42);
    expect(preview.bySource).toEqual([
      { source: "bolagsverket", companyCount: 30, personCount: 30 },
      { source: "esef", companyCount: 10, personCount: 10 },
      { source: "wikidata", companyCount: 2, personCount: 2 },
    ]);
    expect(preview.sample).toEqual([
      { name: "Anna Svensson", companyId: "5560125220", source: "bolagsverket" },
    ]);
    expect(preview.sampleSize).toBe(20);

    const [, sampleCall] = chQuery.mock.calls as [unknown, unknown][];
    expect(sampleCall[1]).toEqual({ sampleSize: 20 });
  });

  it("reports zero counts and an empty sample when nothing is pending", async () => {
    chQuery.mockReset();
    chQuery.mockImplementation(async (sql: string) =>
      sql === SIMPLE_SYNC_STATS_SQL
        ? [
            {
              company_count: "0",
              person_count: "0",
              bolagsverket_company_count: "0",
              bolagsverket_person_count: "0",
              esef_company_count: "0",
              esef_person_count: "0",
              wikidata_company_count: "0",
              wikidata_person_count: "0",
            },
          ]
        : [],
    );

    const preview = await loadSimpleSyncPreview();
    expect(preview.companyCount).toBe(0);
    expect(preview.sample).toEqual([]);
  });
});
