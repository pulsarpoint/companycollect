import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  buildCorrectionsListFilter,
  buildInfoListFilter,
  CORRECTION_STATUS_EXPR,
  CORRECTIONS_LIST_COUNT_SQL,
  CORRECTIONS_LIST_SELECT_SQL,
  INFO_COUNTS_BY_SOURCE_SQL,
  INFO_COUNTS_TOTALS_SQL,
  INFO_LIST_COUNT_SQL,
  INFO_LIST_SELECT_SQL,
  listSeCompanyInfoCorrectionsPage,
  listSeCompanyInfoPage,
  loadSeCompanyInfoCounts,
  PAGE_LIMIT_OFFSET_SQL,
  UNDONE_CTE_SQL,
} from "~/lib/se-company-info-lists.server";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

describe("buildInfoListFilter", () => {
  it("adds no predicates when every filter is absent", () => {
    const { where, params } = buildInfoListFilter({});
    expect(where).toEqual([]);
    expect(params).toEqual({});
  });

  it("adds a predicate only for the filter that is set, one at a time", () => {
    expect(buildInfoListFilter({ companyId: "5565200028" })).toEqual({
      where: ["i.company_id = {companyId:String}"],
      params: { companyId: "5565200028" },
    });
    expect(buildInfoListFilter({ name: "Alpha" })).toEqual({
      where: ["i.legal_name ILIKE {name:String}"],
      params: { name: "%Alpha%" },
    });
    expect(buildInfoListFilter({ source: "llm" })).toEqual({
      where: ["toString(i.description_source) = {source:String}"],
      params: { source: "llm" },
    });
    expect(buildInfoListFilter({ multi: true })).toEqual({
      where: ["i.description_source_count > 1"],
      params: {},
    });
    expect(buildInfoListFilter({ entity: "legal" })).toEqual({
      where: ["length(i.company_id) = 10"],
      params: {},
    });
    expect(buildInfoListFilter({ entity: "sole" })).toEqual({
      where: ["length(i.company_id) = 12"],
      params: {},
    });
    expect(buildInfoListFilter({ corrected: true })).toEqual({
      where: ["notEmpty(i.correction_ids)"],
      params: {},
    });
  });

  it("maps source=none to the empty-string description_source", () => {
    expect(buildInfoListFilter({ source: "none" }).params).toEqual({ source: "" });
  });

  it("ignores blank strings and unknown source values instead of filtering on them", () => {
    expect(buildInfoListFilter({ companyId: "  ", name: "" })).toEqual({
      where: [],
      params: {},
    });
    expect(buildInfoListFilter({ source: "bogus" })).toEqual({ where: [], params: {} });
  });

  it("ANDs every set filter together, in a stable order", () => {
    const { where, params } = buildInfoListFilter({
      companyId: "5565200028",
      name: "Alpha",
      source: "scb",
      multi: true,
      entity: "legal",
      corrected: true,
    });
    expect(where).toEqual([
      "i.company_id = {companyId:String}",
      "i.legal_name ILIKE {name:String}",
      "toString(i.description_source) = {source:String}",
      "i.description_source_count > 1",
      "length(i.company_id) = 10",
      "notEmpty(i.correction_ids)",
    ]);
    expect(params).toEqual({
      companyId: "5565200028",
      name: "%Alpha%",
      source: "scb",
    });
  });
});

describe("se_company_info list SQL shape", () => {
  it("reads FINAL and every filter predicate appears only when its param is set", () => {
    expect(INFO_LIST_SELECT_SQL).toContain("FROM corpscout.se_company_info AS i FINAL");
    expect(INFO_LIST_COUNT_SQL).toContain("FROM corpscout.se_company_info AS i FINAL");
    expect(PAGE_LIMIT_OFFSET_SQL).toBe("LIMIT {limit:UInt32} OFFSET {offset:UInt32}");
  });
});

describe("listSeCompanyInfoPage", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("reads FINAL, orders by company_id, and pages with named LIMIT/OFFSET params", async () => {
    clickhouse.query.mockResolvedValueOnce([]).mockResolvedValueOnce([{ total: "3500000" }]);
    const page = await listSeCompanyInfoPage({ page: 1, pageSize: 50 });

    expect(page.total).toBe(3500000);
    expect(clickhouse.query).toHaveBeenCalledTimes(2);
    const [rowsSql, rowsParams] = clickhouse.query.mock.calls[0];
    expect(rowsSql).toContain("FROM corpscout.se_company_info AS i FINAL");
    expect(rowsSql).toContain("ORDER BY i.company_id");
    expect(rowsSql).toContain(PAGE_LIMIT_OFFSET_SQL);
    expect(rowsSql).not.toContain("WHERE");
    expect(rowsParams).toEqual({ limit: 50, offset: 0 });

    const [countSql, countParams] = clickhouse.query.mock.calls[1];
    expect(countSql).toContain("FROM corpscout.se_company_info AS i FINAL");
    expect(countSql).not.toContain("WHERE");
    expect(countParams).toEqual({});
  });

  it("clamps pageSize to [10, 200] and computes offset from page", async () => {
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyInfoPage({ page: 3, pageSize: 5 });
    expect(clickhouse.query.mock.calls[0][1]).toMatchObject({ limit: 10, offset: 20 });

    clickhouse.query.mockClear();
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyInfoPage({ page: 2, pageSize: 500 });
    expect(clickhouse.query.mock.calls[0][1]).toMatchObject({ limit: 200, offset: 200 });
  });

  it("threads filters into both the row query and the count query identically", async () => {
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyInfoPage({ page: 2, pageSize: 50, source: "llm", multi: true });

    const [rowsSql, rowsParams] = clickhouse.query.mock.calls[0];
    const [countSql, countParams] = clickhouse.query.mock.calls[1];
    expect(rowsSql).toContain("WHERE toString(i.description_source) = {source:String} AND i.description_source_count > 1");
    expect(countSql).toContain("WHERE toString(i.description_source) = {source:String} AND i.description_source_count > 1");
    expect(rowsParams).toEqual({ source: "llm", limit: 50, offset: 50 });
    expect(countParams).toEqual({ source: "llm" });
  });
});

describe("loadSeCompanyInfoCounts", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("groups by description_source and computes the multi-source and pending-model totals with the same filters as the table", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        { description_source: "scb", count: "100" },
        { description_source: "llm", count: "40" },
      ])
      .mockResolvedValueOnce([{ multi_source_count: "40", pending_model_count: "12" }]);

    const counts = await loadSeCompanyInfoCounts({ entity: "legal" });

    expect(counts).toEqual({
      bySource: [
        { source: "scb", count: 100 },
        { source: "llm", count: 40 },
      ],
      multiSourceCount: 40,
      pendingModelCount: 12,
    });

    const [bySourceSql, bySourceParams] = clickhouse.query.mock.calls[0];
    expect(bySourceSql).toContain(INFO_COUNTS_BY_SOURCE_SQL);
    expect(bySourceSql).toContain("WHERE length(i.company_id) = 10");
    expect(bySourceSql).toContain("GROUP BY i.description_source");
    expect(bySourceParams).toEqual({});

    const [totalsSql, totalsParams] = clickhouse.query.mock.calls[1];
    expect(totalsSql).toContain(INFO_COUNTS_TOTALS_SQL);
    expect(totalsSql).toContain("WHERE length(i.company_id) = 10");
    expect(totalsSql).toContain("description_source_count > 1");
    expect(totalsSql).toContain("suggestion_id IS NULL");
    expect(totalsSql).toContain("empty(i.correction_ids)");
    expect(totalsParams).toEqual({});
  });
});

describe("buildCorrectionsListFilter", () => {
  it("always carries the zero hash (the status expression needs it even unfiltered)", () => {
    expect(buildCorrectionsListFilter({})).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
  });

  it("adds a predicate only for the filter that is set", () => {
    expect(buildCorrectionsListFilter({ companyId: "5565200028" })).toEqual({
      where: ["c.company_id = {companyId:String}"],
      params: { zeroHash: ZERO_EVIDENCE_HASH, companyId: "5565200028" },
    });
    expect(buildCorrectionsListFilter({ kind: "override_field" })).toEqual({
      where: ["c.correction_kind = {kind:String}"],
      params: { zeroHash: ZERO_EVIDENCE_HASH, kind: "override_field" },
    });
    const statusFilter = buildCorrectionsListFilter({ status: "applied" });
    expect(statusFilter.where).toEqual([`(${CORRECTION_STATUS_EXPR}) = {status:String}`]);
    expect(statusFilter.params).toEqual({ zeroHash: ZERO_EVIDENCE_HASH, status: "applied" });
  });

  it("ignores an unknown status instead of filtering on it", () => {
    expect(buildCorrectionsListFilter({ status: "bogus" })).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
  });
});

describe("se_company_info_correction list SQL shape", () => {
  it("starts from the undone CTE, LEFT JOINs the published row FINAL, and guards the evidence_set_hash miss with ifNull", () => {
    for (const sql of [CORRECTIONS_LIST_SELECT_SQL, CORRECTIONS_LIST_COUNT_SQL]) {
      expect(sql).toContain(UNDONE_CTE_SQL);
      expect(sql).toContain("FROM corpscout.se_company_info_correction AS c");
      expect(sql).toContain("LEFT JOIN corpscout.se_company_info AS p FINAL ON p.company_id = c.company_id");
    }
    // correction_ids is Array(UUID): a LEFT JOIN miss defaults it to [], so
    // has() needs no ifNull -- only the FixedString evidence_set_hash does.
    expect(CORRECTION_STATUS_EXPR).toContain("has(p.correction_ids, c.correction_id)");
    expect(CORRECTION_STATUS_EXPR).toContain("ifNull(toString(p.evidence_set_hash), '')");
    expect(CORRECTION_STATUS_EXPR).toContain("{zeroHash:String}");
    expect(CORRECTION_STATUS_EXPR).not.toContain("ifNull(p.correction_ids");
  });
});

describe("listSeCompanyInfoCorrectionsPage", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("orders newest first and pages with named LIMIT/OFFSET params", async () => {
    clickhouse.query.mockResolvedValueOnce([]).mockResolvedValueOnce([{ total: "12" }]);
    const page = await listSeCompanyInfoCorrectionsPage({ page: 1, pageSize: 50 });

    expect(page.total).toBe(12);
    const [rowsSql, rowsParams] = clickhouse.query.mock.calls[0];
    expect(rowsSql).toContain("ORDER BY c.created_at DESC, c.correction_id DESC");
    expect(rowsSql).toContain(PAGE_LIMIT_OFFSET_SQL);
    // The CTE's own WHERE (scoping it to rows that supersede something,
    // indented inside the WITH clause) is always present; it's the *outer*,
    // unindented filter clause that must be absent when no list filter is set.
    expect(rowsSql).not.toContain("\nWHERE ");
    expect(rowsParams).toEqual({ zeroHash: ZERO_EVIDENCE_HASH, limit: 50, offset: 0 });

    const [countSql, countParams] = clickhouse.query.mock.calls[1];
    expect(countSql).not.toContain("\nWHERE ");
    expect(countParams).toEqual({ zeroHash: ZERO_EVIDENCE_HASH });
  });

  it("threads companyId/kind/status filters into both queries identically", async () => {
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyInfoCorrectionsPage({
      page: 1,
      pageSize: 50,
      companyId: "5565200028",
      kind: "undo",
    });

    const [rowsSql, rowsParams] = clickhouse.query.mock.calls[0];
    const [countSql, countParams] = clickhouse.query.mock.calls[1];
    expect(rowsSql).toContain(
      "WHERE c.company_id = {companyId:String} AND c.correction_kind = {kind:String}",
    );
    expect(countSql).toContain(
      "WHERE c.company_id = {companyId:String} AND c.correction_kind = {kind:String}",
    );
    expect(rowsParams).toEqual({
      zeroHash: ZERO_EVIDENCE_HASH,
      companyId: "5565200028",
      kind: "undo",
      limit: 50,
      offset: 0,
    });
    expect(countParams).toEqual({
      zeroHash: ZERO_EVIDENCE_HASH,
      companyId: "5565200028",
      kind: "undo",
    });
  });
});
