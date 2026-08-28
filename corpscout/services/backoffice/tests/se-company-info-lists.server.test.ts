import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  buildCorrectionsListFilter,
  buildInfoListFilter,
  DATATYPE_PRESENCE_EXPR,
  CORRECTION_FILTER_OPTIONS_SQL,
  CORRECTION_SORT_COLUMNS,
  CORRECTION_STATUS_EXPR,
  CORRECTIONS_LIST_COUNT_SQL,
  CORRECTIONS_LIST_SELECT_SQL,
  correctionsOrderBySql,
  FILTER_OPTIONS_TTL_MS,
  INFO_COUNTS_SQL,
  INFO_FILTER_OPTIONS_SQL,
  INFO_LIST_SELECT_SQL,
  INFO_SORT_COLUMNS,
  infoOrderBySql,
  listSeCompanyInfoCorrectionsPage,
  listSeCompanyInfoPage,
  loadSeCompanyInfoCorrectionFilterOptions,
  loadSeCompanyInfoCounts,
  loadSeCompanyInfoFilterOptions,
  PAGE_LIMIT_OFFSET_SQL,
  PROFILE_SOURCE_PREDICATES,
  PROFILE_SOURCES_EXPR,
  resetSeCompanyInfoFilterOptionsCache,
  resolveCorrectionsSort,
  resolveInfoSort,
  SCOPED_PUBLISHED_JOIN_SQL,
  SE_COMPANIES_SERVING_TABLE,
  UNDONE_CTE_SQL,
} from "~/lib/se-company-info-lists.server";
import {
  PROFILE_DATATYPES,
  PROFILE_SOURCES,
} from "~/lib/se-company-info-filters";
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
    expect(buildInfoListFilter({ entity: "legal" })).toEqual({
      where: ["length(i.company_id) = 10"],
      params: {},
    });
    expect(buildInfoListFilter({ entity: "sole" })).toEqual({
      where: ["length(i.company_id) = 12"],
      params: {},
    });
    // The serving view's status and legal_form_code are plain ''-folded
    // Strings, so neither predicate needs the old toString/ifNull guards.
    expect(buildInfoListFilter({ status: "active" })).toEqual({
      where: ["i.status = {status:String}"],
      params: { status: "active" },
    });
    expect(buildInfoListFilter({ legalForm: "AB" })).toEqual({
      where: ["i.legal_form_code = {legalForm:String}"],
      params: { legalForm: "AB" },
    });
    // Task 17: the one description-shaped thing this list still says -- does
    // the company have a published description at all? The serving view's
    // precomputed flag (the description TEXT is not a view column at all).
    expect(buildInfoListFilter({ description: "yes" })).toEqual({
      where: ["i.has_description = 1"],
      params: {},
    });
    expect(buildInfoListFilter({ description: "no" })).toEqual({
      where: ["i.has_description = 0"],
      params: {},
    });
    // Sources: one predicate per source a profile can be built from, reusing
    // the very term the B/S/E/W letter is derived from -- so a row the column
    // marks E is exactly a row `?source=esef` returns, forever. Every catalog
    // source is covered here, so a new one cannot arrive filter-less.
    for (const source of PROFILE_SOURCES) {
      expect(buildInfoListFilter({ source: source.value })).toEqual({
        where: [PROFILE_SOURCE_PREDICATES[source.value]],
        params: {},
      });
    }
    // SCB is the register base (owner ruling): every published row has it, so
    // its predicate is the tautology the 'S' letter is, kept for uniformity.
    expect(buildInfoListFilter({ source: "scb" })).toEqual({
      where: ["1"],
      params: {},
    });
    // ...and no other source is a tautology: a filter that returned every
    // company would be worse than no filter at all.
    for (const source of ["bolagsverket", "esef", "wikidata"] as const) {
      expect(PROFILE_SOURCE_PREDICATES[source]).not.toBe("1");
    }
  });

  it("whitelists the source filter against the profile-source catalog instead of interpolating it", () => {
    // The value names a KEY of the predicate map, never SQL text: an unknown
    // source (a stale ?source=llm from the removed provenance filter, the
    // select's own sentinel, an injection attempt) adds no predicate at all.
    for (const source of ["llm", "any", "none", "", "  ", "esef') OR 1=(1"]) {
      expect(buildInfoListFilter({ source })).toEqual({ where: [], params: {} });
    }
  });

  it("treats the select's 'any' sentinel and a blank as absent on every data-driven filter", () => {
    for (const filters of [
      { status: "any" },
      { status: "  " },
      { legalForm: "any" },
      { description: "any" },
      { description: "maybe" },
      { source: "any" },
    ]) {
      expect(buildInfoListFilter(filters)).toEqual({ where: [], params: {} });
    }
  });

  it("ignores blank strings instead of filtering on them", () => {
    expect(buildInfoListFilter({ companyId: "  ", name: "" })).toEqual({
      where: [],
      params: {},
    });
  });

  it("no longer builds any description-provenance predicate", () => {
    // Task 17: the source/language/suggestion/multi/corrected filters are gone
    // from this page -- they belong to the detail page, which keeps the whole
    // description story. A stale URL naming them must simply not filter.
    const stale = {
      language: "en",
      suggestion: "yes",
      multi: true,
      corrected: true,
    } as Record<string, unknown>;
    expect(buildInfoListFilter(stale)).toEqual({ where: [], params: {} });
  });

  it("ANDs every set filter together, in a stable order", () => {
    const { where, params } = buildInfoListFilter({
      companyId: "5565200028",
      name: "Alpha",
      entity: "legal",
      status: "active",
      legalForm: "AB",
      description: "yes",
      source: "wikidata",
    });
    expect(where).toEqual([
      "i.company_id = {companyId:String}",
      "i.legal_name ILIKE {name:String}",
      "length(i.company_id) = 10",
      "i.status = {status:String}",
      "i.legal_form_code = {legalForm:String}",
      "i.has_description = 1",
      "i.source_wikidata = 1",
    ]);
    expect(params).toEqual({
      companyId: "5565200028",
      name: "%Alpha%",
      status: "active",
      legalForm: "AB",
    });
  });
});

describe("company list SQL shape (the se_companies_serving view)", () => {
  it("reads the serving view as a plain table -- NO FINAL -- and projects the precomputed description flag", () => {
    expect(SE_COMPANIES_SERVING_TABLE).toBe("corpscout.se_companies_serving");
    expect(INFO_LIST_SELECT_SQL).toContain(
      "FROM corpscout.se_companies_serving AS i",
    );
    // The whole point of the repoint: the presence/source flags this page used
    // to recompute per load as IN-subqueries over five datatype tables (~1.4s
    // list + ~1.6s counts) are precomputed in the view (migration 000335, the
    // Dagster builder build_se_companies_serving_sql owns the semantics), so
    // no query here may FINAL anything or build an IN-set again.
    for (const sql of [INFO_LIST_SELECT_SQL, INFO_COUNTS_SQL, INFO_FILTER_OPTIONS_SQL]) {
      expect(sql).not.toContain("FINAL");
      expect(sql).not.toContain("se_company_info");
      expect(sql).not.toContain("se_company_address");
      expect(sql).not.toContain("se_bolagsverket_financial_metrics");
      expect(sql).not.toContain("esef_financial_metrics");
      expect(sql).not.toContain("se_financial_reports");
      expect(sql).not.toContain("company_identifier");
      expect(sql).not.toContain("se_company_person");
      expect(sql).not.toContain("company_domains");
      expect(sql).not.toContain("company_id IN (");
    }
    expect(INFO_LIST_SELECT_SQL).toContain(
      "i.has_description AS has_description",
    );
    expect(INFO_LIST_SELECT_SQL).toContain(
      "if(length(i.company_id) = 12, 'sole', 'legal') AS entity_type",
    );
    // Task 19: the Legal form column reads as a NAME, so both labels travel
    // with the code. They are the row's own copies (migration 000306), not a
    // join -- the list must stay one scan.
    expect(INFO_LIST_SELECT_SQL).toContain(
      "i.legal_form_label_en AS legal_form_label_en",
    );
    expect(INFO_LIST_SELECT_SQL).toContain(
      "i.legal_form_label_sv AS legal_form_label_sv",
    );
    expect(INFO_LIST_SELECT_SQL).not.toContain("se_code_labels");
    // Sources: ONE derived string per row, not the arrays it is derived from.
    expect(INFO_LIST_SELECT_SQL).toContain(
      `${PROFILE_SOURCES_EXPR} AS profile_sources`,
    );
    // Task 17: no description text crosses the wire for this list at all -- so
    // no snippet, and none of the provenance columns the detail page shows.
    // The serving view doesn't even carry them: the letters come from stored
    // flags, not from reading description_sources per row.
    for (const gone of [
      "substringUTF8",
      "description_sources",
      "description_source_record_uids",
      "description_source_count",
      "description_language",
      "suggestion_id",
      "correction_ids",
      "resolved_at",
    ]) {
      expect(INFO_LIST_SELECT_SQL).not.toContain(gone);
    }
    expect(INFO_COUNTS_SQL).toContain(
      "FROM corpscout.se_companies_serving AS i",
    );
    expect(PAGE_LIMIT_OFFSET_SQL).toBe("LIMIT {limit:UInt32} OFFSET {offset:UInt32}");
  });

  it("derives the Sources letters in one pinned expression: B, then S, then E, then W", () => {
    // Pinned WHOLE, not by fragments: this string is the column, the sort key
    // and (term by term) the filter, so a silent edit to any part of it -- a
    // dropped flag, a swapped letter, a reordered concat, an IN-subquery
    // creeping back in -- must fail here.
    //
    // The letters read the view's STORED flags (which tables earn a register
    // its flag is settled in the Dagster builder, migration 000335). SCB has
    // no stored flag on purpose: it is 1 for every row by construction (owner
    // ruling: SCB is the register base), so its letter is the `if(1, ...)`
    // tautology.
    expect(PROFILE_SOURCES_EXPR).toBe(
      `concat(
  if(i.source_bolagsverket = 1, 'B', ''),
  if(1, 'S', ''),
  if(i.source_esef = 1, 'E', ''),
  if(i.source_wikidata = 1, 'W', '')
)`,
    );
    // One predicate per catalog source, in the catalog's order, and the concat
    // is built FROM them, so column and filter cannot drift apart.
    expect(Object.keys(PROFILE_SOURCE_PREDICATES)).toEqual(
      PROFILE_SOURCES.map((source) => source.value),
    );
    for (const source of PROFILE_SOURCES) {
      expect(PROFILE_SOURCES_EXPR).toContain(
        `if(${PROFILE_SOURCE_PREDICATES[source.value]}, '${source.letter}', '')`,
      );
    }
  });

  it("reads every datatype's presence from the view's own precomputed flag column", () => {
    // Keyed by the client-safe catalog, so a datatype shown as a column always
    // has SQL behind it (the Record type is the compile-time half of this).
    // Each entry is a plain column of the serving view -- the per-datatype
    // IN-sets this page used to spell (COMPANY_SETS) moved into the Dagster
    // builder (build_se_companies_serving_sql), flag semantics intact: the
    // three-arm has_financial owner ruling included.
    expect(Object.keys(DATATYPE_PRESENCE_EXPR)).toEqual(
      PROFILE_DATATYPES.map((datatype) => datatype.key),
    );
    expect(DATATYPE_PRESENCE_EXPR).toEqual({
      has_address: "i.has_address",
      has_financial: "i.has_financial",
      has_people: "i.has_people",
      has_domains: "i.has_domains",
      is_publicly_traded: "i.is_publicly_traded",
      has_government_contracts: "i.has_government_contracts",
      has_job_ads: "i.has_job_ads",
    });
  });

  it("projects one presence column per catalog datatype, straight off the flags", () => {
    for (const datatype of PROFILE_DATATYPES) {
      expect(INFO_LIST_SELECT_SQL).toContain(
        `${DATATYPE_PRESENCE_EXPR[datatype.key]} AS ${datatype.key},`,
      );
    }
  });
});

describe("listSeCompanyInfoPage", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("reads the serving view without FINAL, orders by company_id, pages with named LIMIT/OFFSET params, and runs no separate count() query", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    const page = await listSeCompanyInfoPage({ page: 1, pageSize: 50 });

    expect(page).toEqual({ rows: [] });
    // Exactly one query: the row query. The pagination total comes from
    // loadSeCompanyInfoCounts instead (see that function's doc comment) -- one
    // fewer scan of the view per page load.
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    const [rowsSql, rowsParams] = clickhouse.query.mock.calls[0];
    expect(rowsSql).toContain("FROM corpscout.se_companies_serving AS i");
    expect(rowsSql).not.toContain("FINAL");
    expect(rowsSql).toContain("ORDER BY i.company_id");
    expect(rowsSql).toContain(PAGE_LIMIT_OFFSET_SQL);
    // No filter set -> no WHERE clause at all, not a no-op one.
    expect(rowsSql).not.toContain("\nWHERE ");
    expect(rowsParams).toEqual({ limit: 50, offset: 0 });
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

  it("threads filters into the row query", async () => {
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyInfoPage({ page: 2, pageSize: 50, status: "active", description: "no" });

    const [rowsSql, rowsParams] = clickhouse.query.mock.calls[0];
    expect(rowsSql).toContain(
      "WHERE i.status = {status:String} AND i.has_description = 0",
    );
    expect(rowsParams).toEqual({ status: "active", limit: 50, offset: 50 });
  });
});

describe("loadSeCompanyInfoCounts", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("counts total / with description / without description in ONE query, with the table's own filters", async () => {
    clickhouse.query.mockResolvedValueOnce([
      { total: "145", with_description: "140", without_description: "5" },
    ]);

    const counts = await loadSeCompanyInfoCounts({ entity: "legal" });

    expect(counts).toEqual({ total: 145, withDescription: 140, withoutDescription: 5 });
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    const [sql, params] = clickhouse.query.mock.calls[0];
    expect(sql).toContain(INFO_COUNTS_SQL);
    expect(sql).toContain("WHERE length(i.company_id) = 10");
    expect(sql).toContain("countIf(i.has_description = 1)");
    expect(sql).toContain("countIf(i.has_description = 0)");
    // Task 17: the model/review totals belong to the pipeline page, not here.
    expect(sql).not.toContain("description_source_count");
    expect(sql).not.toContain("GROUP BY");
    expect(params).toEqual({});
  });

  it("returns zeroes (never throws) when the filtered table has no rows", async () => {
    clickhouse.query.mockResolvedValueOnce([]);
    expect(await loadSeCompanyInfoCounts({})).toEqual({
      total: 0,
      withDescription: 0,
      withoutDescription: 0,
    });
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
    expect(buildCorrectionsListFilter({ decidedBy: "backoffice" })).toEqual({
      where: ["c.decided_by = {decidedBy:String}"],
      params: { zeroHash: ZERO_EVIDENCE_HASH, decidedBy: "backoffice" },
    });
    // decided_by is data-driven (its options come from the ledger itself), so
    // the select's "any" sentinel is what marks it absent.
    expect(buildCorrectionsListFilter({ decidedBy: "any" })).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
  });

  it("whitelists kind against SE_INFO_CORRECTION_KINDS, ignoring an unknown value (including 'any') instead of filtering on it", () => {
    expect(buildCorrectionsListFilter({ kind: "bogus" })).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
    expect(buildCorrectionsListFilter({ kind: "any" })).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
  });

  it("whitelists status against SE_INFO_CORRECTION_STATUSES, ignoring an unknown value (including 'any') instead of filtering on it", () => {
    expect(buildCorrectionsListFilter({ status: "bogus" })).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
    expect(buildCorrectionsListFilter({ status: "any" })).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
  });
});

describe("se_company_info_correction list SQL shape", () => {
  it("starts from the undone CTE, LEFT JOINs the published row scoped to ledger companies, and guards the evidence_set_hash miss with ifNull", () => {
    for (const sql of [CORRECTIONS_LIST_SELECT_SQL, CORRECTIONS_LIST_COUNT_SQL]) {
      expect(sql).toContain(UNDONE_CTE_SQL);
      expect(sql).toContain("FROM corpscout.se_company_info_correction AS c");
      expect(sql).toContain(SCOPED_PUBLISHED_JOIN_SQL);
    }
    // The join's own subquery must be scoped to companies that actually
    // appear in the ledger -- an unscoped `LEFT JOIN se_company_info FINAL`
    // re-merges and reads the whole 3.5M-row table to decorate a handful of
    // ledger rows.
    expect(SCOPED_PUBLISHED_JOIN_SQL).toContain(
      "WHERE company_id IN (SELECT company_id FROM corpscout.se_company_info_correction)",
    );
    expect(SCOPED_PUBLISHED_JOIN_SQL).toContain("FROM corpscout.se_company_info FINAL");
    // correction_ids is Array(UUID): a LEFT JOIN miss defaults it to [], so
    // has() needs no ifNull -- only the FixedString evidence_set_hash does.
    expect(CORRECTION_STATUS_EXPR).toContain("has(p.correction_ids, c.correction_id)");
    expect(CORRECTION_STATUS_EXPR).toContain("ifNull(toString(p.evidence_set_hash), '')");
    expect(CORRECTION_STATUS_EXPR).toContain("{zeroHash:String}");
    expect(CORRECTION_STATUS_EXPR).not.toContain("ifNull(p.correction_ids");
  });

  it("evaluates multiIf branches in precedence order: undone, then applied, then stale, then the pending default", () => {
    const undoneIdx = CORRECTION_STATUS_EXPR.indexOf("'undone'");
    const appliedIdx = CORRECTION_STATUS_EXPR.indexOf("'applied'");
    const staleIdx = CORRECTION_STATUS_EXPR.indexOf("'stale'");
    const pendingIdx = CORRECTION_STATUS_EXPR.indexOf("'pending'");
    for (const idx of [undoneIdx, appliedIdx, staleIdx, pendingIdx]) {
      expect(idx).toBeGreaterThan(-1);
    }
    expect(undoneIdx).toBeLessThan(appliedIdx);
    expect(appliedIdx).toBeLessThan(staleIdx);
    expect(staleIdx).toBeLessThan(pendingIdx);
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
    // indented inside the WITH clause) and the scoped join's own subquery
    // WHERE are always present; it's the *outer*, unindented filter clause
    // that must be absent when no list filter is set.
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

describe("server-side sorting", () => {
  it("whitelists every sort key against the list row type and falls back to the default, never 500s", () => {
    expect(resolveInfoSort("legal_name", "desc")).toEqual({ sort: "legal_name", dir: "desc" });
    // Anything not in the whitelist -- an unknown column, SQL text, a blank --
    // is the default sort, not an error and never interpolated.
    for (const bogus of ["legal_name; DROP TABLE", "i.legal_name", "unknown", "", undefined]) {
      expect(resolveInfoSort(bogus, "desc")).toEqual({ sort: "company_id", dir: "desc" });
    }
    expect(resolveInfoSort("legal_name", "sideways")).toEqual({
      sort: "legal_name",
      dir: "asc",
    });
    expect(resolveCorrectionsSort("decided_by", "asc")).toEqual({
      sort: "decided_by",
      dir: "asc",
    });
    expect(resolveCorrectionsSort("bogus", "bogus")).toEqual({
      sort: "created_at",
      dir: "desc",
    });
  });

  it("emits ORDER BY <col> <dir> with a stable tiebreak, and never repeats the sorted column", () => {
    expect(infoOrderBySql("legal_name", "desc")).toBe(
      "ORDER BY i.legal_name DESC, i.company_id ASC",
    );
    // has_description is a plain flag column of the view now.
    expect(infoOrderBySql("has_description", "desc")).toBe(
      "ORDER BY i.has_description DESC, i.company_id ASC",
    );
    expect(infoOrderBySql("entity_type", "asc")).toBe(
      "ORDER BY length(i.company_id) ASC, i.company_id ASC",
    );
    // Sources sorts by the derived string itself: 'BS' < 'BSE' < 'S' < 'SW'.
    expect(infoOrderBySql("profile_sources", "desc")).toBe(
      `ORDER BY ${PROFILE_SOURCES_EXPR} DESC, i.company_id ASC`,
    );
    // Every presence column sorts by the SAME flag column it is projected
    // from -- so "which companies have no domains" is one header click over
    // 3.5M rows.
    for (const datatype of PROFILE_DATATYPES) {
      expect(infoOrderBySql(datatype.key, "asc")).toBe(
        `ORDER BY ${DATATYPE_PRESENCE_EXPR[datatype.key]} ASC, i.company_id ASC`,
      );
    }
    // The default sort IS the tiebreak column, so it appears once.
    expect(infoOrderBySql("company_id", "asc")).toBe("ORDER BY i.company_id ASC");
    expect(correctionsOrderBySql("company_id", "asc")).toBe(
      "ORDER BY c.company_id ASC, c.created_at DESC, c.correction_id DESC",
    );
    // The unchanged default: newest first, correction_id breaking ties.
    expect(correctionsOrderBySql("created_at", "desc")).toBe(
      "ORDER BY c.created_at DESC, c.correction_id DESC",
    );
    // A computed column sorts by its expression, not by the SELECT alias.
    expect(correctionsOrderBySql("status", "asc")).toBe(
      `ORDER BY (${CORRECTION_STATUS_EXPR}) ASC, c.created_at DESC, c.correction_id DESC`,
    );
  });

  it("threads the chosen sort into the paged row query", async () => {
    clickhouse.query.mockReset();
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyInfoPage({ page: 1, pageSize: 50, sort: "legal_form_code", dir: "desc" });
    expect(clickhouse.query.mock.calls[0][0]).toContain(
      "ORDER BY i.legal_form_code DESC, i.company_id ASC",
    );

    clickhouse.query.mockClear();
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyInfoCorrectionsPage({
      page: 1,
      pageSize: 50,
      sort: "decided_by",
      dir: "asc",
    });
    expect(clickhouse.query.mock.calls[0][0]).toContain(
      "ORDER BY c.decided_by ASC, c.created_at DESC, c.correction_id DESC",
    );
  });

  it("keys every sort column by a column of the row type it sorts", () => {
    // Sorting a list by a name the row does not have would give a header that
    // silently never sorts; the component's sortKey is typed against these.
    expect(Object.keys(INFO_SORT_COLUMNS)).toEqual([
      "company_id",
      "legal_name",
      "status",
      "legal_form_code",
      "entity_type",
      "has_description",
      "has_address",
      "has_financial",
      "has_people",
      "has_domains",
      "is_publicly_traded",
      "has_government_contracts",
      "has_job_ads",
      "profile_sources",
    ]);
    expect(Object.keys(CORRECTION_SORT_COLUMNS)).toEqual([
      "created_at",
      "company_id",
      "correction_id",
      "correction_kind",
      "payload",
      "reason",
      "decided_by",
      "status",
    ]);
  });
});

describe("discrete filter options", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    resetSeCompanyInfoFilterOptionsCache();
  });

  it("reads every data-driven option list of a table in ONE query, off the serving view", async () => {
    clickhouse.query.mockResolvedValueOnce([
      {
        statuses: ["active", "dissolved"],
        legal_form_codes: ["", "AB-ORGFO", "ZZZ"],
        legal_form_labels: [
          {
            code: "AB-ORGFO",
            label_sv: "Aktiebolag",
            label_en: "Limited company (aktiebolag)",
          },
          // Curated but carried by nobody -- not an option, it would filter to
          // an empty list.
          { code: "SCE-ORGFO", label_sv: "Europakooperativ", label_en: "SCE" },
        ],
      },
    ]);

    const options = await loadSeCompanyInfoFilterOptions();

    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    expect(clickhouse.query.mock.calls[0][0]).toBe(INFO_FILTER_OPTIONS_SQL);
    expect(INFO_FILTER_OPTIONS_SQL).toContain(
      "FROM corpscout.se_companies_serving AS i",
    );
    for (const column of ["i.status", "i.legal_form_code"]) {
      expect(INFO_FILTER_OPTIONS_SQL).toContain(`groupUniqArray(`);
      expect(INFO_FILTER_OPTIONS_SQL).toContain(column);
    }
    // Task 17: description_language is not a filter on this page any more, so
    // its option list is not read either.
    expect(INFO_FILTER_OPTIONS_SQL).not.toContain("description_language");
    // The labels come from the curated dictionary, in the SAME statement -- a
    // second round trip per page load would be the easy way to get this wrong,
    // and reading them off the 3.5M published rows the subtle one (a label
    // rollout leaves the same code carrying two different pairs).
    expect(INFO_FILTER_OPTIONS_SQL).toContain("FROM corpscout.se_code_labels AS l");
    expect(INFO_FILTER_OPTIONS_SQL).toContain("argMax(l.label_sv, l.version)");
    expect(INFO_FILTER_OPTIONS_SQL).toContain("argMax(l.label_en, l.version)");
    expect(INFO_FILTER_OPTIONS_SQL).toContain("l.code_type = 'legal_form'");
    // A NAMED tuple, so JSONEachRow renders each entry as an object -- the
    // shape the loader types its rows as.
    expect(INFO_FILTER_OPTIONS_SQL).toContain(
      "'Tuple(code String, label_sv String, label_en String)'",
    );
    // One option per code IN USE: '' (the "none" option) and the unlabelled
    // ZZZ are kept, and the curated-but-unused SCE-ORGFO is not an option.
    expect(options).toEqual({
      statuses: ["active", "dissolved"],
      legalForms: [
        { code: "", label_sv: "", label_en: "" },
        {
          code: "AB-ORGFO",
          label_sv: "Aktiebolag",
          label_en: "Limited company (aktiebolag)",
        },
        { code: "ZZZ", label_sv: "", label_en: "" },
      ],
    });
  });

  it("serves the cached options within the TTL and re-reads after it", async () => {
    clickhouse.query.mockResolvedValue([
      { statuses: ["active"], legal_form_codes: ["AB-ORGFO"], legal_form_labels: [] },
    ]);
    const now = Date.parse("2026-08-23T10:00:00Z");
    const clock = vi.spyOn(Date, "now").mockReturnValue(now);

    await loadSeCompanyInfoFilterOptions();
    await loadSeCompanyInfoFilterOptions();
    expect(clickhouse.query).toHaveBeenCalledTimes(1);

    clock.mockReturnValue(now + FILTER_OPTIONS_TTL_MS + 1);
    await loadSeCompanyInfoFilterOptions();
    expect(clickhouse.query).toHaveBeenCalledTimes(2);
    clock.mockRestore();
  });

  it("reads the ledger's own decided_by values, cached the same way", async () => {
    clickhouse.query.mockResolvedValue([{ decided_by: ["backoffice", "dagster"] }]);

    expect(await loadSeCompanyInfoCorrectionFilterOptions()).toEqual({
      decidedBy: ["backoffice", "dagster"],
    });
    await loadSeCompanyInfoCorrectionFilterOptions();
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    expect(clickhouse.query.mock.calls[0][0]).toBe(CORRECTION_FILTER_OPTIONS_SQL);
    expect(CORRECTION_FILTER_OPTIONS_SQL).toContain(
      "FROM corpscout.se_company_info_correction AS c",
    );
    expect(CORRECTION_FILTER_OPTIONS_SQL).toContain("groupUniqArray(c.decided_by)");
  });

  it("returns empty option lists (never throws) when the table has no rows", async () => {
    clickhouse.query.mockResolvedValue([]);
    expect(await loadSeCompanyInfoFilterOptions()).toEqual({
      statuses: [],
      legalForms: [],
    });
    resetSeCompanyInfoFilterOptionsCache();
    expect(await loadSeCompanyInfoCorrectionFilterOptions()).toEqual({ decidedBy: [] });
  });
});
