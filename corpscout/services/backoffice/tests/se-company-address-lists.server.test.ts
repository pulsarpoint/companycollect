import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  ADDRESS_CORRECTION_COUNTS_SQL,
  ADDRESS_CORRECTION_FILTER_OPTIONS_SQL,
  ADDRESS_CORRECTION_LIST_SQL,
  ADDRESS_CORRECTION_SORT_COLUMNS,
  ADDRESS_CORRECTION_STATUS_EXPR,
  ADDRESS_SCOPED_PUBLISHED_JOIN_SQL,
  ADDRESS_UNDONE_CTE_SQL,
  buildAddressCorrectionsListFilter,
  correctionsOrderBySql,
  listSeCompanyAddressCorrectionsPage,
  loadSeCompanyAddressCorrectionFilterOptions,
  resetSeCompanyAddressCorrectionFilterOptionsCache,
  resolveCorrectionsSort,
} from "~/lib/se-company-address-lists.server";
import {
  SE_ADDRESS_CORRECTION_STATUSES,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-address-corrections";
import { PAGE_LIMIT_OFFSET_SQL } from "~/lib/se-company-info-lists.server";

describe("buildAddressCorrectionsListFilter", () => {
  it("always carries the zero hash (the status expression needs it even unfiltered)", () => {
    expect(buildAddressCorrectionsListFilter({})).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
  });

  it("adds a predicate only for the filter that is set", () => {
    expect(buildAddressCorrectionsListFilter({ companyId: "5565200028" })).toEqual({
      where: ["c.company_id = {companyId:String}"],
      params: { zeroHash: ZERO_EVIDENCE_HASH, companyId: "5565200028" },
    });
    expect(buildAddressCorrectionsListFilter({ kind: "reject_address" })).toEqual({
      where: ["c.correction_kind = {kind:String}"],
      params: { zeroHash: ZERO_EVIDENCE_HASH, kind: "reject_address" },
    });
    const statusFilter = buildAddressCorrectionsListFilter({ status: "applied" });
    expect(statusFilter.where).toEqual([
      `(${ADDRESS_CORRECTION_STATUS_EXPR}) = {status:String}`,
    ]);
    expect(statusFilter.params).toEqual({
      zeroHash: ZERO_EVIDENCE_HASH,
      status: "applied",
    });
    expect(buildAddressCorrectionsListFilter({ decidedBy: "backoffice" })).toEqual({
      where: ["c.decided_by = {decidedBy:String}"],
      params: { zeroHash: ZERO_EVIDENCE_HASH, decidedBy: "backoffice" },
    });
    // decided_by is data-driven (its options come from the ledger itself), so
    // the select's "any" sentinel is what marks it absent.
    expect(buildAddressCorrectionsListFilter({ decidedBy: "any" })).toEqual({
      where: [],
      params: { zeroHash: ZERO_EVIDENCE_HASH },
    });
  });

  /**
   * The whitelists are the ADDRESS ledger's own, not the info ledger's: an
   * approve_suggestion has no meaning here, and letting it through would filter
   * the list to nothing while the chip claimed a filter was applied.
   */
  it("whitelists kind and status against the address ledger's own enums", () => {
    for (const kind of ["approve_suggestion", "bogus", "any"]) {
      expect(buildAddressCorrectionsListFilter({ kind })).toEqual({
        where: [],
        params: { zeroHash: ZERO_EVIDENCE_HASH },
      });
    }
    for (const kind of ["override_field", "reject_address", "undo"]) {
      expect(buildAddressCorrectionsListFilter({ kind }).where).toEqual([
        "c.correction_kind = {kind:String}",
      ]);
    }
    for (const status of SE_ADDRESS_CORRECTION_STATUSES) {
      expect(buildAddressCorrectionsListFilter({ status }).params.status).toBe(status);
    }
    for (const status of ["bogus", "any"]) {
      expect(buildAddressCorrectionsListFilter({ status })).toEqual({
        where: [],
        params: { zeroHash: ZERO_EVIDENCE_HASH },
      });
    }
  });
});

describe("se_company_address_correction list SQL shape", () => {
  it("reads the ledger, joins the published row each correction names, and names the decided address key", () => {
    for (const sql of [ADDRESS_CORRECTION_LIST_SQL, ADDRESS_CORRECTION_COUNTS_SQL]) {
      expect(sql).toContain(ADDRESS_UNDONE_CTE_SQL);
      expect(sql).toContain("FROM corpscout.se_company_address_correction AS c");
      expect(sql).toContain(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL);
    }
    // The subject of every non-undo correction, lifted out of the payload: a
    // company has several addresses, so the ledger row alone does not say which.
    expect(ADDRESS_CORRECTION_LIST_SQL).toContain(
      "JSONExtractString(c.payload, 'address_key') AS address_key",
    );
    // FINAL, and scoped to the companies actually present in the ledger: an
    // unscoped FINAL re-merges the whole address table to decorate a handful of
    // ledger rows.
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).toContain(
      "FROM corpscout.se_company_address FINAL",
    );
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).toContain(
      "WHERE company_id IN (SELECT company_id FROM corpscout.se_company_address_correction)",
    );
    // No aggregation left: the final is a ReplacingMergeTree ordered by
    // (company_id, address_key), so FINAL already leaves one row per key.
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).not.toContain("GROUP BY");
  });

  /**
   * Review T7-m4, at the list level. The published side is joined per
   * (company_id, address_key) -- THE row each correction names -- because
   * apply_address_ledger looks the payload's address_key up in the produced set
   * and compares against that row. Joining an aggregate per COMPANY, as this
   * list used to, answers a different question.
   */
  it("joins the row the correction names, on company AND address key", () => {
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).toContain(
      `) AS p ON p.company_id = c.company_id AND p.address_key = JSONExtractString(c.payload, 'address_key')`,
    );
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).toContain(
      "toString(address_key) AS address_key",
    );
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).toContain(
      "arrayMap(x -> toString(x), correction_ids) AS applied_correction_ids",
    );
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).toContain(
      "toString(evidence_set_hash) AS evidence_set_hash",
    );
    // The produced set: live rows plus reject-tombstoned ones (the ledger runs
    // before with_set_replacement, so a rejected key keeps its ids and its hash
    // validity), and NOT disappearance tombstones, whose ids were cleared.
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).toContain(
      "is_current OR notEmpty(correction_ids) AS is_produced",
    );
    // The company-wide sets this replaced must be gone entirely, or the two
    // derivations can coexist and the wrong one win.
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).not.toContain("live_address_keys");
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).not.toContain("live_evidence_hashes");
    expect(ADDRESS_SCOPED_PUBLISHED_JOIN_SQL).not.toContain("groupUniqArray");
  });

  it("computes the four statuses, with the reject-with-no-row branch inside applied", () => {
    expect(ADDRESS_CORRECTION_STATUS_EXPR).toContain(
      "c.correction_id IN (SELECT id FROM undone)",
    );
    // Ruling A11: Dagster has no row to stamp a reject that names a key this
    // company does not publish, so the absence of the key from the produced set
    // IS the applied signal. Pinned verbatim -- se-company-address.server.ts
    // spells the same branch for one company.
    expect(ADDRESS_CORRECTION_STATUS_EXPR).toContain(
      `has(p.applied_correction_ids, toString(c.correction_id))
      OR (
        c.correction_kind = 'reject_address'
        AND JSONExtractString(c.payload, 'address_key') != ''
        AND NOT p.is_produced
      ), 'applied',`,
    );
    expect(ADDRESS_CORRECTION_STATUS_EXPR).toContain("{zeroHash:String}");
  });

  /**
   * The mutation check for review T7-m4 at the list level. Pinned verbatim: a
   * regression to `NOT has(p.live_evidence_hashes, ...)` reads "pending" for a
   * correction naming key A while carrying key B's hash (Dagster reads stale),
   * and "stale" for a correction against a reject-tombstoned row (Dagster
   * applies it). The three guards keep the branch the same question
   * apply_address_ledger asks: no address_key means skipped, a kind
   * effective_ledger drops is never considered, and the zero hash is undo's own
   * marker.
   */
  it("compares the evidence hash against the joined row, never against the company", () => {
    expect(ADDRESS_CORRECTION_STATUS_EXPR).toContain(
      `JSONExtractString(c.payload, 'address_key') != ''
      AND c.correction_kind IN ('reject_address', 'override_field')
      AND toString(c.evidence_hash) != {zeroHash:String}
      AND (
        NOT p.is_produced
        OR p.evidence_set_hash != toString(c.evidence_hash)
      ), 'stale',`,
    );
    expect(ADDRESS_CORRECTION_STATUS_EXPR).not.toContain("live_evidence_hashes");
    expect(ADDRESS_CORRECTION_STATUS_EXPR).not.toContain("live_address_keys");
  });

  it("evaluates multiIf branches in precedence order: undone, applied, stale, pending", () => {
    // multiIf returns the FIRST branch that matches, so the branch order is the
    // precedence: an undone correction is history whatever it once did, and an
    // applied one is not waiting on evidence.
    const indexes = ["undone", "applied", "stale", "pending"].map((status) => {
      const index = ADDRESS_CORRECTION_STATUS_EXPR.indexOf(`'${status}'`);
      expect(index, status).toBeGreaterThan(-1);
      return index;
    });
    for (let i = 1; i < indexes.length; i += 1) {
      expect(indexes[i - 1]).toBeLessThan(indexes[i]);
    }
  });
});

describe("listSeCompanyAddressCorrectionsPage", () => {
  beforeEach(() => clickhouse.query.mockReset());

  it("orders newest first and pages with named LIMIT/OFFSET params", async () => {
    clickhouse.query.mockResolvedValueOnce([]).mockResolvedValueOnce([{ total: "7" }]);
    const page = await listSeCompanyAddressCorrectionsPage({ page: 1, pageSize: 50 });

    expect(page.total).toBe(7);
    const [rowsSql, rowsParams] = clickhouse.query.mock.calls[0];
    expect(rowsSql).toContain("ORDER BY c.created_at DESC, c.correction_id DESC");
    expect(rowsSql).toContain(PAGE_LIMIT_OFFSET_SQL);
    // The CTE's own WHERE and the scoped join's own subquery WHERE are always
    // present (both indented); it is the OUTER filter clause that must be
    // absent when nothing is filtered.
    expect(rowsSql).not.toContain("\nWHERE ");
    expect(rowsParams).toEqual({ zeroHash: ZERO_EVIDENCE_HASH, limit: 50, offset: 0 });

    const [countSql, countParams] = clickhouse.query.mock.calls[1];
    expect(countSql).not.toContain("\nWHERE ");
    expect(countParams).toEqual({ zeroHash: ZERO_EVIDENCE_HASH });
  });

  it("clamps pageSize and computes the offset from the page", async () => {
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyAddressCorrectionsPage({ page: 3, pageSize: 5000 });
    expect(clickhouse.query.mock.calls[0][1]).toMatchObject({ limit: 200, offset: 400 });
  });

  it("threads every filter into the row query and the count identically", async () => {
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyAddressCorrectionsPage({
      page: 1,
      pageSize: 50,
      companyId: "5565200028",
      kind: "undo",
    });

    const [rowsSql, rowsParams] = clickhouse.query.mock.calls[0];
    const [countSql, countParams] = clickhouse.query.mock.calls[1];
    for (const sql of [rowsSql, countSql]) {
      expect(sql).toContain(
        "WHERE c.company_id = {companyId:String} AND c.correction_kind = {kind:String}",
      );
    }
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
  it("whitelists every sort key and falls back to the default, never 500s", () => {
    expect(resolveCorrectionsSort(undefined, undefined)).toEqual({
      sort: "created_at",
      dir: "desc",
    });
    expect(resolveCorrectionsSort("bogus", "sideways")).toEqual({
      sort: "created_at",
      dir: "desc",
    });
    for (const key of Object.keys(ADDRESS_CORRECTION_SORT_COLUMNS)) {
      expect(resolveCorrectionsSort(key, "asc")).toEqual({ sort: key, dir: "asc" });
    }
  });

  it("sorts the two computed columns by their expression, never by the SELECT alias", () => {
    // ClickHouse does not guarantee a SELECT-list alias is visible to ORDER BY
    // at the same query level, so both computed columns repeat their expression.
    expect(ADDRESS_CORRECTION_SORT_COLUMNS.status).toBe(
      `(${ADDRESS_CORRECTION_STATUS_EXPR})`,
    );
    expect(ADDRESS_CORRECTION_SORT_COLUMNS.address_key).toBe(
      "JSONExtractString(c.payload, 'address_key')",
    );
  });

  it("keeps (created_at DESC, correction_id DESC) as the tiebreak and never repeats the sorted column", () => {
    expect(correctionsOrderBySql("created_at", "desc")).toBe(
      "ORDER BY c.created_at DESC, c.correction_id DESC",
    );
    expect(correctionsOrderBySql("company_id", "asc")).toBe(
      "ORDER BY c.company_id ASC, c.created_at DESC, c.correction_id DESC",
    );
    expect(correctionsOrderBySql("correction_id", "asc")).toBe(
      "ORDER BY c.correction_id ASC, c.created_at DESC",
    );
  });

  it("threads the chosen sort into the paged row query", async () => {
    clickhouse.query.mockReset();
    clickhouse.query.mockResolvedValue([]);
    await listSeCompanyAddressCorrectionsPage({
      page: 1,
      pageSize: 50,
      sort: "decided_by",
      dir: "asc",
    });
    expect(clickhouse.query.mock.calls[0][0]).toContain(
      "ORDER BY c.decided_by ASC, c.created_at DESC, c.correction_id DESC",
    );
  });
});

describe("decided_by filter options", () => {
  beforeEach(() => {
    clickhouse.query.mockReset();
    resetSeCompanyAddressCorrectionFilterOptionsCache();
  });

  it("reads the ledger's own decided_by values in one query, and caches them", async () => {
    clickhouse.query.mockResolvedValue([{ decided_by: ["backoffice", "dagster"] }]);
    expect(await loadSeCompanyAddressCorrectionFilterOptions()).toEqual({
      decidedBy: ["backoffice", "dagster"],
    });
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
    expect(clickhouse.query.mock.calls[0][0]).toBe(ADDRESS_CORRECTION_FILTER_OPTIONS_SQL);
    expect(ADDRESS_CORRECTION_FILTER_OPTIONS_SQL).toContain(
      "FROM corpscout.se_company_address_correction AS c",
    );
    await loadSeCompanyAddressCorrectionFilterOptions();
    expect(clickhouse.query).toHaveBeenCalledTimes(1);
  });

  it("returns an empty option list (never throws) on an empty ledger", async () => {
    clickhouse.query.mockResolvedValue([]);
    expect(await loadSeCompanyAddressCorrectionFilterOptions()).toEqual({ decidedBy: [] });
  });
});
