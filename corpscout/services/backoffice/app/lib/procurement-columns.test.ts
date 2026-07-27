import { describe, expect, it } from "vitest";
import { isHiddenTableColumn, visibleColumns } from "./procurement-columns";

describe("isHiddenTableColumn", () => {
  it("hides load plumbing on every register", () => {
    for (const name of ["source_slug", "source_run_id", "partition_key", "resolved_at"]) {
      expect(isHiddenTableColumn(name)).toBe(true);
    }
  });

  it("hides FX bookkeeping", () => {
    for (const name of ["fx_rate_to_usd", "fx_rate_date", "fx_source"]) {
      expect(isHiddenTableColumn(name)).toBe(true);
    }
  });

  it("hides estimated and framework value groups (TED, Doffin, Hilma)", () => {
    for (const name of [
      "estimated_value_amount_original",
      "estimated_value_amount_usd",
      "estimated_value_currency",
      "notice_estimated_value_amount_usd",
      "lot_estimated_value_amount_original",
      "framework_maximum_amount_original",
      "framework_maximum_amount_usd",
      "framework_maximum_currency",
      "framework_total_maximum_amount_usd",
      "framework_total_approximate_amount_original",
      "framework_value_reestimated_amount_usd",
    ]) {
      expect(isHiddenTableColumn(name)).toBe(true);
    }
  });

  it("keeps realized values, identity and buyer/winner columns", () => {
    for (const name of [
      "value_amount_original",
      "value_amount_usd",
      "value_currency",
      "awarded_amount_usd",
      "total_value_amount_usd",
      "procurement_value_amount_original",
      "valor_global",
      "valor_global_usd",
      "buyer_name",
      "buyer_national_id",
      "winner_name",
      "winner_org_number",
      "publication_date",
      "notice_type",
      "award_result",
      "source_record_id",
      "source_url",
      "company_id",
    ]) {
      expect(isHiddenTableColumn(name)).toBe(false);
    }
  });
});

describe("visibleColumns", () => {
  it("filters while preserving order", () => {
    expect(
      visibleColumns(["doffin_id", "source_slug", "buyer_name", "fx_source", "value_amount_usd"]),
    ).toEqual(["doffin_id", "buyer_name", "value_amount_usd"]);
  });
});
