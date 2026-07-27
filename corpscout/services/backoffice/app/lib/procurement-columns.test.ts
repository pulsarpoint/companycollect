import { describe, expect, it } from "vitest";
import {
  columnLabel,
  isHiddenTableColumn,
  visibleColumns,
} from "./procurement-columns";

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

describe("columnLabel", () => {
  it("reads a snake_case column as a sentence", () => {
    expect(columnLabel("publication_number")).toBe("Publication number");
    expect(columnLabel("buyer_name")).toBe("Buyer name");
    expect(columnLabel("notice_type")).toBe("Notice type");
    expect(columnLabel("award_result")).toBe("Award result");
  });

  it("keeps acronyms as acronyms rather than sentence-casing them", () => {
    expect(columnLabel("country_iso2")).toBe("Country ISO-2");
    expect(columnLabel("buyer_national_id")).toBe("Buyer national ID");
    expect(columnLabel("source_url")).toBe("Source URL");
    expect(columnLabel("cpv_codes")).toBe("CPV codes");
    expect(columnLabel("doffin_id")).toBe("Doffin ID");
  });

  it("moves a currency or rawness suffix into brackets", () => {
    expect(columnLabel("total_value_amount_original")).toBe("Total value (original)");
    expect(columnLabel("total_value_amount_usd")).toBe("Total value (USD)");
    expect(columnLabel("value_amount_usd")).toBe("Value (USD)");
    expect(columnLabel("buyer_national_id_raw")).toBe("Buyer national ID (raw)");
    expect(columnLabel("winner_org_number_raw")).toBe("Winner org number (raw)");
  });

  it("does not bracket a currency that is not an amount suffix", () => {
    // The pairing is _amount_usd. Applying the bracket to any trailing _usd
    // would turn this into the nonsense "FX rate to (USD)".
    expect(columnLabel("fx_rate_to_usd")).toBe("FX rate to USD");
    expect(columnLabel("valor_global_usd")).toBe("Valor global USD");
  });

  it("tidies non-English column names without translating them", () => {
    // PNCP publishes Portuguese field names. Inventing English ones would make
    // the column unmatchable against the register it came from.
    expect(columnLabel("numero_controle_pncp")).toBe("Numero controle PNCP");
    expect(columnLabel("data_publicacao_pncp")).toBe("Data publicacao PNCP");
  });

  it("leaves a name it cannot split alone rather than emptying it", () => {
    expect(columnLabel("")).toBe("");
    expect(columnLabel("_")).toBe("_");
  });

  it("labels every column the registers actually publish", () => {
    // The guard that matters: these pages discover columns from the database,
    // so a label must never come back empty or still underscored.
    for (const name of [
      "country_iso2",
      "publication_number",
      "publication_date",
      "place_country",
      "buyer_org_ref",
      "buyer_national_id_raw",
      "buyer_country",
      "notice_title",
      "total_value_currency",
      "company_match_status",
      "source_record_id",
      "lots_value_amount_original",
      "subcontracting_amount_usd",
    ]) {
      const label = columnLabel(name);
      expect(label).not.toBe("");
      expect(label).not.toContain("_");
      expect(label[0]).toBe(label[0].toUpperCase());
    }
  });
});
