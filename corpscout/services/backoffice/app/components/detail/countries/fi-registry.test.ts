import { describe, expect, it } from "vitest";
import {
  decorateFiRecord,
  fiRegistrationFlags,
  fiTradeRegisterStatusText,
  fiVatId,
} from "~/components/detail/countries/fi-registry";

describe("fiTradeRegisterStatusText", () => {
  it("labels the known codes with the raw code kept visible", () => {
    expect(fiTradeRegisterStatusText("1")).toBe("1 — Registered");
    expect(fiTradeRegisterStatusText("0")).toBe("0 — Not in trade register");
    expect(fiTradeRegisterStatusText("4")).toBe("4 — Ceased");
  });

  it("passes unknown codes through raw without inventing a label", () => {
    expect(fiTradeRegisterStatusText("3")).toBe("3");
  });

  it("returns null for empty values", () => {
    expect(fiTradeRegisterStatusText("")).toBeNull();
    expect(fiTradeRegisterStatusText(null)).toBeNull();
    expect(fiTradeRegisterStatusText(undefined)).toBeNull();
  });
});

describe("decorateFiRecord", () => {
  it("replaces trade_register_status and leaves other fields untouched", () => {
    const record = { trade_register_status: "1", raw_status_code: "2", eu_id: "FIFPRO.x" };
    const decorated = decorateFiRecord(record);
    expect(decorated.trade_register_status).toBe("1 — Registered");
    expect(decorated.raw_status_code).toBe("2");
    expect(decorated.eu_id).toBe("FIFPRO.x");
    expect(record.trade_register_status).toBe("1"); // input not mutated
  });

  it("returns the record unchanged when there is nothing to decorate", () => {
    const record = { trade_register_status: "", name: "X" };
    expect(decorateFiRecord(record)).toBe(record);
  });

  it("fills the derived VAT id for VAT-registered companies", () => {
    const record = {
      business_id: "2858394-9",
      vat_id: null,
      is_vat_registered: 1,
      trade_register_status: "",
    };
    const decorated = decorateFiRecord(record);
    expect(decorated.vat_id).toBe("FI28583949");
    expect(record.vat_id).toBeNull(); // input not mutated
  });
});

describe("fiVatId", () => {
  it("derives FI + digits from the business id when VAT-registered", () => {
    expect(fiVatId({ business_id: "2858394-9", is_vat_registered: 1 })).toBe("FI28583949");
    expect(fiVatId({ business_id: "0104539-0", is_vat_registered: true })).toBe("FI01045390");
  });

  it("returns null when not VAT-registered", () => {
    expect(fiVatId({ business_id: "2858394-9", is_vat_registered: 0 })).toBeNull();
    expect(fiVatId({ business_id: "2858394-9" })).toBeNull();
  });

  it("prefers a source-provided vat_id over the derivation", () => {
    expect(fiVatId({ business_id: "2858394-9", vat_id: "FI99999999", is_vat_registered: 1 })).toBe(
      "FI99999999",
    );
  });

  it("refuses malformed business ids", () => {
    expect(fiVatId({ business_id: "not-an-id", is_vat_registered: 1 })).toBeNull();
    expect(fiVatId({ is_vat_registered: 1 })).toBeNull();
  });
});

describe("fiRegistrationFlags", () => {
  it("maps numeric ClickHouse UInt8 flags to active booleans", () => {
    const flags = fiRegistrationFlags({
      is_vat_registered: 1,
      is_employer_registered: 0,
      is_prepayment_registered: 1,
    });
    expect(flags).toEqual([
      { label: "VAT", active: true },
      { label: "Employer", active: false },
      { label: "Prepayment", active: true },
    ]);
  });

  it("treats missing flags as inactive", () => {
    expect(fiRegistrationFlags({}).every((f) => !f.active)).toBe(true);
  });
});
