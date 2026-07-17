import { describe, expect, it } from "vitest";
import {
  decorateFiRecord,
  fiRegistrationFlags,
  fiTradeRegisterStatusText,
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

  it("returns the record unchanged when the status is empty", () => {
    const record = { trade_register_status: "", name: "X" };
    expect(decorateFiRecord(record)).toBe(record);
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
