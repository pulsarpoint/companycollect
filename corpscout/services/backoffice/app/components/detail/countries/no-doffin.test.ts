import { describe, expect, it } from "vitest";

import {
  DOFFIN_SECTIONS,
  doffinAwardResult,
  doffinDirective,
  doffinFieldValue,
  doffinIsRedundant,
  doffinValueInconsistency,
} from "./no-doffin";

describe("Doffin coded fields", () => {
  it("says what an award result means", () => {
    expect(doffinAwardResult("winner_selected")).toBe("A winner was named");
    expect(doffinAwardResult("no_winner_named")).toBe("No winner was named");
  });

  it("leaves an unmapped result as stored rather than inventing one", () => {
    expect(doffinAwardResult("cancelled")).toBe("cancelled");
    expect(doffinAwardResult("")).toBeNull();
  });

  it("reads the published directive flag", () => {
    // cbc:RegulatoryDomain is PUBLISHED, not inferred -- 'yes' means the notice
    // named an EU directive, 'no' means nationally regulated.
    expect(doffinDirective("yes")).toBe("Governed by an EU procurement directive");
    expect(doffinDirective("no")).toBe("Nationally regulated");
    expect(doffinDirective("")).toBeNull();
  });
});

describe("doffinValueInconsistency", () => {
  it("flags a winner award larger than the whole notice", () => {
    // Doffin notice 2025-120612: Bærum kommune's electricity award publishes
    // BT-720 = 200,000,000,000 NOK against BT-161 = 2,000,000,000 NOK. Verified
    // against the source XML -- the register really does say this.
    const note = doffinValueInconsistency({
      value_amount_original: "200000000000",
      notice_value_amount_original: "2000000000",
    });
    expect(note).toContain("100×");
    expect(note).toContain("as published");
  });

  it("says nothing when the winner's share is part of the notice total", () => {
    // The overwhelmingly normal shape: equal, or a clean fraction.
    expect(
      doffinValueInconsistency({
        value_amount_original: "500000",
        notice_value_amount_original: "500000",
      }),
    ).toBeNull();
    expect(
      doffinValueInconsistency({
        value_amount_original: "250000",
        notice_value_amount_original: "500000",
      }),
    ).toBeNull();
  });

  it("says nothing when either figure is absent", () => {
    expect(
      doffinValueInconsistency({ value_amount_original: "100", notice_value_amount_original: null }),
    ).toBeNull();
    expect(doffinValueInconsistency({})).toBeNull();
  });

  it("does not flag on a rounding-sized excess", () => {
    expect(
      doffinValueInconsistency({
        value_amount_original: "500000.01",
        notice_value_amount_original: "500000",
      }),
    ).toBeNull();
  });
});

describe("doffinFieldValue", () => {
  it("renders an array as a list, not as JSON", () => {
    expect(doffinFieldValue("location_ids", ["NO081", "NO082"])).toBe("NO081, NO082");
  });

  it("decodes the coded fields through their own mappers", () => {
    expect(doffinFieldValue("award_result", "winner_selected")).toBe("A winner was named");
    expect(doffinFieldValue("directive_governed", "yes")).toBe(
      "Governed by an EU procurement directive",
    );
  });

  it("drops empties so the section never renders a blank row", () => {
    expect(doffinFieldValue("lot_heading", "")).toBeNull();
    expect(doffinFieldValue("location_ids", [])).toBeNull();
    expect(doffinFieldValue("received_tenders", null)).toBeNull();
  });

  it("shows a zero tender count rather than hiding it", () => {
    // 0 received tenders is a fact about the procurement, not a missing value.
    expect(doffinFieldValue("received_tenders", 0)).toBe("0");
  });
});

describe("doffinIsRedundant", () => {
  it("hides the raw winner id when it matches the normalised one", () => {
    // A Norwegian winner's id normalises to itself, so both rows print the same
    // nine digits -- noise dressed up as two facts.
    expect(
      doffinIsRedundant("winner_org_number_raw", {
        winner_org_number: "979139268",
        winner_org_number_raw: "979139268",
      }),
    ).toBe(true);
  });

  it("keeps the raw id when it differs, which is the whole reason it is stored", () => {
    // A Swedish winner's 556516-1352 never normalises to nine digits, so the
    // raw value is the only record of what the register actually published.
    expect(
      doffinIsRedundant("winner_org_number_raw", {
        winner_org_number: "",
        winner_org_number_raw: "556516-1352",
      }),
    ).toBe(false);
  });

  it("leaves every other field alone", () => {
    expect(doffinIsRedundant("buyer_name", { buyer_name: "Bærum kommune" })).toBe(false);
  });
});

describe("DOFFIN_SECTIONS", () => {
  it("never lists a money or CPV column, which have their own sections", () => {
    const keys = DOFFIN_SECTIONS.flatMap((s) => s.fields.map((f) => f.key));
    expect(keys.filter((k) => /_amount_|_currency$|^cpv/.test(k))).toEqual([]);
  });

  it("names every key only once across all sections", () => {
    const keys = DOFFIN_SECTIONS.flatMap((s) => s.fields.map((f) => f.key));
    expect(new Set(keys).size).toBe(keys.length);
  });
});
