import { describe, expect, it } from "vitest";

import {
  maskPersonalSupplierId,
  supplierPosition,
  supplierStatusLabel,
} from "./supplier-label";

describe("supplier status labels", () => {
  it("says what each unmatched status actually means", () => {
    // Four distinct situations. Collapsing them into one word like "External"
    // would call a Brazilian individual external and dress our own matching
    // failure up as a property of the supplier.
    expect(supplierStatusLabel("foreign_winner")).toBe("Foreign");
    expect(supplierStatusLabel("natural_person")).toBe("Individual");
    expect(supplierStatusLabel("unmatched_company")).toBe("Unmatched");
    expect(supplierStatusLabel("invalid_supplier_id")).toBe("Unverified id");
    expect(supplierStatusLabel("missing_supplier_id")).toBe("Unverified id");
    expect(supplierStatusLabel("unknown_person_type")).toBe("Unverified id");
    expect(supplierStatusLabel("invalid_identifier")).toBe("Unverified id");
  });

  it("shows no badge for a supplier that resolved to a company", () => {
    // 'exact' is the normal case and needs no decoration.
    expect(supplierStatusLabel("exact")).toBeNull();
    expect(supplierStatusLabel("")).toBeNull();
  });

  it("shows an unrecognised status raw rather than inventing a meaning", () => {
    expect(supplierStatusLabel("some_new_status")).toBe("some_new_status");
  });
});

describe("masking personal supplier ids", () => {
  it("masks a Brazilian CPF the way RFB itself publishes one", () => {
    // PNCP publishes 2,733 unmasked CPFs; RFB masks its own as ***XXXXXX**.
    // Three leading and two trailing digits hidden, matching that convention so
    // the two sources read alike.
    expect(maskPersonalSupplierId("02015169180", "br")).toBe("***151691**");
    expect(maskPersonalSupplierId("95091211187", "br")).toBe("***912111**");
  });

  it("masks by SHAPE, not by match status", () => {
    // 2,696 CPFs sit under natural_person but 37 more are filed as
    // invalid_supplier_id -- PNCP misclassified the person. A status-driven rule
    // would publish those 37 in full.
    expect(maskPersonalSupplierId("12345678901", "br")).toBe("***456789**");
  });

  it("leaves a company CNPJ alone", () => {
    // 14 digits is a company, not a person. 16 natural_person rows and all 179
    // unknown_person_type rows carry one.
    expect(maskPersonalSupplierId("55696882000163", "br")).toBe("55696882000163");
  });

  it("does not touch other countries' ids", () => {
    // Scoped to BR because 11 digits is what makes an id a CPF. Norway's org
    // numbers are 9 digits, Estonia's 8, Sweden's 10 -- none are personal, and a
    // blanket length rule would mask them wrongly.
    expect(maskPersonalSupplierId("12345678901", "no")).toBe("12345678901");
    expect(maskPersonalSupplierId("5560125220", "se")).toBe("5560125220");
  });

  it("passes through anything that is not an id", () => {
    expect(maskPersonalSupplierId("", "br")).toBe("");
    expect(maskPersonalSupplierId("not-an-id", "br")).toBe("not-an-id");
  });
});

describe("multi-supplier position", () => {
  it("names which of how many the shown supplier is", () => {
    // Replaces the old "+2", which said how many were hidden rather than where
    // the visible one sits.
    expect(supplierPosition(3)).toBe("1/3");
    expect(supplierPosition(2)).toBe("1/2");
  });

  it("says nothing when there is only one supplier", () => {
    expect(supplierPosition(1)).toBeNull();
    expect(supplierPosition(0)).toBeNull();
  });
});
