import { describe, expect, it } from "vitest";
import {
  parseSeRatsitRequestSelection,
  seRatsitRequestListPath,
  seRatsitRequestPath,
} from "~/lib/se-ratsit-results";
import {
  SE_COMPANIES_TABS,
  seCompaniesTabFromPath,
  seCompaniesTabPath,
} from "~/lib/se-companies-tabs";

const selection = {
  companyId: "193407093016",
  batchId: "5e53617b-9263-5529-8313-70a41661beac",
};

describe("the Ratsit companies tab", () => {
  it("is linkable and resolves from a cold-load path", () => {
    expect(SE_COMPANIES_TABS).toContainEqual({ value: "ratsit", label: "Ratsit" });
    expect(seCompaniesTabPath("ratsit")).toBe("/admin/se/companies/ratsit");
    expect(seCompaniesTabFromPath("/admin/se/companies/ratsit")).toBe("ratsit");
  });

  it("parses only a complete company and UUID selection", () => {
    expect(
      parseSeRatsitRequestSelection(
        new URL(
          `http://backoffice/admin/se/companies/ratsit?companyId=${selection.companyId}&batchId=${selection.batchId}`,
        ),
      ),
    ).toEqual(selection);
    expect(
      parseSeRatsitRequestSelection(
        new URL(
          "http://backoffice/admin/se/companies/ratsit?companyId=5560125220&batchId=bad",
        ),
      ),
    ).toBeNull();
  });

  it("preserves paging while opening and closing a request", () => {
    const detail = seRatsitRequestPath(selection, "?page=3&pageSize=100");
    expect(detail).toContain("page=3");
    expect(detail).toContain(`companyId=${selection.companyId}`);
    expect(seRatsitRequestListPath(new URL(detail, "http://backoffice").search)).toBe(
      "/admin/se/companies/ratsit?page=3&pageSize=100",
    );
  });
});
