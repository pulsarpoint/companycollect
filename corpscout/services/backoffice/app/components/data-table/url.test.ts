import { describe, expect, it } from "vitest";
import {
  clearAllFilters,
  nextSortDir,
  removeFilterValue,
  tableSearch,
  toggleFilterValue,
} from "~/components/data-table/url";

describe("tableSearch", () => {
  it("preserves existing params and patches page", () => {
    const current = new URLSearchParams("q=grupp&sort=status&dir=desc&page=3");
    expect(tableSearch(current, { page: 4 })).toBe("?q=grupp&sort=status&dir=desc&page=4");
  });

  it("resets page when sort changes", () => {
    const current = new URLSearchParams("q=grupp&page=3");
    const s = tableSearch(current, { sort: "status", dir: "asc" });
    expect(s).toContain("sort=status");
    expect(s).toContain("dir=asc");
    expect(s).not.toContain("page=");
    expect(s).toContain("q=grupp");
  });

  it("resets page when pageSize changes", () => {
    const current = new URLSearchParams("page=9");
    expect(tableSearch(current, { pageSize: 100 })).toBe("?pageSize=100");
  });
});

describe("nextSortDir", () => {
  it("starts asc on a new column", () => {
    expect(nextSortDir("name", "asc", "status")).toBe("asc");
  });
  it("toggles on the same column", () => {
    expect(nextSortDir("status", "asc", "status")).toBe("desc");
    expect(nextSortDir("status", "desc", "status")).toBe("asc");
  });
});

describe("toggleFilterValue", () => {
  it("adds a value and resets page", () => {
    const current = new URLSearchParams("q=grupp&page=3");
    const s = toggleFilterValue(current, "status", "Registered");
    expect(s).toContain("f_status=Registered");
    expect(s).toContain("q=grupp");
    expect(s).not.toContain("page=");
  });

  it("removes an already-selected value, keeps siblings", () => {
    const current = new URLSearchParams("f_status=A&f_status=B");
    const s = toggleFilterValue(current, "status", "A");
    expect(s).toContain("f_status=B");
    expect(s).not.toContain("f_status=A");
  });
});

describe("removeFilterValue", () => {
  it("removes the value, keeps siblings, resets page", () => {
    const current = new URLSearchParams("f_status=A&f_status=B&page=2");
    const s = removeFilterValue(current, "status", "A");
    expect(s).toContain("f_status=B");
    expect(s).not.toContain("f_status=A");
    expect(s).not.toContain("page=");
  });

  it("is idempotent: never re-adds an absent value", () => {
    const current = new URLSearchParams("f_status=B");
    expect(removeFilterValue(current, "status", "A")).toBe("?f_status=B");
  });
});

describe("clearAllFilters", () => {
  it("removes every f_* param and page, keeps the rest", () => {
    const current = new URLSearchParams(
      "q=x&sort=status&f_status=A&f_legal_form=B&page=2",
    );
    const s = clearAllFilters(current);
    expect(s).toBe("?q=x&sort=status");
  });
});
