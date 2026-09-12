import { describe, expect, it } from "vitest";
import {
  compressYears,
  groupPersonRoles,
  personRoleYearsToRows,
  type SePersonRoleGroupRow,
} from "~/lib/se-person-roles";

describe("compressYears", () => {
  it("renders a single year as itself", () => {
    expect(compressYears([2025])).toBe("2025");
  });

  it("compresses a consecutive run into one en-dash range", () => {
    expect(compressYears([2021, 2022, 2023])).toBe("2021–2023");
  });

  it("keeps a gap as two comma-separated pieces", () => {
    expect(compressYears([2021, 2022, 2023, 2026])).toBe("2021–2023, 2026");
  });

  it("renders nothing for an empty list", () => {
    expect(compressYears([])).toBe("");
  });

  it("de-duplicates and sorts before compressing", () => {
    expect(compressYears([2022, 2021, 2022])).toBe("2021–2022");
  });
});

describe("groupPersonRoles", () => {
  const row = (partial: Partial<SePersonRoleGroupRow>): SePersonRoleGroupRow => ({
    role_code: "board_member",
    role_year: 2025,
    role_from: "",
    role_to: "",
    source: "bolagsverket",
    is_current: 0,
    ...partial,
  });

  it("groups two roles with interleaved years, one group per role_code", () => {
    const groups = groupPersonRoles([
      row({ role_code: "board_member", role_year: 2021 }),
      row({ role_code: "legal_representative", role_year: 2026 }),
      row({ role_code: "board_member", role_year: 2022 }),
    ]);
    expect(groups).toHaveLength(2);
    const boardMember = groups.find((group) => group.role_code === "board_member");
    expect(boardMember?.years).toEqual([2021, 2022]);
    expect(boardMember?.yearRanges).toBe("2021–2022");
  });

  it("merges and sorts sources across a role's rows, distinct", () => {
    const groups = groupPersonRoles([
      row({ role_year: 2021, source: "esef" }),
      row({ role_year: 2022, source: "bolagsverket" }),
      row({ role_year: 2022, source: "esef" }),
    ]);
    expect(groups[0].sources).toEqual(["bolagsverket", "esef"]);
  });

  it("takes from as the earliest non-empty role_from and to as the latest non-empty role_to", () => {
    const groups = groupPersonRoles([
      row({ role_year: 0, role_from: "2021-05-01", role_to: "2022-03-31" }),
      row({ role_year: 0, role_from: "2019-01-01", role_to: "2020-12-31" }),
      row({ role_year: 0, role_from: "", role_to: "" }),
    ]);
    expect(groups[0].from).toBe("2019-01-01");
    expect(groups[0].to).toBe("2022-03-31");
  });

  it("propagates current when any row in the group is current", () => {
    const groups = groupPersonRoles([
      row({ role_year: 2021, is_current: 0 }),
      row({ role_year: 2022, is_current: 1 }),
    ]);
    expect(groups[0].current).toBe(true);
    const noneCurrent = groupPersonRoles([row({ is_current: 0 })]);
    expect(noneCurrent[0].current).toBe(false);
  });

  it("excludes year 0 from years and sets noFiscalYear when any row carried it", () => {
    const groups = groupPersonRoles([
      row({ role_year: 0 }),
      row({ role_year: 2025 }),
    ]);
    expect(groups[0].years).toEqual([2025]);
    expect(groups[0].noFiscalYear).toBe(true);
    expect(groups[0].yearRanges).toBe("2025");

    const onlyZero = groupPersonRoles([row({ role_year: 0 })]);
    expect(onlyZero[0].years).toEqual([]);
    expect(onlyZero[0].noFiscalYear).toBe(true);
    expect(onlyZero[0].yearRanges).toBe("");
  });

  it("orders groups by latest year desc, then role_code asc", () => {
    const groups = groupPersonRoles([
      row({ role_code: "auditor", role_year: 2020 }),
      row({ role_code: "board_member", role_year: 2025 }),
      row({ role_code: "legal_representative", role_year: 2026 }),
      row({ role_code: "executive", role_year: 2025 }),
    ]);
    expect(groups.map((group) => group.role_code)).toEqual([
      "legal_representative",
      "board_member",
      "executive",
      "auditor",
    ]);
  });

  it("returns nothing for no rows", () => {
    expect(groupPersonRoles([])).toEqual([]);
  });
});

describe("personRoleYearsToRows", () => {
  it("expands one row per source and reads is_current off the person's current_roles", () => {
    const rows = personRoleYearsToRows(
      [
        { code: "board_member", year: 2025, sources: ["bolagsverket", "esef"] },
        { code: "auditor", year: 2022, sources: ["wikidata"] },
      ],
      ["board_member"],
    );
    expect(rows).toEqual([
      {
        role_code: "board_member", role_year: 2025, role_from: "", role_to: "",
        source: "bolagsverket", is_current: 1,
      },
      {
        role_code: "board_member", role_year: 2025, role_from: "", role_to: "",
        source: "esef", is_current: 1,
      },
      {
        role_code: "auditor", role_year: 2022, role_from: "", role_to: "",
        source: "wikidata", is_current: 0,
      },
    ]);
  });

  it("feeds groupPersonRoles cleanly for a role with two years and one source", () => {
    const rows = personRoleYearsToRows(
      [
        { code: "board_member", year: 2021, sources: ["esef"] },
        { code: "board_member", year: 2022, sources: ["esef"] },
      ],
      [],
    );
    const groups = groupPersonRoles(rows);
    expect(groups).toHaveLength(1);
    expect(groups[0].yearRanges).toBe("2021–2022");
    expect(groups[0].sources).toEqual(["esef"]);
    expect(groups[0].current).toBe(false);
  });
});
