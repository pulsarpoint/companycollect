import { describe, expect, it } from "vitest";

import { cpvCodeList, cpvDepth, cpvDivisionLabel, cpvPrefix, cpvSubjects } from "./cpv";

describe("reading a CPV code", () => {
  it("names the division", () => {
    expect(cpvDivisionLabel("72000000")).toBe("IT services");
    expect(cpvDivisionLabel("45000000")).toBe("Construction work");
    expect(cpvDivisionLabel("33196000")).toBe("Medical equipment and pharmaceuticals");
  });

  it("places a code written with its check digit", () => {
    expect(cpvDivisionLabel("33196000-6")).toBe("Medical equipment and pharmaceuticals");
  });

  it("returns nothing for a code it cannot place, rather than guessing", () => {
    expect(cpvDivisionLabel("99000000")).toBeNull();
    expect(cpvDivisionLabel("")).toBeNull();
  });

  it("measures specificity by trailing zeros", () => {
    // 40% of codes in use are division-only, so this is what separates
    // "construction, broadly" from "wheelchairs".
    expect(cpvDepth("72000000")).toBe(2);
    expect(cpvDepth("71300000")).toBe(3);
    expect(cpvDepth("33193100")).toBe(6);
  });
});

describe("grouping a notice's codes into subjects", () => {
  // The array that prompted this, verbatim from a Norwegian notice.
  const REAL = [
    "71314000", "71314200", "66140000", "66000000", "71300000",
    "09000000", "66100000", "71310000", "09300000", "09310000", "71000000",
  ];

  it("collapses eleven codes into the three subjects they describe", () => {
    // 71000000 -> 71300000 -> 71310000 -> 71314000 -> 71314200 is ONE hierarchy
    // chain, and so are the 66s and the 09s. A notice naming an ancestor and its
    // descendants is saying one thing at several depths.
    const subjects = cpvSubjects(REAL);

    expect(subjects).toHaveLength(3);
    expect(subjects.map((s) => s.division)).toEqual(["71", "09", "66"]);
    expect(subjects.map((s) => s.label)).toEqual([
      "Architectural, engineering and inspection services",
      "Energy and fuel",
      "Financial and insurance services",
    ]);
  });

  it("keeps the deepest code per division, since that is what the buyer said", () => {
    const subjects = cpvSubjects(REAL);

    expect(subjects[0].code).toBe("71314200");
    // The rest stay available rather than being discarded.
    expect(subjects[0].codes).toContain("71000000");
    expect(subjects[0].codes).toHaveLength(5);
  });

  it("leads with the most specific subject", () => {
    // A notice naming wheelchairs and "medical, broadly" is about wheelchairs.
    const subjects = cpvSubjects(["33000000", "45213100"]);

    expect(subjects[0].code).toBe("45213100");
  });

  it("accepts a single code as well as an array", () => {
    expect(cpvSubjects("72000000")).toHaveLength(1);
    expect(cpvSubjects("72000000")[0].label).toBe("IT services");
  });

  it("is empty for a notice with no classification", () => {
    expect(cpvSubjects([])).toEqual([]);
    expect(cpvSubjects(null)).toEqual([]);
    expect(cpvSubjects("")).toEqual([]);
  });

  it("still places an unknown division rather than dropping the code", () => {
    const subjects = cpvSubjects(["99123456"]);

    expect(subjects).toHaveLength(1);
    expect(subjects[0].label).toBe("CPV division 99");
    expect(subjects[0].code).toBe("99123456");
  });
});

describe("a real nine-code notice", () => {
  it("collapses two IT/business chains into two subjects", () => {
    // Doffin 2024-113936, verbatim. 72000000 -> 72200000 -> 72220000 -> 72224000
    // -> 72224100 and 79000000 -> 79400000 -> 79420000 -> 79421000 are two chains,
    // so nine codes describe two things: IT services and business consulting.
    const subjects = cpvSubjects([
      "72000000", "72224000", "79400000", "79000000", "79420000",
      "79421000", "72220000", "72224100", "72200000",
    ]);

    expect(subjects).toHaveLength(2);
    expect(subjects.map((s) => `${s.label} (${s.code})`)).toEqual([
      "IT services (72224100)",
      "Business services: law, marketing, consulting and security (79421000)",
    ]);
  });
});

describe("cpvCodeList", () => {
  it("splits Hilma's comma-joined string into separate codes", () => {
    // 1,782 Finnish rows publish several codes in ONE string. Read as a single
    // code its digits concatenate into nonsense.
    expect(cpvCodeList("72317000, 48800000, 72000000")).toEqual([
      "72317000",
      "48800000",
      "72000000",
    ]);
  });

  it("leaves an array as it is", () => {
    expect(cpvCodeList(["45000000", "77210000"])).toEqual(["45000000", "77210000"]);
  });

  it("treats a single code as one code", () => {
    expect(cpvCodeList("45213100")).toEqual(["45213100"]);
  });

  it("returns nothing for absent values", () => {
    expect(cpvCodeList(null)).toEqual([]);
    expect(cpvCodeList("")).toEqual([]);
    expect(cpvCodeList([])).toEqual([]);
  });
});

describe("cpvSubjects with joined strings", () => {
  it("finds all three subjects in one Hilma string", () => {
    // Previously this produced ONE subject whose code was the whole string.
    const subjects = cpvSubjects("72317000, 48800000, 72000000");
    expect(subjects.map((s) => s.division).sort()).toEqual(["48", "72"]);
  });
});

describe("cpvPrefix", () => {
  it("reduces a code to the prefix its descendants share", () => {
    expect(cpvPrefix("45000000")).toBe("45");
    expect(cpvPrefix("45210000")).toBe("4521");
    expect(cpvPrefix("45213100")).toBe("452131");
  });

  it("never goes below the division, even when the division ends in zero", () => {
    // 30000000 strips to '3', which is not a division. Held at '30'.
    expect(cpvPrefix("30000000")).toBe("30");
    expect(cpvPrefix("90000000")).toBe("90");
  });

  it("passes a bare division through", () => {
    expect(cpvPrefix("45")).toBe("45");
  });

  it("rejects anything that is not a usable code", () => {
    expect(cpvPrefix("")).toBeNull();
    expect(cpvPrefix("4")).toBeNull();
    expect(cpvPrefix(null)).toBeNull();
    expect(cpvPrefix("abc")).toBeNull();
  });

  it("is a prefix of every descendant, which is what makes selection work", () => {
    const parent = cpvPrefix("45210000")!;
    for (const child of ["45210000", "45213100", "45213150"]) {
      expect(child.startsWith(parent)).toBe(true);
    }
    expect("45000000".startsWith(parent)).toBe(false);
  });
});
