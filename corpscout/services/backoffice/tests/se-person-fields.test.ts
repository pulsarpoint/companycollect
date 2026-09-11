import { describe, expect, it } from "vitest";
import {
  foldsIntoPerson,
  groupOfRowSlot,
  isPersonKey,
  isPersonSource,
  isPersonStatus,
  MAIN_PERSON_SOURCES,
  normalizeSePersonName,
  personFoldPending,
  personGroupSlot,
  personRowSlot,
  personSourceLabel,
  PERSON_SOURCES,
  PERSON_STATUSES,
  roleLabel,
  selectedPersonFromSearch,
  validateSePersonInput,
} from "~/lib/se-person-fields";

const KEY = "a".repeat(64);
const ROLE_CODES = ["board_member", "board_chair", "chief_executive_officer"];
const ROLE_OPTIONS = [
  { code: "board_member", label: "Board member", group: "governance" },
  { code: "board_chair", label: "Board chair", group: "governance" },
];
const base = {
  firstName: "Anna",
  lastName: "Svensson",
  birthYear: "1975",
  wikidataId: "",
  roles: [{ code: "board_member", fromYear: "2023", toYear: "2025" }],
  data: "",
  note: "",
};

describe("person catalogue", () => {
  it("names the entity's six sources, three statuses and its slots", () => {
    expect([...PERSON_SOURCES]).toEqual([
      "bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft",
    ]);
    expect([...PERSON_STATUSES]).toEqual(["active", "hidden", "withdrawn"]);
    expect(isPersonSource("wikidata") && !isPersonSource("scb")).toBe(true);
    expect(isPersonStatus("withdrawn") && !isPersonStatus("gone")).toBe(true);
    expect(personSourceLabel("esef")).toBe("ESEF");
    expect(personSourceLabel("reviewer_draft")).toBe("Reviewer draft");
    // Minor 5: `reviewer_draft` never folds into a published row, so it always returns
    // zero rows from the list's Source filter -- offer the other five. Ratsit joined
    // them on 2026-09-11 with the person extractor.
    expect([...MAIN_PERSON_SOURCES]).toEqual([
      "bolagsverket", "esef", "wikidata", "ratsit", "reviewer",
    ]);
    expect(roleLabel("board_chair", ROLE_OPTIONS)).toBe("Board chair");
    // An unmapped code (a source's own label, published as itself) reads as itself.
    expect(roleLabel("styrelseledarmot", ROLE_OPTIONS)).toBe("styrelseledarmot");
    expect(isPersonKey(KEY) && !isPersonKey("nope")).toBe(true);
    expect(selectedPersonFromSearch(new URLSearchParams(`person=${KEY}`))).toBe(KEY);
    expect(selectedPersonFromSearch(new URLSearchParams("person=nope"))).toBeNull();
    // Ruling 2: the group slot is the stamp's 17 digits, a row slot adds an ordinal.
    expect(personGroupSlot("2026-09-10 12:00:00.123")).toBe("r20260910120000123");
    expect(personRowSlot("r20260910120000123", 1)).toBe("r2026091012000012301");
    expect(groupOfRowSlot("r2026091012000012310")).toBe("r20260910120000123");
  });
});

describe("normalizeSePersonName (the port of normalize_se.py, spec 4.1 to 4.4)", () => {
  it("splits, folds and keeps the delivered spelling", () => {
    expect(normalizeSePersonName({ firstName: "Anna", lastName: "Svensson" })).toMatchObject({
      status: "ok",
      firstTokens: ["anna"],
      middleTokens: [],
      lastTokens: ["svensson"],
      displayName: "Anna Svensson",
    });
    expect(normalizeSePersonName({ firstName: "Anna Maria", lastName: "Svensson" })).toMatchObject({
      firstTokens: ["anna"], middleTokens: ["maria"], lastTokens: ["svensson"],
    });
    // Diacritics fold, hyphens split, initials lose their periods.
    expect(normalizeSePersonName({ firstName: "Hakan", lastName: "Oberg" })).toMatchObject({
      firstTokens: ["hakan"], lastTokens: ["oberg"],
    });
    expect(normalizeSePersonName({ firstName: "Sven-Erik", lastName: "Ek" })).toMatchObject({
      firstTokens: ["sven"], middleTokens: ["erik"], lastTokens: ["ek"],
    });
    // A one-string name: the last word is the last name, particles glue to it.
    expect(normalizeSePersonName({ fullName: "Carl von Essen" })).toMatchObject({
      firstTokens: ["carl"], lastTokens: ["von", "essen"], displayLast: "von Essen",
    });
    expect(normalizeSePersonName({ fullName: "Svensson, Anna" })).toMatchObject({
      firstTokens: ["anna"], lastTokens: ["svensson"], displayName: "Anna Svensson",
    });
    // Titles are dropped from the display spelling, whole words only.
    expect(normalizeSePersonName({ firstName: "Dr Anna", lastName: "Svensson" })).toMatchObject({
      status: "ok", displayFirst: "Anna",
    });
    expect(normalizeSePersonName({ firstName: "Ing-Marie", lastName: "Ek" }).displayFirst).toBe("Ing-Marie");
  });

  it("scores partial and no_person exactly as the normalizer does", () => {
    expect(normalizeSePersonName({ lastName: "Svensson" })).toMatchObject({
      status: "partial", reason: "only one name word",
    });
    expect(normalizeSePersonName({ firstName: "A", lastName: "Svensson" })).toMatchObject({
      status: "partial", reason: "initials only",
    });
    expect(normalizeSePersonName({})).toMatchObject({ status: "no_person", reason: "empty name" });
    expect(normalizeSePersonName({ fullName: "Styrelseledamot" })).toMatchObject({
      status: "no_person", reason: "role word in the name field",
    });
    expect(normalizeSePersonName({ fullName: "Anna Svensson AB" })).toMatchObject({
      status: "no_person", reason: "company suffix in the name field",
    });
    expect(normalizeSePersonName({ fullName: "Anna 1985" })).toMatchObject({
      status: "no_person", reason: "digits in the name field",
    });
  });
});

describe("foldsIntoPerson (spec 5.1's guarded relation, Ruling 1)", () => {
  const anna = {
    firstTokens: ["anna"], middleTokens: [], lastTokens: ["svensson"], birthYear: "", wikidataId: "",
  };
  it("matches equal names, a lone middle name and a shared QID", () => {
    expect(foldsIntoPerson(anna, { ...anna })).toBe(true);
    expect(foldsIntoPerson(anna, { ...anna, middleTokens: ["maria"] })).toBe(true);
    expect(foldsIntoPerson(anna, { ...anna, lastTokens: ["svenson"] })).toBe(false);
    expect(foldsIntoPerson(anna, { ...anna, firstTokens: ["annika"] })).toBe(false);
    expect(
      foldsIntoPerson(
        { ...anna, wikidataId: "Q42" },
        { firstTokens: ["carl"], middleTokens: [], lastTokens: ["essen"], birthYear: "", wikidataId: "Q42" },
      ),
    ).toBe(true);
  });
  it("never matches two different birth years, whatever the name says", () => {
    expect(foldsIntoPerson({ ...anna, birthYear: "1975" }, { ...anna, birthYear: "1975" })).toBe(true);
    expect(foldsIntoPerson({ ...anna, birthYear: "1975" }, { ...anna, birthYear: "1980" })).toBe(false);
    expect(
      foldsIntoPerson(
        { ...anna, birthYear: "1975", wikidataId: "Q42" },
        { ...anna, birthYear: "1980", wikidataId: "Q42" },
      ),
    ).toBe(false);
  });
});

describe("validateSePersonInput", () => {
  it("accepts a first and last name with a role span and normalizes the data object", () => {
    expect(validateSePersonInput({ ...base, data: ' {"title":"Chair"} ' }, ROLE_CODES, 2026)).toEqual({
      ok: true,
      input: {
        firstName: "Anna",
        lastName: "Svensson",
        birthYear: "1975",
        wikidataId: "",
        roles: [{ code: "board_member", fromYear: "2023", toYear: "2025" }],
        data: '{"title":"Chair"}',
        note: "",
      },
    });
    // No roles at all is still a person: the fold publishes an empty roles block.
    expect(validateSePersonInput({ ...base, roles: [] }, ROLE_CODES, 2026).ok).toBe(true);
    // A blank role row is dropped, not refused: the sheet always renders one.
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "", fromYear: "", toYear: "" }] }, ROLE_CODES, 2026),
    ).toMatchObject({ ok: true, input: { roles: [] } });
  });

  it("refuses a name the normalizer would not fold, and says why", () => {
    expect(validateSePersonInput({ ...base, firstName: "A" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "initials only",
    });
    expect(validateSePersonInput({ ...base, lastName: "" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "only one name word",
    });
    expect(validateSePersonInput({ ...base, lastName: "AB" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "company suffix in the name field",
    });
  });

  it("refuses a bad birth year, a bad QID, an unknown role, a reversed span and a far year", () => {
    expect(validateSePersonInput({ ...base, birthYear: "1750" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Birth year must be between 1850 and 2026.",
    });
    expect(validateSePersonInput({ ...base, wikidataId: "42" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Wikidata id must look like Q42.",
    });
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "vd", fromYear: "", toYear: "" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "Unknown role: vd." });
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "2025", toYear: "2019" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "A role cannot end before it starts." });
    // An open span (from-only): the 1970 floor applies, not the single-year 1900 one.
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "2040", toYear: "" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "From year must be between 1970 and 2031." });
  });

  it("refuses a row with years but no code, naming the row", () => {
    // Important 2: the sheet always renders a trailing blank row, and typing a year
    // into it without picking a role must not fall through to "Unknown role: .".
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "", fromYear: "2024", toYear: "2024" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "Pick a role for row 1." });
    // The row number is the position among ALL rows the sheet rendered, blank ones
    // included, so it names the row the reviewer is looking at.
    expect(
      validateSePersonInput(
        {
          ...base,
          roles: [
            { code: "board_member", fromYear: "2023", toYear: "2025" },
            { code: "", fromYear: "", toYear: "2026" },
          ],
        },
        ROLE_CODES,
        2026,
      ),
    ).toEqual({ ok: false, error: "Pick a role for row 2." });
  });

  it("keeps 1900 for a single fiscal year but floors a span, an open span and an end-only year at 1970", () => {
    // Important 3: ClickHouse `Date` floors at 1970-01-01 (verified on prod:
    // toDate('1901-05-04') -> '1970-01-01'), so anything that writes into
    // `role_from`/`role_to` -- everything but a single fiscal year -- must refuse below
    // it, or an open span silently inflates into decades of fabricated role-years.
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "1900", toYear: "1900" }] }, ROLE_CODES, 2026).ok,
    ).toBe(true);
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "1899", toYear: "1899" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "From year must be between 1900 and 2031." });
    // A span.
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "1970", toYear: "1975" }] }, ROLE_CODES, 2026).ok,
    ).toBe(true);
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "1950", toYear: "1955" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "From year must be between 1970 and 2031." });
    // An open span (from-only, no end).
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "1970", toYear: "" }] }, ROLE_CODES, 2026).ok,
    ).toBe(true);
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "1950", toYear: "" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "From year must be between 1970 and 2031." });
    // An end-only year (to-only, no start).
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "", toYear: "1970" }] }, ROLE_CODES, 2026).ok,
    ).toBe(true);
    expect(
      validateSePersonInput({ ...base, roles: [{ code: "board_member", fromYear: "", toYear: "1950" }] }, ROLE_CODES, 2026),
    ).toEqual({ ok: false, error: "To year must be between 1970 and 2031." });
  });

  it("refuses data that is not an object, a reserved key, and a note with a control character", () => {
    expect(validateSePersonInput({ ...base, data: "[1,2]" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Data must be a JSON object.",
    });
    expect(validateSePersonInput({ ...base, data: "not json" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Data must be a JSON object.",
    });
    // Ruling 4: the three keys the backoffice sets itself are refused here.
    expect(validateSePersonInput({ ...base, data: '{"note":"mine"}' }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Data may not carry the reserved key note.",
    });
    expect(validateSePersonInput({ ...base, data: '{"replaces_key":"x"}' }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Data may not carry the reserved key replaces_key.",
    });
    expect(validateSePersonInput({ ...base, note: "line\nbreak" }, ROLE_CODES, 2026).ok).toBe(true);
    expect(validateSePersonInput({ ...base, note: "bell\u0007" }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Note must be plain text.",
    });
    expect(validateSePersonInput({ ...base, note: "x".repeat(501) }, ROLE_CODES, 2026)).toEqual({
      ok: false, error: "Note is longer than 500 characters.",
    });
  });
});

describe("personFoldPending", () => {
  it("is pending when a stamp is newer than the fold, or when nothing was folded but rows fold", () => {
    expect(personFoldPending("2026-09-10 10:00:00.000", ["2026-09-10 09:00:00.000"], true)).toBe(false);
    expect(personFoldPending("2026-09-10 10:00:00.000", ["2026-09-10 11:00:00.000"], true)).toBe(true);
    expect(personFoldPending(null, [], true)).toBe(true);
    expect(personFoldPending(null, [], false)).toBe(false);
  });
});
