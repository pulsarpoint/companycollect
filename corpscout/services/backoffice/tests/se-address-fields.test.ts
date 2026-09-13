import { describe, expect, it } from "vitest";
import {
  ADDRESS_KINDS, ADDRESS_SOURCES, REVIEWER_KINDS, addressFoldPending, addressKindLabel, addressSearchString,
  addressSourceLabel, geocodeStatusLabel, isAddressKind, isAddressSource, MAX_WORKPLACE_PAGE,
  MAX_WORKPLACE_QUERY_LENGTH, selectedAddressFromSearch, validateSeAddressInput, WORKPLACE_PAGE_SIZE,
  workplacePageFromSearch, workplaceQueryFromSearch,
} from "~/lib/se-address-fields";

const KEY = "a".repeat(64);
const base = { careOf: "", streetLine: "Storgatan 5", postalCode: "111 22", city: "Stockholm", country: "SE", kind: "postal", note: "" };

describe("address catalogue", () => {
  it("names the six sources and six kinds of the entity", () => {
    expect([...ADDRESS_SOURCES]).toEqual(["scb", "bolagsverket", "ratsit", "esef", "reviewer", "reviewer_draft"]);
    expect([...ADDRESS_KINDS]).toEqual(["postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown"]);
    expect([...REVIEWER_KINDS]).toEqual(["postal", "visiting", "visiting_or_postal", "registered"]);
    expect(isAddressSource("ratsit") && !isAddressSource("llm")).toBe(true);
    expect(isAddressKind("registered") && !isAddressKind("home")).toBe(true);
    expect(addressSourceLabel("bolagsverket")).toBe("Bolagsverket");
    expect(addressSourceLabel("esef")).toBe("ESEF");
    expect(addressSourceLabel("reviewer_draft")).toBe("Reviewer draft");
    expect(addressKindLabel("visiting_or_postal")).toBe("Visiting or postal");
    expect(geocodeStatusLabel("matched_area")).toBe("Area (centroid)");
    expect(geocodeStatusLabel("")).toBe("Not geocoded");
  });
  it("reads the selected address key from the search params", () => {
    expect(selectedAddressFromSearch(new URLSearchParams(`address=${KEY}`))).toBe(KEY);
    expect(selectedAddressFromSearch(new URLSearchParams("address=nope"))).toBeNull();
    expect(selectedAddressFromSearch(new URLSearchParams())).toBeNull();
  });
});

describe("validateSeAddressInput", () => {
  it("accepts a street line with a spaced postcode and normalizes the postcode", () => {
    const result = validateSeAddressInput(base);
    expect(result).toEqual({ ok: true, input: { ...base, postalCode: "11122" } });
  });
  it("accepts a box line and a care-of", () => {
    expect(validateSeAddressInput({ ...base, streetLine: "Box 123", careOf: "Anna Svensson" }).ok).toBe(true);
  });
  it("refuses a missing street line, a bad postcode, a missing city, a bad kind, a foreign country", () => {
    expect(validateSeAddressInput({ ...base, streetLine: " " })).toEqual({ ok: false, error: "A street line or a box is required." });
    expect(validateSeAddressInput({ ...base, postalCode: "1112" })).toEqual({ ok: false, error: "Postcode must be five digits." });
    expect(validateSeAddressInput({ ...base, city: "" })).toEqual({ ok: false, error: "City is required." });
    expect(validateSeAddressInput({ ...base, kind: "home" })).toEqual({ ok: false, error: "Unknown address kind." });
    expect(validateSeAddressInput({ ...base, country: "NO" })).toEqual({ ok: false, error: "Only Swedish addresses can be typed here." });
  });
  it("caps lengths and refuses control characters", () => {
    expect(validateSeAddressInput({ ...base, streetLine: "x".repeat(201) })).toEqual({ ok: false, error: "Street line is longer than 200 characters." });
    expect(validateSeAddressInput({ ...base, careOf: "x".repeat(201) })).toEqual({ ok: false, error: "Care-of is longer than 200 characters." });
    expect(validateSeAddressInput({ ...base, city: "x".repeat(101) })).toEqual({ ok: false, error: "City is longer than 100 characters." });
    expect(validateSeAddressInput({ ...base, note: "x".repeat(501) })).toEqual({ ok: false, error: "Note is longer than 500 characters." });
    expect(validateSeAddressInput({ ...base, streetLine: "Storgatan\u00075" })).toEqual({ ok: false, error: "Street line must be plain text." });
  });
  it("keeps the line breaks of a multi-line note but still refuses a control character in it", () => {
    // The note is a Textarea, and a browser submits its newlines as CRLF.
    const multiline = validateSeAddressInput({ ...base, note: "moved next door\nsee the letter\r\nfrom 2026-09" });
    expect(multiline).toEqual({ ok: true, input: { ...base, postalCode: "11122", note: "moved next door\nsee the letter\r\nfrom 2026-09" } });
    expect(validateSeAddressInput({ ...base, note: "moved\u0007next door" })).toEqual({ ok: false, error: "Note must be plain text." });
  });
  it("trims every field and keeps the reviewer's casing", () => {
    const result = validateSeAddressInput({ ...base, streetLine: "  Storgatan 5 ", city: " Stockholm " });
    expect(result.ok && result.input.streetLine).toBe("Storgatan 5");
    expect(result.ok && result.input.city).toBe("Stockholm");
  });
});

describe("addressFoldPending", () => {
  it("is pending when a stamp is newer than the fold, or when nothing was folded but rows exist", () => {
    expect(addressFoldPending("2026-09-07 10:00:00.000", ["2026-09-07 09:00:00.000"], true)).toBe(false);
    expect(addressFoldPending("2026-09-07 10:00:00.000", ["2026-09-07 11:00:00.000"], true)).toBe(true);
    expect(addressFoldPending(null, [], true)).toBe(true);
    expect(addressFoldPending(null, [], false)).toBe(false);
  });
});

describe("workplace paging search params", () => {
  it("pages fifty rows at a time", () => {
    // Spec 8 (amended 2026-09-13): the Workplaces card is paged server-side,
    // fifty rows a page. The number is a contract between the loader's OFFSET
    // and the card's "<from>-<to> of <total>" footer, so it is pinned here.
    expect(WORKPLACE_PAGE_SIZE).toBe(50);
    expect(MAX_WORKPLACE_QUERY_LENGTH).toBe(100);
  });

  it("reads a 1-based page and calls anything that is not a whole number page 1", () => {
    expect(workplacePageFromSearch(new URLSearchParams("workplaces=3"))).toBe(3);
    expect(workplacePageFromSearch(new URLSearchParams("workplaces=31"))).toBe(31);
    expect(workplacePageFromSearch(new URLSearchParams())).toBe(1);
    for (const raw of ["0", "-2", "2.5", "abc", "", "%20", "1e3", "01x"]) {
      expect(workplacePageFromSearch(new URLSearchParams(`workplaces=${raw}`))).toBe(1);
    }
  });

  it("clamps a page past MAX_WORKPLACE_PAGE rather than overflowing the loader's UInt32 offset", () => {
    // The loader binds `(page - 1) * WORKPLACE_PAGE_SIZE` as `{offset:UInt32}`;
    // an unclamped hand-typed page would overflow that and 500 the route.
    expect(MAX_WORKPLACE_PAGE).toBe(100_000);
    expect(workplacePageFromSearch(new URLSearchParams("workplaces=90000000"))).toBe(100_000);
    expect(workplacePageFromSearch(new URLSearchParams("workplaces=100001"))).toBe(100_000);
    expect(workplacePageFromSearch(new URLSearchParams("workplaces=100000"))).toBe(100_000);
  });

  it("trims the filter and caps it at a hundred characters", () => {
    // `+` is a space in a query string, so this is the filter box's own posting.
    expect(workplaceQueryFromSearch(new URLSearchParams("workplace_q=+storgatan+"))).toBe("storgatan");
    expect(workplaceQueryFromSearch(new URLSearchParams("workplace_q=Box%20123"))).toBe("Box 123");
    expect(workplaceQueryFromSearch(new URLSearchParams())).toBe("");
    expect(workplaceQueryFromSearch(new URLSearchParams("workplace_q=   "))).toBe("");
    const long = workplaceQueryFromSearch(new URLSearchParams(`workplace_q=${"x".repeat(150)}`));
    expect(long).toHaveLength(100);
  });

  it("builds the tab's whole search string and leaves the defaults out", () => {
    expect(addressSearchString({ address: null, workplacePage: 1, workplaceQuery: "" })).toBe("");
    expect(addressSearchString({ address: KEY, workplacePage: 1, workplaceQuery: "" })).toBe(
      `?address=${KEY}`,
    );
    expect(addressSearchString({ address: null, workplacePage: 4, workplaceQuery: "" })).toBe(
      "?workplaces=4",
    );
    // Everything at once, in a stable order: the selected address, the page, the filter.
    expect(addressSearchString({ address: KEY, workplacePage: 2, workplaceQuery: "Box 1" })).toBe(
      `?address=${KEY}&workplaces=2&workplace_q=Box+1`,
    );
    // What it builds is what the readers read back.
    const round = new URLSearchParams(
      addressSearchString({ address: KEY, workplacePage: 2, workplaceQuery: "Box 1" }).slice(1),
    );
    expect(selectedAddressFromSearch(round)).toBe(KEY);
    expect(workplacePageFromSearch(round)).toBe(2);
    expect(workplaceQueryFromSearch(round)).toBe("Box 1");
  });
});
