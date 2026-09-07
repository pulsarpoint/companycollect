import { describe, expect, it } from "vitest";
import {
  ADDRESS_KINDS, ADDRESS_SOURCES, REVIEWER_KINDS, addressFoldPending, addressKindLabel, addressSourceLabel,
  geocodeStatusLabel, isAddressKind, isAddressSource, selectedAddressFromSearch, validateSeAddressInput,
} from "~/lib/se-address-fields";

const KEY = "a".repeat(64);
const base = { careOf: "", streetLine: "Storgatan 5", postalCode: "111 22", city: "Stockholm", country: "SE", kind: "postal", note: "" };

describe("address catalogue", () => {
  it("names the five sources and six kinds of the entity", () => {
    expect([...ADDRESS_SOURCES]).toEqual(["scb", "bolagsverket", "ratsit", "reviewer", "reviewer_draft"]);
    expect([...ADDRESS_KINDS]).toEqual(["postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown"]);
    expect([...REVIEWER_KINDS]).toEqual(["postal", "visiting", "visiting_or_postal", "registered"]);
    expect(isAddressSource("ratsit") && !isAddressSource("llm")).toBe(true);
    expect(isAddressKind("registered") && !isAddressKind("home")).toBe(true);
    expect(addressSourceLabel("bolagsverket")).toBe("Bolagsverket");
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
