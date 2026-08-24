import { describe, expect, it } from "vitest";
import {
  buildCorrectionInput,
  liveOverrideRefusal,
  payloadFor,
} from "~/lib/se-address-review-form";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-address-corrections";

const KEY = "a".repeat(64);
const OTHER_KEY = "c".repeat(64);
const HASH = "b".repeat(64);
const CORRECTION_ID = "33333333-3333-4333-8333-333333333333";
const params = { companyId: "5565200028" };

function form(entries: Record<string, string>): FormData {
  const data = new FormData();
  for (const [name, value] of Object.entries(entries)) data.append(name, value);
  return data;
}

/** The hidden fields every override form on the page carries. */
const overrideBase = {
  correction_kind: "override_field",
  address_key: KEY,
  evidence_hash: HASH,
  reason: "Care-of was wrong.",
};

describe("buildCorrectionInput", () => {
  it("sends only the fields the reviewer actually changed", () => {
    const built = buildCorrectionInput(
      form({
        ...overrideBase,
        care_of: "c/o Anna",
        original_care_of: "c/o Bo",
        city: "Stockholm",
        original_city: "Stockholm",
      }),
      params,
    );
    expect(built.ok).toBe(true);
    // An untouched city must not enter the payload: Dagster replays the
    // correction on every run, so sending it would pin the computed value for
    // ever.
    if (built.ok) {
      expect(built.input.payload).toEqual({ address_key: KEY, care_of: "c/o Anna" });
    }
  });

  it("turns a clear checkbox into an explicit null", () => {
    const built = buildCorrectionInput(
      form({
        ...overrideBase,
        reason: "No care-of on this address.",
        care_of: "c/o Bo",
        original_care_of: "c/o Bo",
        clear_care_of: "yes",
      }),
      params,
    );
    expect(built.ok).toBe(true);
    if (built.ok) {
      expect(built.input.payload).toEqual({ address_key: KEY, care_of: null });
    }
  });

  it("refuses an override where nothing moved", () => {
    const built = buildCorrectionInput(
      form({ ...overrideBase, reason: "No change.", care_of: "c/o Bo", original_care_of: "c/o Bo" }),
      params,
    );
    expect(built).toEqual({ ok: false, error: "Nothing changed." });
  });

  /**
   * An emptied input with its clear box unticked trims to "" and is
   * indistinguishable from "untouched", so it would be dropped in silence --
   * point at the checkbox instead, and do it BEFORE the empty-payload test so a
   * change to another field cannot carry the emptied one silently past it.
   */
  it("points at the clear checkbox when a field was emptied without ticking it", () => {
    expect(
      buildCorrectionInput(
        form({ ...overrideBase, care_of: "", original_care_of: "c/o Bo" }),
        params,
      ),
    ).toEqual({ ok: false, error: "To clear Care of, tick its box." });
    expect(
      buildCorrectionInput(
        form({
          ...overrideBase,
          care_of: "",
          original_care_of: "c/o Bo",
          city: "Solna",
          original_city: "Stockholm",
        }),
        params,
      ),
    ).toEqual({ ok: false, error: "To clear Care of, tick its box." });
    // Already empty before the reviewer arrived: nothing was cleared, so the
    // generic refusal is the honest one.
    expect(
      buildCorrectionInput(form({ ...overrideBase, care_of: "", original_care_of: "" }), params),
    ).toEqual({ ok: false, error: "Nothing changed." });
  });

  it("passes the whole override through with the hash and key the form carried", () => {
    const built = buildCorrectionInput(
      form({ ...overrideBase, street_address: "Borgargatan 16", original_street_address: "Borgargatan 1" }),
      params,
    );
    expect(built).toEqual({
      ok: true,
      input: {
        companyId: params.companyId,
        kind: "override_field",
        payload: { address_key: KEY, street_address: "Borgargatan 16" },
        evidenceHash: HASH,
        reason: "Care-of was wrong.",
        supersedesCorrectionId: null,
      },
    });
  });

  it("sends a reject as the address key alone", () => {
    const built = buildCorrectionInput(
      form({
        correction_kind: "reject_address",
        address_key: KEY,
        evidence_hash: HASH,
        reason: "The accountant's address, not the company's.",
        // A stray field a reject may not decide never reaches the validator.
        care_of: "c/o Anna",
        original_care_of: "c/o Bo",
      }),
      params,
    );
    expect(built).toEqual({
      ok: true,
      input: {
        companyId: params.companyId,
        kind: "reject_address",
        payload: { address_key: KEY },
        evidenceHash: HASH,
        reason: "The accountant's address, not the company's.",
        supersedesCorrectionId: null,
      },
    });
  });

  /** Undo supersedes a decision, not evidence: it names no address and always
   * carries the zero hash, whatever the form posted. */
  it("forces the zero evidence hash on undo and carries the superseded id", () => {
    const built = buildCorrectionInput(
      form({
        correction_kind: "undo",
        evidence_hash: HASH,
        address_key: KEY,
        supersedes_correction_id: CORRECTION_ID,
        reason: "Wrong call.",
      }),
      params,
    );
    expect(built).toEqual({
      ok: true,
      input: {
        companyId: params.companyId,
        kind: "undo",
        payload: {},
        evidenceHash: ZERO_EVIDENCE_HASH,
        reason: "Wrong call.",
        supersedesCorrectionId: CORRECTION_ID,
      },
    });
  });

  it("refuses a kind the ledger does not define, before building any payload", () => {
    expect(
      buildCorrectionInput(
        form({ correction_kind: "delete_address", address_key: KEY, evidence_hash: HASH, reason: "x" }),
        params,
      ),
    ).toEqual({ ok: false, error: "Unknown correction kind." });
    expect(
      buildCorrectionInput(form({ address_key: KEY, evidence_hash: HASH, reason: "x" }), params),
    ).toEqual({ ok: false, error: "Unknown correction kind." });
  });
});

describe("payloadFor", () => {
  it("names the address key on the two kinds that decide a row, and nothing on undo", () => {
    expect(payloadFor(form({ address_key: KEY, care_of: "c/o A" }), "override_field")).toEqual({
      address_key: KEY,
      care_of: "c/o A",
    });
    expect(payloadFor(form({ address_key: KEY }), "reject_address")).toEqual({
      address_key: KEY,
    });
    expect(payloadFor(form({ address_key: KEY }), "undo")).toEqual({});
  });

  it("collects every overridable field that moved, each as text or an explicit null", () => {
    expect(
      payloadFor(
        form({
          address_key: KEY,
          care_of: "c/o Anna",
          original_care_of: "",
          street_address: "Borgargatan 16",
          original_street_address: "Borgargatan 16",
          postal_code: "11734",
          original_postal_code: "11733",
          city: "Stockholm",
          original_city: "Stockholm",
          country_code: "SE",
          original_country_code: "SE",
          normalized_address: "x",
          original_normalized_address: "x",
          clear_normalized_address: "yes",
        }),
        "override_field",
      ),
    ).toEqual({
      address_key: KEY,
      care_of: "c/o Anna",
      normalized_address: null,
      postal_code: "11734",
    });
  });
});

describe("liveOverrideRefusal", () => {
  const row = (over: Partial<Parameters<typeof liveOverrideRefusal>[2][number]> = {}) => ({
    correction_id: "22222222-2222-4222-8222-222222222222",
    correction_kind: "override_field",
    address_key: KEY,
    supersedes_correction_id: null,
    is_current: 1,
    is_stale: 0,
    created_at: "2026-08-24 09:00:00.000",
    ...over,
  });

  it("refuses a second override of a row that already carries a live one", () => {
    expect(liveOverrideRefusal("override_field", KEY, [row()])).toBe(
      "This address already has a live override — undo it before overriding again.",
    );
  });

  it("is per address key, and silent when no live override stands", () => {
    expect(liveOverrideRefusal("override_field", OTHER_KEY, [row()])).toBeNull();
    expect(liveOverrideRefusal("override_field", KEY, [])).toBeNull();
    expect(liveOverrideRefusal("override_field", KEY, [row({ is_stale: 1 })])).toBeNull();
  });

  /**
   * Only a second override is misleading. A reject and an override decide
   * different questions (ADDRESS_KIND_ORDER lets both stand), and an undo is
   * the way OUT of a live override -- refusing either would trap the reviewer.
   */
  it("never refuses a reject or an undo", () => {
    for (const kind of ["reject_address", "undo"]) {
      expect(liveOverrideRefusal(kind, KEY, [row()])).toBeNull();
    }
  });
});
