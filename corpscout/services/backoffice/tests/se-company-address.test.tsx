import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";

/**
 * `SeCompanyAddressTab` (the OLD address entity's component -- spec
 * 2026-09-06 section 9 slice 5's cutover retires it). Extracted verbatim from
 * `tests/admin-se-company-address.test.tsx` when that file was rewritten to
 * test only the NEW `admin-se-company-address` route over the new address
 * entity: ruling 5 of `2026-09-07-se-company-address-3-backoffice.md` leaves
 * this component and its tests in place until the cutover, but it is no
 * longer reachable through that route, so its coverage needs a home of its
 * own. Nothing in this file changed from the original.
 */

import {
  SeCompanyAddressTab,
  type SeCompanyAddressReviewResult,
} from "~/components/admin/se-company-address";
import {
  OVERRIDABLE_FIELDS,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-address-corrections";
import type {
  SeCompanyAddressCorrectionRow,
  SeCompanyAddressDetail,
  SeCompanyAddressRow,
} from "~/lib/se-company-address.server";

const COMPANY_ID = "5560125220";
const KEY = "f".repeat(64);
const OTHER_KEY = "e".repeat(64);
const HASH = "a".repeat(64);
const OVERRIDE_CORRECTION_ID = "22222222-2222-4222-8222-222222222222";
const REJECT_CORRECTION_ID = "11111111-1111-4111-8111-111111111111";

const address: SeCompanyAddressRow = {
  address_key: KEY,
  address_type: "postal",
  care_of: "Nicklas",
  street_address: "Borgargatan 16, lgh 1302",
  normalized_address: "borgargatan 16 lgh 1302|11734|stockholm|se",
  postal_code: "11734",
  city: "STOCKHOLM",
  country_code: "SE",
  address_id: "9".repeat(64),
  latitude: "59.3167337",
  longitude: "18.0347148",
  geocode_status: "matched_exact",
  geocoded_at: "2026-08-17 20:14:13.671",
  sources: ["bolagsverket", "scb"],
  source_record_uids: ["bolagsverket:5560125220:postal"],
  evidence_set_hash: HASH,
  correction_ids: [],
  resolved_at: "2026-08-24 09:12:00.000",
};

const secondAddress: SeCompanyAddressRow = {
  ...address,
  address_key: OTHER_KEY,
  address_type: "visiting",
  evidence_set_hash: "b".repeat(64),
};

const correction = (
  over: Partial<SeCompanyAddressCorrectionRow> = {},
): SeCompanyAddressCorrectionRow => ({
  correction_id: REJECT_CORRECTION_ID,
  correction_kind: "reject_address",
  payload: JSON.stringify({ address_key: KEY }),
  address_key: KEY,
  evidence_hash: HASH,
  reason: "The accountant's address, not the company's.",
  decided_by: "backoffice",
  supersedes_correction_id: null,
  created_at: "2026-08-24 08:00:00.000",
  is_current: 1,
  is_stale: 0,
  is_applied: 0,
  ...over,
});

const liveOverride = correction({
  correction_id: OVERRIDE_CORRECTION_ID,
  correction_kind: "override_field",
  payload: JSON.stringify({ address_key: KEY, care_of: "c/o Anna" }),
  reason: "Care-of was wrong.",
});

const empty: SeCompanyAddressDetail = { addresses: [], removed: [], corrections: [] };

function render(
  detail: SeCompanyAddressDetail,
  result: SeCompanyAddressReviewResult = null,
): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: <SeCompanyAddressTab detail={detail} result={result} />,
        action: () => null,
      },
    ],
    { initialEntries: [`/admin/se/company/${COMPANY_ID}/address`] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

/** The innermost `<form>` body containing every needle, so per-form assertions
 * stay scoped to the one decision they are about -- a card carries three forms
 * over the same address key, so one needle is not enough to name one of them. */
function formContaining(html: string, ...needles: string[]): string {
  for (const part of html.split("<form")) {
    const end = part.indexOf("</form>");
    const body = end === -1 ? part : part.slice(0, end);
    if (needles.every((needle) => body.includes(needle))) return body;
  }
  throw new Error(`no <form> containing ${needles.join(" + ")}`);
}

const OVERRIDE = 'name="correction_kind" value="override_field"';
const REJECT = 'name="correction_kind" value="reject_address"';
const UNDO = 'name="correction_kind" value="undo"';

function count(html: string, needle: string): number {
  return html.split(needle).length - 1;
}

describe("address tab review controls", () => {
  it("gives every published address its own override form, keyed and hashed to that row", () => {
    const html = render({ ...empty, addresses: [address, secondAddress] });
    expect(count(html, OVERRIDE)).toBe(2);

    const form = formContaining(html, OVERRIDE, `name="address_key" value="${KEY}"`);
    // The hash the reviewer was SHOWN travels with the decision: the append
    // re-reads the row and refuses a decision made against evidence that moved.
    expect(form).toContain(`name="evidence_hash" value="${HASH}"`);
    for (const field of OVERRIDABLE_FIELDS) {
      expect(form, field).toContain(`name="${field}"`);
      expect(form, field).toContain(`name="original_${field}"`);
      expect(form, field).toContain(`name="clear_${field}"`);
    }
    // The original is the text the reviewer sees, so an untouched field can be
    // diffed away server-side rather than pinned for ever.
    expect(form).toContain('name="original_care_of" value="Nicklas"');
    expect(form).toContain('aria-label="Reason"');
    // Each card decides its own row: the second card carries the other key and
    // the other row's evidence hash.
    const second = formContaining(html, OVERRIDE, `name="address_key" value="${OTHER_KEY}"`);
    expect(second).toContain(`name="evidence_hash" value="${"b".repeat(64)}"`);
  });

  it("offers a reject per published address, deciding only that address key", () => {
    const html = render({ ...empty, addresses: [address, secondAddress] });
    expect(count(html, REJECT)).toBe(2);
    const form = formContaining(html, REJECT);
    expect(form).toContain('name="address_key"');
    expect(form).toContain('name="evidence_hash"');
    expect(form).toContain('name="reason"');
  });

  it("offers Undo on a live correction, carrying the zero hash and the id it supersedes", () => {
    const html = render({
      ...empty,
      addresses: [address],
      corrections: [liveOverride],
    });
    const form = formContaining(html, UNDO);
    expect(form).toContain(
      `name="supersedes_correction_id" value="${OVERRIDE_CORRECTION_ID}"`,
    );
    // Undo supersedes a decision, not evidence.
    expect(form).toContain(`name="evidence_hash" value="${ZERO_EVIDENCE_HASH}"`);
    expect(form).toContain('aria-label="Why undo"');
  });

  it("does not offer to undo an undo", () => {
    const html = render({
      ...empty,
      addresses: [address],
      corrections: [
        correction({
          correction_id: "44444444-4444-4444-8444-444444444444",
          correction_kind: "undo",
          payload: "{}",
          address_key: "",
          evidence_hash: ZERO_EVIDENCE_HASH,
          supersedes_correction_id: OVERRIDE_CORRECTION_ID,
          reason: "Wrong call.",
        }),
        { ...liveOverride, is_current: 0 },
      ],
    });
    expect(count(html, UNDO)).toBe(0);
  });

  /**
   * Dagster's kind-ranking always lets a live override win, and a SECOND
   * override of the same row wins by created_at and buries the first -- so the
   * form is closed and the reviewer is pointed at the undo instead.
   */
  it("closes the override form of a row that already carries a live override", () => {
    const html = render({
      ...empty,
      addresses: [address, secondAddress],
      corrections: [liveOverride],
    });
    expect(html).toContain(
      "This address already has a live override — undo it before overriding again.",
    );
    // `disabled=""` is the rendered attribute; the Tailwind classes carry
    // `disabled:` variants on every input, so the bare word proves nothing.
    expect(
      formContaining(html, OVERRIDE, `name="address_key" value="${KEY}"`),
    ).toContain('disabled=""');
    // Only that row: the other address is still overridable.
    expect(
      formContaining(html, OVERRIDE, `name="address_key" value="${OTHER_KEY}"`),
    ).not.toContain('disabled=""');
  });

  it("marks the card whose correction no longer matches the row's evidence", () => {
    const html = render({
      ...empty,
      addresses: [address],
      corrections: [correction({ is_stale: 1 })],
    });
    expect(html).toContain("evidence changed");
    expect(html).toContain(">stale<");
  });

  /**
   * An override written against a reject-tombstoned row is the stale trap this
   * page must never create: Dagster drops it on the next run without telling
   * anyone. The removed section offers the one control that can act on such a
   * row -- undo.
   */
  it("offers only Undo in the removed section, never an override or another reject", () => {
    const html = render({
      addresses: [],
      removed: [{ ...address, correction_ids: [REJECT_CORRECTION_ID] }],
      corrections: [correction({ is_applied: 1 })],
    });
    expect(html).toContain("Removed / rejected");
    expect(html).toContain(UNDO);
    expect(html).not.toContain(OVERRIDE);
    expect(html).not.toContain(REJECT);
  });

  it("reports what the action decided, in the reviewer's own words", () => {
    const saved = render({ ...empty, addresses: [address] }, {
      ok: true,
      correctionId: OVERRIDE_CORRECTION_ID,
    });
    expect(saved).toContain("Saved");
    expect(saved).toContain(OVERRIDE_CORRECTION_ID);
    const refused = render({ ...empty, addresses: [address] }, {
      ok: false,
      error: "The evidence changed while you were reviewing. Reload and decide again.",
    });
    expect(refused).toContain("Not saved");
    expect(refused).toContain(
      "The evidence changed while you were reviewing. Reload and decide again.",
    );
  });

  it("still says so, with no form at all, when no source recorded an address", () => {
    const html = render(empty);
    expect(html).toContain("No address recorded");
    expect(html).not.toContain("<form");
  });
});
