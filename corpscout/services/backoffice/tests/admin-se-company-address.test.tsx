import type { ReactElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

// Hoisted mock of the ClickHouse-backed module the route imports, so `action`'s
// calls to loadSeCompanyAddresses / appendSeCompanyAddressCorrection are
// directly assertable without a live ClickHouse -- the same idiom
// tests/admin-se-company-info.test.tsx uses one layer up from the query module.
const server = vi.hoisted(() => ({
  loadSeCompanyAddresses: vi.fn(),
  appendSeCompanyAddressCorrection: vi.fn(),
}));
vi.mock("~/lib/se-company-address.server", () => server);

import { action } from "~/routes/admin-se-company-address";
import { SeCompanyAddressCorrectionsTable } from "~/components/admin/se-company-address-corrections-table";
import {
  SeCompanyAddressTab,
  type SeCompanyAddressReviewResult,
} from "~/components/admin/se-company-address";
import {
  OVERRIDABLE_FIELDS,
  SeAddressCorrectionValidationError,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-address-corrections";
import type {
  SeCompanyAddressCorrectionRow,
  SeCompanyAddressDetail,
  SeCompanyAddressRow,
} from "~/lib/se-company-address.server";
import type { SeCompanyAddressCorrectionListRow } from "~/lib/se-company-address-lists.server";
import { EMPTY_CORRECTION_FILTERS } from "~/lib/se-company-info-filters";
import { SeCompanyInfoCorrectionsFilterSheet } from "~/components/admin/se-company-info-filter-sheet";

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

describe("admin-se-company-address action", () => {
  beforeEach(() => {
    server.loadSeCompanyAddresses.mockReset();
    server.appendSeCompanyAddressCorrection.mockReset();
  });

  function postAction(entries: Record<string, string>) {
    const form = new FormData();
    for (const [key, value] of Object.entries(entries)) form.append(key, value);
    return action({
      request: new Request(
        `http://localhost/admin/se/company/${COMPANY_ID}/address`,
        { method: "POST", body: form },
      ),
      params: { companyId: COMPANY_ID },
    } as unknown as Parameters<typeof action>[0]);
  }

  it("appends an override of only the changed fields", async () => {
    server.loadSeCompanyAddresses.mockResolvedValue(empty);
    server.appendSeCompanyAddressCorrection.mockResolvedValue({
      correctionId: OVERRIDE_CORRECTION_ID,
    });

    const result = await postAction({
      correction_kind: "override_field",
      address_key: KEY,
      evidence_hash: HASH,
      reason: "Care-of was wrong.",
      care_of: "c/o Anna",
      original_care_of: "Nicklas",
      city: "STOCKHOLM",
      original_city: "STOCKHOLM",
    });

    expect(result).toEqual({ ok: true, correctionId: OVERRIDE_CORRECTION_ID });
    expect(server.appendSeCompanyAddressCorrection).toHaveBeenCalledWith({
      companyId: COMPANY_ID,
      kind: "override_field",
      payload: { address_key: KEY, care_of: "c/o Anna" },
      evidenceHash: HASH,
      reason: "Care-of was wrong.",
      supersedesCorrectionId: null,
    });
  });

  it("refuses a second override of a row that already carries a live one, without writing", async () => {
    server.loadSeCompanyAddresses.mockResolvedValue({
      ...empty,
      addresses: [address],
      corrections: [liveOverride],
    });

    const result = await postAction({
      correction_kind: "override_field",
      address_key: KEY,
      evidence_hash: HASH,
      reason: "Again.",
      care_of: "c/o Bo",
      original_care_of: "c/o Anna",
    });

    expect(result).toEqual({
      ok: false,
      error: "This address already has a live override — undo it before overriding again.",
    });
    expect(server.appendSeCompanyAddressCorrection).not.toHaveBeenCalled();
  });

  it("never blocks a reject or an undo on the live-override check", async () => {
    server.appendSeCompanyAddressCorrection.mockResolvedValue({
      correctionId: REJECT_CORRECTION_ID,
    });

    const rejected = await postAction({
      correction_kind: "reject_address",
      address_key: KEY,
      evidence_hash: HASH,
      reason: "Not this company's address.",
    });
    expect(rejected).toEqual({ ok: true, correctionId: REJECT_CORRECTION_ID });
    expect(server.appendSeCompanyAddressCorrection).toHaveBeenCalledWith({
      companyId: COMPANY_ID,
      kind: "reject_address",
      payload: { address_key: KEY },
      evidenceHash: HASH,
      reason: "Not this company's address.",
      supersedesCorrectionId: null,
    });

    await postAction({
      correction_kind: "undo",
      supersedes_correction_id: OVERRIDE_CORRECTION_ID,
      reason: "Wrong call.",
    });
    expect(server.appendSeCompanyAddressCorrection).toHaveBeenLastCalledWith({
      companyId: COMPANY_ID,
      kind: "undo",
      payload: {},
      evidenceHash: ZERO_EVIDENCE_HASH,
      reason: "Wrong call.",
      supersedesCorrectionId: OVERRIDE_CORRECTION_ID,
    });
    // Neither kind can be buried by a live override, so neither needs the
    // current ledger to decide.
    expect(server.loadSeCompanyAddresses).not.toHaveBeenCalled();
  });

  it("shows a validation refusal to the reviewer and rethrows anything else", async () => {
    server.appendSeCompanyAddressCorrection.mockRejectedValueOnce(
      new SeAddressCorrectionValidationError("This address is not published."),
    );
    expect(
      await postAction({
        correction_kind: "reject_address",
        address_key: KEY,
        evidence_hash: HASH,
        reason: "Gone.",
      }),
    ).toEqual({ ok: false, error: "This address is not published." });

    server.appendSeCompanyAddressCorrection.mockRejectedValueOnce(
      new Error("ClickHouse is down"),
    );
    await expect(
      postAction({
        correction_kind: "reject_address",
        address_key: KEY,
        evidence_hash: HASH,
        reason: "Gone.",
      }),
    ).rejects.toThrow("ClickHouse is down");
  });

  it("refuses a malformed post before it reaches ClickHouse", async () => {
    expect(
      await postAction({ correction_kind: "delete_address", address_key: KEY, reason: "x" }),
    ).toEqual({ ok: false, error: "Unknown correction kind." });
    expect(
      await postAction({
        correction_kind: "override_field",
        address_key: KEY,
        evidence_hash: HASH,
        reason: "x",
        care_of: "Nicklas",
        original_care_of: "Nicklas",
      }),
    ).toEqual({ ok: false, error: "Nothing changed." });
    expect(server.appendSeCompanyAddressCorrection).not.toHaveBeenCalled();
  });
});

const listRow: SeCompanyAddressCorrectionListRow = {
  correction_id: OVERRIDE_CORRECTION_ID,
  company_id: COMPANY_ID,
  created_at: "2026-08-24 09:00:00.000",
  correction_kind: "override_field",
  address_key: KEY,
  payload: JSON.stringify({ address_key: KEY, care_of: "c/o Anna", city: null }),
  reason: "Care-of was wrong.",
  decided_by: "backoffice",
  supersedes_correction_id: null,
  status: "applied",
};

function renderTable(rows: SeCompanyAddressCorrectionListRow[]): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <SeCompanyAddressCorrectionsTable
            rows={rows}
            total={rows.length}
            page={1}
            pageSize={50}
            sort="created_at"
            dir="desc"
            filters={EMPTY_CORRECTION_FILTERS}
            options={{ decidedBy: ["backoffice"] }}
          />
        ),
      },
    ],
    { initialEntries: ["/admin/se/company-address/corrections"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("address corrections ledger table", () => {
  it("names the address each correction decides and links it to that company's Address tab", () => {
    const html = renderTable([listRow]);
    expect(html).toContain(KEY.slice(0, 8));
    expect(html).toContain(`href="/admin/se/company/${COMPANY_ID}/address"`);
    // The whole key is the title, so a reviewer can copy it out of the list.
    expect(html).toContain(`title="${KEY}"`);
    expect(html).toContain(">applied<");
  });

  it("says what each kind decided, with a cleared field reading as the clear it is", () => {
    const html = renderTable([
      listRow,
      {
        ...listRow,
        correction_id: REJECT_CORRECTION_ID,
        correction_kind: "reject_address",
        payload: JSON.stringify({ address_key: KEY }),
        status: "pending",
      },
      {
        ...listRow,
        correction_id: "44444444-4444-4444-8444-444444444444",
        correction_kind: "undo",
        payload: "{}",
        // An undo names a correction, not an address: it has no card to link to.
        address_key: "",
        supersedes_correction_id: OVERRIDE_CORRECTION_ID,
        status: "undone",
      },
    ]);
    expect(html).toContain("care_of = c/o Anna, clear city");
    expect(html).toContain("not an address of this company");
    expect(html).toContain(`undo ${OVERRIDE_CORRECTION_ID.slice(0, 8)}`);
    expect(html).toContain(">—<");
  });

  /**
   * The filter sheet is shared with the info ledger (SeCompanyInfoCorrectionsFilterSheet);
   * only the `kinds`/`statuses` props tell the two apart, and the Select's own
   * option list never reaches static markup (Base UI's popup only mounts once
   * opened -- see SelectPortal), so the props actually passed in are inspected
   * directly instead of grepping rendered HTML. Guards against those props
   * regressing to the info ledger's defaults, which would offer filters this
   * ledger cannot decide.
   */
  it("passes this ledger's own kinds to the shared filter sheet, not the info ledger's", () => {
    const tree = SeCompanyAddressCorrectionsTable({
      rows: [listRow],
      total: 1,
      page: 1,
      pageSize: 50,
      sort: "created_at",
      dir: "desc",
      filters: EMPTY_CORRECTION_FILTERS,
      options: { decidedBy: ["backoffice"] },
    }) as ReactElement<{ children: ReactElement[] }>;
    const sheet = tree.props.children.find(
      (child): child is ReactElement<{ kinds: readonly string[] }> =>
        child.type === SeCompanyInfoCorrectionsFilterSheet,
    );
    expect(sheet?.props.kinds).toContain("reject_address");
    expect(sheet?.props.kinds).not.toContain("approve_suggestion");
  });
});
