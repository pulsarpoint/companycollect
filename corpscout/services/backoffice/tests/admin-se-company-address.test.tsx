import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

// Hoisted mock of the address entity's server module, wholesale, the same
// idiom tests/admin-se-company-basic-info.test.tsx uses one layer up from its
// store -- the loader/action tests below assert the route dispatches to
// these functions; they never touch a live ClickHouse or Dagster.
const server = vi.hoisted(() => ({
  loadSeAddressDetail: vi.fn(),
  removeSeAddress: vi.fn(),
  resetSeAddress: vi.fn(),
  saveSeAddressDraft: vi.fn(),
  activateSeAddressDraft: vi.fn(),
  discardSeAddressDraft: vi.fn(),
  launchSeAddressFold: vi.fn(),
  SeAddressDecisionError: class SeAddressDecisionError extends Error {},
}));
vi.mock("~/lib/se-company-address-entity.server", () => server);

import { action, loader } from "~/routes/admin-se-company-address";
import { removeSuccessCopy, SeAddressWorkspace } from "~/components/admin/se-address-workspace";
import type {
  SeAddressDetail,
  SeAddressDraft,
  SeAddressPublished,
  SeAddressRawRow,
  SeAddressRow,
} from "~/lib/se-company-address-entity.server";

const COMPANY = "5560125220";
const KEY = "a".repeat(64);
const SLOT = "r20260905090200000";

const row: SeAddressRow = {
  company_id: COMPANY,
  address_key: KEY,
  care_of: "",
  box: "",
  street_name: "Storgatan",
  house_number: "5",
  unit: "",
  postal_code: "11122",
  city: "Stockholm",
  country_code: "SE",
  normalized_address: "storgatan 5|11122|stockholm|se",
  kinds: ["postal"],
  sources: ["bolagsverket"],
  slots: [""],
  normalized_ids: ["norm-1"],
  text_source: "bolagsverket",
  active: 1,
  inactive_reason: "",
  latitude: 59.33,
  longitude: 18.06,
  geocode_status: "matched_exact",
  geocode_method: "osm",
  geocode_confidence: 0.98,
  geocode_precision: "rooftop",
  geocode_policy: "v7",
  geocode_reference: "ref-1",
  geocoded_at: "2026-09-01 12:00:00.000",
  normalizer_version: "se-address-normalizer-v2",
  folded_at: "2026-09-02 08:00:00.000",
  fold_version: "fold-v1",
  source_run_id: "run-f",
};

const rawRow: SeAddressRawRow = {
  company_id: COMPANY,
  source: "bolagsverket",
  slot: "",
  suggestion_id: "sug-1",
  source_record_uid: "rec-1",
  observed_at: "2026-09-01 08:00:00.000",
  kind: "postal",
  raw_address: "",
  care_of: "",
  street_address: "Storgatan 5",
  postal_code: "11122",
  post_town: "Stockholm",
  county: "",
  country_code: "SE",
  decided_by: "",
  note: "",
  replaces_key: "",
  suggested_at: "2026-09-01 08:00:00.000",
  source_run_id: "run-b",
  extractor_version: "bolagsverket-v2",
};

const published: SeAddressPublished = {
  row,
  members: [
    {
      source: "bolagsverket",
      slot: "",
      normalizedId: "norm-1",
      current: null,
      raw: rawRow,
      refoldPending: false,
      completeness: 6,
    },
  ],
  textSourceReason: "single source",
  hideRule: null,
};

/** A second active address of the same company, geocoded no finer than its
 * postcode -- two rows for the list's one map to carry. */
const SECOND_KEY = "b".repeat(64);
const secondPublished: SeAddressPublished = {
  ...published,
  row: {
    ...row,
    address_key: SECOND_KEY,
    street_name: "Kungsgatan",
    house_number: "12",
    postal_code: "11143",
    normalized_address: "kungsgatan 12|11143|stockholm|se",
    latitude: 59.34,
    longitude: 18.07,
    geocode_precision: "postcode",
  },
};

const detail: SeAddressDetail = {
  published: [published],
  drafts: [],
  history: [],
  rules: [],
  foldPending: false,
};

/** What the route hands the workspace for a company no source has an address
 * for (and what `loadSeAddressDetail` returning null becomes). */
const EMPTY_DETAIL: SeAddressDetail = {
  published: [],
  drafts: [],
  history: [],
  rules: [],
  foldPending: false,
};

/** A Correct in progress: the draft replaces the published row above and no
 * normalize run has parsed it yet. */
const draft: SeAddressDraft = {
  slot: SLOT,
  raw: {
    ...rawRow,
    source: "reviewer_draft",
    slot: SLOT,
    suggestion_id: "sug-draft",
    source_record_uid: "",
    kind: "visiting",
    care_of: "Anna Svensson",
    street_address: "Storgatan 7",
    decided_by: "backoffice",
    note: "moved next door",
    replaces_key: KEY,
    source_run_id: "backoffice",
    extractor_version: "backoffice-v1",
  },
  normalized: null,
  replacesKey: KEY,
};

function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/address", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/address${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SeAddressWorkspace", () => {
  it("renders the published address line with its kind, source and geocode badges, and marks the selected row", () => {
    const html = render(
      <SeAddressWorkspace companyId={COMPANY} detail={detail} selectedKey={KEY} result={null} />,
    );
    expect(html).toContain("storgatan 5|11122|stockholm|se");
    expect(html).toContain("Postal");
    expect(html).toContain("Bolagsverket");
    expect(html).toContain("Exact");
    expect(html).toContain('aria-current="true"');
  });

  it("shows the fold-pending alert only when the detail says so", () => {
    const pending = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={{ ...detail, foldPending: true }}
        selectedKey={null}
        result={null}
      />,
    );
    expect(pending).toContain("Fold pending");
    const settled = render(
      <SeAddressWorkspace companyId={COMPANY} detail={detail} selectedKey={null} result={null} />,
    );
    expect(settled).not.toContain("Fold pending");
  });

  it("shows the empty state, with Add address still in reach, for a company with no rows", () => {
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={EMPTY_DETAIL}
        selectedKey={null}
        result={null}
      />,
    );
    expect(html).toContain("No addresses published yet");
    expect(html).toContain("Add address");
  });

  it("renders a draft with its actions, what it replaces and that nothing has parsed it", () => {
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={{ ...detail, drafts: [draft] }}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(html).toContain(`data-slot-id="${SLOT}"`);
    expect(html).toContain("Anna Svensson, Storgatan 7, 11122 Stockholm");
    expect(html).toContain("moved next door");
    expect(html).toContain("Visiting");
    // Not parsed yet: the fold is what turns the draft into an address.
    expect(html).toContain("parses on Fold now");
    // The published row the Correct replaces, named by its own line.
    expect(html).toContain("Replaces");
    expect(html).toContain("storgatan 5|11122|stockholm|se");
    for (const label of ["Edit", "Activate", "Discard"]) {
      expect(html).toContain(`>${label}</button>`);
    }
  });

  it("maps the active addresses once for the whole list, and links the selected one out to OpenStreetMap", () => {
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={{ ...detail, published: [published, secondPublished] }}
        selectedKey={KEY}
        result={null}
      />,
    );
    // The map is client-only, so what renders here is its placeholder: one for
    // the list -- not one per row -- and one for the panel's single point.
    const placeholders = html.match(/bg-muted text-muted-foreground flex h-56/g) ?? [];
    expect(placeholders).toHaveLength(2);
    expect(html).toContain("Open in OpenStreetMap");
    // Zoom 18: the selected row's geocode is exact. `&` is escaped in an
    // attribute, so the href reads with `&amp;`.
    expect(html).toContain(
      "https://www.openstreetmap.org/?mlat=59.33&amp;mlon=18.06#map=18/59.33/18.06",
    );
  });

  it("says what Remove did, in the wording of the row it acted on", () => {
    // The workspace has not been clicked through server-side, so the alert
    // shows the whole-address wording; the reviewer-only one is the helper's.
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={detail}
        selectedKey={KEY}
        result={{ ok: true, intent: "remove" }}
      />,
    );
    expect(html).toContain("Address hidden; fold to apply.");
    expect(removeSuccessCopy(false)).toBe("Address hidden; fold to apply.");
    expect(removeSuccessCopy(true)).toBe("Reviewer address withdrawn; fold to apply.");
  });
});

describe("admin-se-company-address route", () => {
  beforeEach(() => {
    server.loadSeAddressDetail.mockReset().mockResolvedValue(detail);
    server.removeSeAddress.mockReset().mockResolvedValue({ decidedAt: "2026-09-05 09:00:00.000" });
    server.resetSeAddress.mockReset().mockResolvedValue({ decidedAt: "2026-09-05 09:01:00.000" });
    server.saveSeAddressDraft
      .mockReset()
      .mockResolvedValue({ decidedAt: "2026-09-05 09:02:00.000", slot: SLOT });
    server.activateSeAddressDraft
      .mockReset()
      .mockResolvedValue({ decidedAt: "2026-09-05 09:03:00.000" });
    server.discardSeAddressDraft
      .mockReset()
      .mockResolvedValue({ decidedAt: "2026-09-05 09:04:00.000" });
    server.launchSeAddressFold.mockReset().mockResolvedValue({ runId: "run-9", url: null });
  });

  it("loads the detail, and opens on an empty one when the company has no rows", async () => {
    const response = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/address`),
      params: { companyId: COMPANY },
    } as never);
    expect(response).toEqual({ detail, selectedKey: null });

    // No 404: Add address must stay reachable for a company nothing has
    // suggested an address for. The company layout 404s an unknown company.
    server.loadSeAddressDetail.mockResolvedValueOnce(null);
    const missing = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/address`),
      params: { companyId: COMPANY },
    } as never);
    expect(missing).toEqual({ detail: EMPTY_DETAIL, selectedKey: null });
    const html = render(
      <SeAddressWorkspace
        companyId={COMPANY}
        detail={missing.detail}
        selectedKey={missing.selectedKey}
        result={null}
      />,
    );
    expect(html).toContain("No addresses published yet");
    expect(html).toContain("Add address");
  });

  it("passes the selected key from ?address=, ignoring anything malformed", async () => {
    const selected = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/address?address=${KEY}`),
      params: { companyId: COMPANY },
    } as never);
    expect(selected).toEqual({ detail, selectedKey: KEY });

    const malformed = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/address?address=not-a-key`),
      params: { companyId: COMPANY },
    } as never);
    expect(malformed).toEqual({ detail, selectedKey: null });
  });

  function post(entries: Record<string, string>) {
    const body = new FormData();
    for (const [key, value] of Object.entries(entries)) body.set(key, value);
    return action({
      request: new Request(`http://x/admin/se/company/${COMPANY}/address`, {
        method: "POST",
        body,
      }),
      params: { companyId: COMPANY },
    } as never);
  }

  it("refuses to act on a malformed company id before any write or launch", async () => {
    const body = new FormData();
    body.set("intent", "reset");
    body.set("address_key", KEY);
    const result = await action({
      request: new Request("http://x/address", { method: "POST", body }),
      params: { companyId: "abc" },
    } as never);
    expect(result).toEqual({ ok: false, intent: "", error: "Company id must be 10 or 12 digits." });
    expect(server.resetSeAddress).not.toHaveBeenCalled();
    expect(server.launchSeAddressFold).not.toHaveBeenCalled();
  });

  it("refuses a malformed post before it reaches the store, carrying the posted intent", async () => {
    expect(await post({ intent: "remove", address_key: "not-a-key" })).toEqual({
      ok: false,
      intent: "remove",
      error: "Unknown address.",
    });
    expect(server.removeSeAddress).not.toHaveBeenCalled();

    expect(await post({ intent: "activate", slot: "bogus" })).toEqual({
      ok: false,
      intent: "activate",
      error: "Unknown draft.",
    });
    expect(server.activateSeAddressDraft).not.toHaveBeenCalled();
  });

  it("dispatches remove and reset to their own store writes", async () => {
    expect(await post({ intent: "remove", address_key: KEY, note: "gone" })).toEqual({
      ok: true,
      intent: "remove",
    });
    expect(server.removeSeAddress).toHaveBeenCalledWith(COMPANY, {
      intent: "remove",
      addressKey: KEY,
      note: "gone",
    });

    expect(await post({ intent: "reset", address_key: KEY, note: "" })).toEqual({
      ok: true,
      intent: "reset",
    });
    expect(server.resetSeAddress).toHaveBeenCalledWith(COMPANY, {
      intent: "reset",
      addressKey: KEY,
      note: "",
    });
  });

  it("dispatches save-draft, carrying the written slot back", async () => {
    expect(
      await post({
        intent: "save-draft",
        care_of: "",
        street_line: "Storgatan 5",
        postal_code: "111 22",
        city: "Stockholm",
        country: "SE",
        kind: "postal",
        note: "",
        slot: "",
        replaces_key: "",
      }),
    ).toEqual({ ok: true, intent: "save-draft", slot: SLOT });
    expect(server.saveSeAddressDraft).toHaveBeenCalledWith(COMPANY, {
      intent: "save-draft",
      slot: null,
      replacesKey: null,
      input: {
        careOf: "",
        streetLine: "Storgatan 5",
        postalCode: "11122",
        city: "Stockholm",
        country: "SE",
        kind: "postal",
        note: "",
      },
    });
  });

  it("dispatches activate and discard to their own store writes", async () => {
    expect(await post({ intent: "activate", slot: SLOT, note: "looks right" })).toEqual({
      ok: true,
      intent: "activate",
    });
    expect(server.activateSeAddressDraft).toHaveBeenCalledWith(COMPANY, {
      intent: "activate",
      slot: SLOT,
      note: "looks right",
    });

    expect(await post({ intent: "discard", slot: SLOT })).toEqual({ ok: true, intent: "discard" });
    expect(server.discardSeAddressDraft).toHaveBeenCalledWith(COMPANY, {
      intent: "discard",
      slot: SLOT,
    });
  });

  it("launches a fold and reports what it started, without going through the try/catch", async () => {
    expect(await post({ intent: "fold-now" })).toEqual({
      ok: true,
      intent: "fold-now",
      runId: "run-9",
      url: null,
    });
    expect(server.launchSeAddressFold).toHaveBeenCalledWith(COMPANY);
  });

  it("turns a SeAddressDecisionError into a refusal carrying the intent, and rethrows anything else", async () => {
    server.removeSeAddress.mockRejectedValueOnce(new server.SeAddressDecisionError("Already hidden."));
    expect(await post({ intent: "remove", address_key: KEY, note: "" })).toEqual({
      ok: false,
      intent: "remove",
      error: "Already hidden.",
    });

    server.resetSeAddress.mockRejectedValueOnce(new Error("clickhouse down"));
    await expect(post({ intent: "reset", address_key: KEY, note: "" })).rejects.toThrow(
      "clickhouse down",
    );
    expect(server.resetSeAddress).toHaveBeenLastCalledWith(COMPANY, {
      intent: "reset",
      addressKey: KEY,
      note: "",
    });
  });
});
