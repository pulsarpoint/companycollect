import type { ReactElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";

/**
 * `SeCompanyAddressCorrectionsTable`, rendered by the `admin-se-company-address-corrections`
 * route (the corrections queue page, untouched by the address-entity slice --
 * see ruling 5 of `2026-09-07-se-company-address-3-backoffice.md`). Extracted
 * verbatim from `tests/admin-se-company-address.test.tsx` when that file was
 * rewritten to test only the NEW `admin-se-company-address` route: this
 * component has never been reachable from that route (its own route is
 * `admin-se-company-address-corrections.tsx`), so its coverage moves here
 * rather than disappearing with the rewrite. Nothing in this file changed
 * from the original.
 */

import { SeCompanyAddressCorrectionsTable } from "~/components/admin/se-company-address-corrections-table";
import { SeCompanyInfoCorrectionsFilterSheet } from "~/components/admin/se-company-info-filter-sheet";
import { EMPTY_CORRECTION_FILTERS } from "~/lib/se-company-info-filters";
import type { SeCompanyAddressCorrectionListRow } from "~/lib/se-company-address-lists.server";

const COMPANY_ID = "5560125220";
const KEY = "f".repeat(64);
const OVERRIDE_CORRECTION_ID = "22222222-2222-4222-8222-222222222222";
const REJECT_CORRECTION_ID = "11111111-1111-4111-8111-111111111111";

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
