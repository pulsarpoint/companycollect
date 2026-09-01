import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { SeCompanyInfoCorrectionsFilterFields } from "~/components/admin/se-company-info-filter-sheet";
import type { SeCompanyInfoCorrectionFilterOptions } from "~/lib/se-company-info-lists.server";
import {
  correctionFilterChips,
  EMPTY_CORRECTION_FILTERS,
  parseCorrectionFilters,
  type SeCompanyInfoCorrectionsTableFilters,
} from "~/lib/se-company-info-filters";

/**
 * The correction-ledger filter vocabulary that outlived the info corrections
 * page: the fields component (rendered by SeCompanyInfoCorrectionsFilterSheet,
 * which the ADDRESS ledger's table opens) and the URL parser both default to
 * the info enums. The address ledger's own page test covers the enums it passes
 * explicitly; these pin the shared pieces in their own right.
 */

const OPTIONS: SeCompanyInfoCorrectionFilterOptions = {
  decidedBy: ["backoffice", "dagster"],
};

const APPLIED_FILTERS: SeCompanyInfoCorrectionsTableFilters = {
  companyId: "5565200028",
  kind: "undo",
  status: "applied",
  decidedBy: "backoffice",
};

describe("SeCompanyInfoCorrectionsFilterFields", () => {
  it("offers a field for every filter, including one select per discrete column", () => {
    const html = renderToStaticMarkup(
      <SeCompanyInfoCorrectionsFilterFields
        filters={EMPTY_CORRECTION_FILTERS}
        options={OPTIONS}
        view={{ sort: "created_at", dir: "desc", pageSize: 100 }}
      />,
    );
    for (const name of ["companyId", "kind", "status", "decidedBy"]) {
      expect(html).toContain(`name="${name}"`);
    }
    for (const label of ["Company id", "Kind", "Status", "Decided by"]) {
      expect(html).toContain(label);
    }
    // An unset filter shows the explicit "Any" item Base UI needs.
    expect(html).toContain('name="kind" value="any"');
    expect(html).toContain('name="decidedBy" value="any"');
    // A Base UI select's trigger is a button whose only text is the current
    // value, so the visible <Label> above it names nothing to a screen reader.
    for (const label of ["Company id", "Kind", "Status", "Decided by"]) {
      expect(html).toContain(`aria-label="${label}"`);
    }
    // Not filters, but they must survive one being applied.
    expect(html).toContain('type="hidden" name="pageSize" value="100"');
    expect(html).toContain('type="hidden" name="sort" value="created_at"');
    expect(html).toContain('type="hidden" name="dir" value="desc"');
  });

  it("carries the applied filters as the form's default values", () => {
    const html = renderToStaticMarkup(
      <SeCompanyInfoCorrectionsFilterFields
        filters={APPLIED_FILTERS}
        options={OPTIONS}
        view={{ sort: "created_at", dir: "desc", pageSize: 50 }}
      />,
    );
    expect(html).toContain('name="companyId" value="5565200028"');
    expect(html).toContain('name="kind" value="undo"');
    expect(html).toContain('name="status" value="applied"');
    expect(html).toContain('name="decidedBy" value="backoffice"');
  });
});

describe("parseCorrectionFilters", () => {
  const at = (search: string) =>
    new URL(`http://localhost/admin/se/company-address/corrections${search}`);

  it("reads every ledger filter from the URL", () => {
    expect(
      parseCorrectionFilters(
        at("?companyId=5565200028&kind=undo&status=applied&decidedBy=backoffice"),
      ),
    ).toEqual(APPLIED_FILTERS);
  });

  it("drops a kind or status the query builder would ignore, so no chip claims it", () => {
    for (const search of ["?kind=bogus", "?kind=any", "?status=bogus", "?status=any"]) {
      const filters = parseCorrectionFilters(at(search));
      expect(filters).toEqual(EMPTY_CORRECTION_FILTERS);
      expect(correctionFilterChips(filters)).toEqual([]);
    }
  });

  it("passes decided_by through: its options come from the ledger, not from an enum", () => {
    expect(parseCorrectionFilters(at("?decidedBy=dagster")).decidedBy).toBe("dagster");
  });
});
