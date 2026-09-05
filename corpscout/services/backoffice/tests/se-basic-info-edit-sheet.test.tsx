import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import {
  SeBasicInfoEditForm,
  SeBasicInfoEditSheet,
} from "~/components/admin/se-basic-info-edit-sheet";
import type {
  SeBasicInfoDetail,
  SeBasicInfoLegalFormOption,
  SeBasicInfoSuggestionRow,
} from "~/lib/se-basic-info.server";

const COMPANY = "0113004022";

const legalFormOptions: SeBasicInfoLegalFormOption[] = Array.from({ length: 32 }, (_, index) => {
  const code = String(index + 1).padStart(2, "0");
  return { code, label_sv: `Bolagsform ${code}`, label_en: `Legal form ${code}` };
});

const reviewerDraft: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "reviewer_draft",
  source_record_uid: "",
  observed_at: "2026-09-05 09:00:00.000",
  suggested_at: "2026-09-05 09:00:00.000",
  legal_name: "Draft name AB",
  legal_form_code: "",
  status: "",
  incorporation_date: "",
  lei: "",
  wikidata_id: "",
  description: "Draft description text.",
  description_language: "sv",
  description_sv: "",
  decided_by: "backoffice",
  note: "",
  source_run_id: "backoffice",
  extractor_version: "backoffice-v1",
};

const detail: SeBasicInfoDetail = {
  info: {
    company_id: COMPANY,
    legal_name: "Sportstugan upa",
    legal_name_source: "scb",
    legal_form_code: "05",
    legal_form_code_source: "scb",
    status: "active",
    status_source: "scb",
    incorporation_date: "1937-05-12",
    incorporation_date_source: "scb",
    lei: "",
    lei_source: "",
    wikidata_id: "",
    wikidata_id_source: "",
    description: "Published description.",
    description_source: "bolagsverket",
    description_language: "en",
    description_sv: "",
    description_sv_source: "",
    folded_at: "2026-09-04 17:04:01.293",
    fold_version: "fold-v1",
    source_run_id: "run-f",
  },
  suggestions: [reviewerDraft],
  history: [],
  precedence: [],
  rules: [],
  legalFormLabels: {},
  legalFormOptions,
  foldPending: false,
};

function render(element: React.ReactElement): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/info", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/info`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SeBasicInfoEditForm", () => {
  it("renders a native select with every legal-form option for legal_form_code", () => {
    const html = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="legal_form_code"
        detail={detail}
        busy={false}
        onCancel={() => {}}
      />,
    );
    for (const option of legalFormOptions) {
      expect(html).toContain(`value="${option.code}"`);
      expect(html).toContain(`Bolagsform ${option.code}`);
    }
    expect((html.match(/<option/g) ?? []).length).toBe(32);
    // The published value wins the prefill: the draft has nothing for this field.
    expect(html).toContain(`value="${detail.info?.legal_form_code}"`);
  });

  it("renders active/inactive options for status", () => {
    const html = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="status"
        detail={detail}
        busy={false}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('value="active"');
    expect(html).toContain('value="inactive"');
    expect(html).toContain("Active");
    expect(html).toContain("Inactive");
  });

  it("renders a date input for incorporation_date", () => {
    const html = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="incorporation_date"
        detail={detail}
        busy={false}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('type="date"');
    expect(html).toContain(`value="${detail.info?.incorporation_date}"`);
  });

  it("renders plain inputs for legal_name, lei and wikidata_id", () => {
    for (const field of ["legal_name", "lei", "wikidata_id"] as const) {
      const html = render(
        <SeBasicInfoEditForm
          companyId={COMPANY}
          field={field}
          detail={detail}
          busy={false}
          onCancel={() => {}}
        />,
      );
      expect(html).toContain('data-slot="input"');
      expect(html).not.toContain("<textarea");
      expect(html).not.toContain("<select");
    }
  });

  it("renders a textarea and a language select for description, prefilled from the draft", () => {
    const html = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="description"
        detail={detail}
        busy={false}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('data-slot="textarea"');
    expect(html).toContain('rows="8"');
    // The draft has a description; it wins the textarea's prefill, but the
    // published value is still shown for context in the "Published now" line.
    expect(html).toContain(">Draft description text.<");
    expect(html).toContain('name="language"');
    // The draft's own description_language (sv) is the language default.
    expect(html).toContain('value="sv" selected');
    expect(html).toContain("Published now: Published description. (Bolagsverket)");
    expect(html).toContain("Draft: Draft description text.");
  });

  it("renders a textarea with no language select for description_sv", () => {
    const html = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="description_sv"
        detail={detail}
        busy={false}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('data-slot="textarea"');
    expect(html).not.toContain('name="language"');
  });

  it("falls back to the published value when the draft has none for the field, and shows (none) when neither has one", () => {
    const html = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="status"
        detail={detail}
        busy={false}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain("Published now: active (SCB)");
    expect(html).not.toContain("Draft:");
    const noInfo: SeBasicInfoDetail = { ...detail, info: null, suggestions: [] };
    const withNothing = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="lei"
        detail={noInfo}
        busy={false}
        onCancel={() => {}}
      />,
    );
    expect(withNothing).toContain("Published now: (none)");
  });

  it("posts the hidden intent, field and legal_form_codes", () => {
    const html = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="lei"
        detail={detail}
        busy={false}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('name="intent"');
    expect(html).toContain('value="edit"');
    expect(html).toContain('name="field"');
    expect(html).toContain('value="lei"');
    expect(html).toContain('name="legal_form_codes"');
    expect(html).toContain(legalFormOptions.map((option) => option.code).join(","));
    expect(html).toContain('placeholder="Note (optional)"');
    expect(html).toContain("Cancel");
    expect(html).toContain("Save draft");
  });

  it("shows a refusal as an alert in the footer", () => {
    const html = render(
      <SeBasicInfoEditForm
        companyId={COMPANY}
        field="lei"
        detail={detail}
        busy={false}
        onCancel={() => {}}
        error="LEI must be 20 letters or digits."
      />,
    );
    expect(html).toContain('role="alert"');
    expect(html).toContain("LEI must be 20 letters or digits.");
  });
});

describe("SeBasicInfoEditSheet", () => {
  it("renders nothing when field is null", () => {
    const html = render(
      <SeBasicInfoEditSheet
        companyId={COMPANY}
        field={null}
        detail={detail}
        open={false}
        onOpenChange={() => {}}
        busy={false}
      />,
    );
    expect(html).toBe("");
  });
});
