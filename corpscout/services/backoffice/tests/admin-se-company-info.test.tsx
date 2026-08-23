import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import {
  SeCompanyInfoNotPublished,
  SeCompanyInfoReviewWorkspace,
} from "~/components/admin/se-company-info-review-workspace";
import type { SeCompanyInfoDetail } from "~/lib/se-company-info.server";

const EVIDENCE_HASH = "e".repeat(64);
const SUGGESTION_ID = "11111111-1111-4111-8111-111111111111";
const OVERRIDE_CORRECTION_ID = "22222222-2222-4222-8222-222222222222";

// Rebuilt against Task 9's shipped types (is_newest/is_published on
// suggestions, every SeCompanyInfoRow column) -- the task-10 brief's fixture
// predates that shape.
const detail: SeCompanyInfoDetail = {
  info: {
    company_id: "5565200028",
    legal_name: "Alpha AB",
    legal_form_code: "AB",
    status: "active",
    incorporation_date: "2001-02-03",
    description: "Alpha builds payment software.",
    description_language: "en",
    description_source: "llm",
    description_sources: ["llm", "scb"],
    description_source_record_uids: ["scb:1"],
    description_source_count: 1,
    primary_nace_code: "62.01",
    primary_sni_code: "62010",
    wikidata_id: "Q1",
    lei: null,
    source_record_uids: ["scb:1", "wikidata:Q1"],
    evidence_hashes: ["a".repeat(64), "c".repeat(64)],
    evidence_set_hash: EVIDENCE_HASH,
    correction_ids: [],
    suggestion_id: SUGGESTION_ID,
    model_provider: "deepseek",
    model_name: "m",
    prompt_version: "v",
    source_run_id: "run-1",
    resolved_at: "2026-08-22 09:00:00.000",
  },
  artifacts: [
    {
      source: "scb",
      source_record_uid: "scb:1",
      observed_at: "2026-08-01 00:00:00.000",
      evidence_hash: "a".repeat(64),
      summary: "IT-konsulter.",
    },
    {
      source: "wikidata",
      source_record_uid: "wikidata:Q1",
      observed_at: "2026-08-01 00:00:00.000",
      evidence_hash: "c".repeat(64),
      summary: "Swedish fintech company",
    },
  ],
  suggestions: [
    {
      suggestion_id: SUGGESTION_ID,
      input_hash: "h".repeat(64),
      suggestion:
        '{"description":"Alpha builds payment software.","language":"en"}',
      model_provider: "deepseek",
      model_name: "m",
      prompt_version: "v",
      created_at: "2026-08-22 08:59:00.000",
      is_published: 1,
      is_newest: 1,
    },
  ],
  corrections: [],
};

/** The same company with a live (current, non-stale) override on the ledger. */
const overriddenDetail: SeCompanyInfoDetail = {
  ...detail,
  corrections: [
    {
      correction_id: OVERRIDE_CORRECTION_ID,
      correction_kind: "override_field",
      payload: '{"description":"Reviewer-written summary."}',
      evidence_hash: EVIDENCE_HASH,
      reason: "SCB copy was templated boilerplate",
      decided_by: "backoffice",
      supersedes_correction_id: null,
      created_at: "2026-08-22 11:00:00.000",
      is_current: 1,
      is_stale: 0,
      is_applied: 1,
    },
  ],
};

function render(
  workspaceDetail: SeCompanyInfoDetail = detail,
  result: Parameters<typeof SeCompanyInfoReviewWorkspace>[0]["result"] = null,
) {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <SeCompanyInfoReviewWorkspace
            detail={workspaceDetail}
            result={result}
          />
        ),
        action: () => null,
      },
    ],
    { initialEntries: ["/admin/se/company/5565200028/info"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

/** The innermost `<form>` body that contains `needle`, so per-form assertions stay scoped. */
function formContaining(html: string, needle: string): string {
  for (const part of html.split("<form")) {
    const end = part.indexOf("</form>");
    const body = end === -1 ? part : part.slice(0, end);
    if (body.includes(needle)) return body;
  }
  throw new Error(`no <form> containing ${needle}`);
}

describe("company info review page", () => {
  it("shows the merged row, its sources, the published suggestion and every form with the evidence hash", () => {
    const html = render();
    expect(html).toContain("Alpha AB");
    expect(html).toContain("IT-konsulter.");
    expect(html).toContain("Swedish fintech company");
    expect(html).toContain("published");
    for (const kind of [
      "override_field",
      "approve_suggestion",
      "reject_suggestion",
    ]) {
      expect(html).toContain(`value="${kind}"`);
    }
    expect(html).toContain(`name="evidence_hash" value="${EVIDENCE_HASH}"`);
    expect(html).toContain('name="original_description"');
  });

  it("shows suggestion language and rationale parsed from the suggestion body", () => {
    const withRationale: SeCompanyInfoDetail = {
      ...detail,
      suggestions: [
        {
          ...detail.suggestions[0],
          suggestion:
            '{"description":"Alpha builds payment software.","language":"en","rationale":"Matches the SCB filing."}',
        },
      ],
    };
    const html = render(withRationale);
    expect(html).toContain("Language: en");
    expect(html).toContain("Rationale: Matches the SCB filing.");
  });

  it("confirms a save and renders the not-published state", () => {
    const html = render(detail, {
      ok: true,
      correctionId: "22222222-2222-4222-8222-222222222222",
    });
    expect(html).toContain("Queued for the next Dagster review run");
    expect(
      renderToStaticMarkup(<SeCompanyInfoNotPublished companyId="5565200028" />),
    ).toContain("not published");
  });

  it("renders a validation error", () => {
    const html = render(detail, {
      ok: false,
      error: "The evidence changed while you were reviewing.",
    });
    expect(html).toContain("The evidence changed while you were reviewing.");
  });

  it("only offers approve/reject on the newest suggestion", () => {
    const supersededDetail: SeCompanyInfoDetail = {
      ...detail,
      suggestions: [{ ...detail.suggestions[0], is_newest: 0 }],
    };
    const html = render(supersededDetail);
    expect(html).toContain("superseded evidence");
    expect(html).not.toContain('value="approve_suggestion"');
    expect(html).not.toContain('value="reject_suggestion"');
    expect(html).toContain("Alpha builds payment software.");
  });

  it("shows an undo form on the current non-undo correction", () => {
    const undoForm = formContaining(
      render(overriddenDetail),
      'name="correction_kind" value="undo"',
    );
    expect(undoForm).toContain(
      `name="supersedes_correction_id" value="${OVERRIDE_CORRECTION_ID}"`,
    );
    expect(undoForm).not.toContain("evidence_hash");
  });

  it("P7: disables approve/reject and explains why while a live override stands", () => {
    const html = render(overriddenDetail);
    const approveForm = formContaining(
      html,
      'name="correction_kind" value="approve_suggestion"',
    );
    const rejectForm = formContaining(
      html,
      'name="correction_kind" value="reject_suggestion"',
    );
    expect(approveForm).toContain("disabled=\"\"");
    expect(rejectForm).toContain("disabled=\"\"");
    expect(html).toContain(
      `Undo the current override first (${OVERRIDE_CORRECTION_ID}).`,
    );
  });
});
