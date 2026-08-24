import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

// Hoisted mock of the ClickHouse-backed server module the route imports --
// see tests/people.corrections.test.ts / tests/se-company-info.server.test.ts
// for the same vi.hoisted + vi.mock("...server", ...) idiom, applied one
// layer up here so `action`'s calls to loadSeCompanyInfoDetail /
// appendSeCompanyInfoCorrection are directly assertable without a live
// ClickHouse.
const server = vi.hoisted(() => ({
  loadSeCompanyInfoDetail: vi.fn(),
  appendSeCompanyInfoCorrection: vi.fn(),
}));
vi.mock("~/lib/se-company-info.server", () => server);

import { action } from "~/routes/admin-se-company-info";
import {
  SeCompanyInfoNotPublished,
  SeCompanyInfoReviewWorkspace,
} from "~/components/admin/se-company-info-review-workspace";
import type { SeCompanyInfoDetail } from "~/lib/se-company-info.server";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

const COMPANY_ID = "5565200028";
const EVIDENCE_HASH = "e".repeat(64);
const SUGGESTION_ID = "11111111-1111-4111-8111-111111111111";
const OVERRIDE_CORRECTION_ID = "22222222-2222-4222-8222-222222222222";

// Rebuilt against Task 9's shipped types (is_newest/is_published on
// suggestions, every SeCompanyInfoRow column) -- the task-10 brief's fixture
// predates that shape.
const detail: SeCompanyInfoDetail = {
  info: {
    company_id: COMPANY_ID,
    legal_name: "Alpha AB",
    legal_form_code: "AB-ORGFO",
    legal_form_label_en: "Limited company (aktiebolag)",
    legal_form_label_sv: "Aktiebolag",
    status: "active",
    incorporation_date: "2001-02-03",
    description: "Alpha builds payment software.",
    description_sv: "Alpha bygger betalprogramvara.",
    description_language: "en",
    // Task 17: one boolean instead of a source label. The text is the model's,
    // merged from the two sources listed below.
    llm_enhanced: 1,
    description_sources: ["wikidata", "scb"],
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
  // Task 16: each artifact carries its FULL payload (every column of its table),
  // and the page renders every one of them under a label.
  artifacts: [
    {
      source: "scb",
      source_record_uid: "scb:1",
      observed_at: "2026-08-01 00:00:00.000",
      evidence_hash: "a".repeat(64),
      payload: {
        legal_name: "Alpha AB",
        legal_name_raw: "ALPHA AB",
        legal_form_code: "AB",
        status: "active",
        incorporation_date: "2001-02-03",
        dissolution_date: "",
        activity_description: "IT-konsulter.",
        activity_description_en: "IT consultancy.",
        primary_sni_code: "62010",
        primary_nace_code: "62.01",
      },
    },
    {
      source: "wikidata",
      source_record_uid: "wikidata:Q1",
      observed_at: "2026-08-01 00:00:00.000",
      evidence_hash: "c".repeat(64),
      payload: {
        wikidata_id: "Q1",
        wikidata_url: "https://www.wikidata.org/wiki/Q1",
        name: "Alpha",
        official_name: "Alpha AB",
        company_description: "Swedish fintech company",
        inception_date: "2001-02-03",
        legal_form_label: "aktiebolag",
        industry_wikidata_id: "Q837171",
        industry_label: "financial technology",
        headquarters_label: "Stockholm",
        employee_count: "120",
        // A column this app does not know about yet (a future migration's).
        future_column: "kept anyway",
      },
    },
    {
      source: "esef",
      source_record_uid: "esef:doc-1",
      observed_at: "2026-08-02 00:00:00.000",
      evidence_hash: "d".repeat(64),
      payload: {
        source_document_id: "doc-1",
        lei: "549300ALPHA0000000AB",
        entity_name: "Alpha AB",
        fiscal_year: "2025",
        company_description: "Alpha provides payment infrastructure.",
        description_language: "en",
        description_confidence: "0.92",
        // The shape the ESEF disclosure extractor actually writes: objects
        // with a name plus its confidence/evidence.
        products_and_services_json:
          '[{"name":"Payment terminals","confidence":0.9,"evidence_ids":["E0010"]},{"name":"Card issuing"}]',
        business_segments_json: "[]",
      },
    },
  ],
  suggestions: [
    {
      suggestion_id: SUGGESTION_ID,
      input_hash: "h".repeat(64),
      suggestion:
        '{"description":"Alpha builds payment software.","description_sv":"Alpha AB bygger betalprogramvara i Sverige.","language":"en"}',
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
    // Scoped per-form, not page-wide: every non-undo form must carry this
    // exact evidence hash, not just the page somewhere.
    for (const needle of [
      'name="correction_kind" value="override_field"',
      'name="correction_kind" value="approve_suggestion"',
      'name="correction_kind" value="reject_suggestion"',
    ]) {
      expect(formContaining(html, needle)).toContain(
        `name="evidence_hash" value="${EVIDENCE_HASH}"`,
      );
    }
    expect(html).toContain('name="original_description"');
  });

  // Task 14: the published row holds both languages (migration 000301), so the page
  // shows both and the override form edits both.
  it("shows both published languages and gives the override form a field for each", () => {
    const html = render();
    expect(html).toContain("Alpha builds payment software.");
    expect(html).toContain("Alpha bygger betalprogramvara.");
    const overrideForm = formContaining(
      html,
      'name="correction_kind" value="override_field"',
    );
    for (const field of [
      'name="original_description"',
      'name="description"',
      'name="clear_description"',
      'name="original_description_sv"',
      'name="description_sv"',
      'name="clear_description_sv"',
    ]) {
      expect(overrideForm).toContain(field);
    }
    // The Swedish original the reviewer is diffed against, not the English one.
    expect(overrideForm).toContain(
      'name="original_description_sv" value="Alpha bygger betalprogramvara."',
    );
  });

  it("says so when a company has no Swedish text at all", () => {
    const html = render({
      ...detail,
      info: { ...detail.info, description_sv: null },
    });
    expect(html).toContain("No Swedish description.");
    // The empty override field still posts the original it was diffed against.
    expect(html).toContain('name="original_description_sv" value=""');
  });

  it("shows the suggestion's Swedish half beside its English one, each labelled", () => {
    const html = render();
    expect(html).toContain("English: Alpha builds payment software.");
    expect(html).toContain("Swedish: Alpha AB bygger betalprogramvara i Sverige.");
  });

  // Task 13 round 1: the artifact stamp means "when the pipeline recorded this
  // version", not a register date -- the label has to say so.
  it("explains what an artifact's observed stamp means", () => {
    const html = render();
    expect(html).toContain('title="when the pipeline recorded this version"');
  });

  it("badges the LLM flag, the sources and the reviewed state on the published card", () => {
    const html = render();
    // Task 17: no description_source label anywhere -- the published card says
    // whether the model wrote the text and which sources fed it (nor the
    // leftover "ClickHouse").
    expect(html).not.toContain("ClickHouse");
    expect(html).toContain("LLM yes");
    expect(html).toContain("Sources: wikidata, scb");
    // Task 18: the "reviewed" badge came off the page header and onto the row
    // it is a fact about.
    const reviewed = render({
      ...detail,
      info: { ...detail.info, correction_ids: [OVERRIDE_CORRECTION_ID] },
    });
    expect(reviewed).toContain(">reviewed<");
    expect(html).not.toContain(">reviewed<");
  });

  it("says LLM no for a description that was copied from one input", () => {
    // The deterministic pick, a reviewer override and a rejected suggestion all
    // land here: the published text is not the model's.
    const html = render({
      ...detail,
      info: { ...detail.info, llm_enhanced: 0, description_sources: ["scb"] },
    });
    expect(html).toContain("LLM no");
    expect(html).toContain("Sources: scb");
  });

  // Task 18: Info is one tab of the company area, and the area's layout owns
  // the company header -- the name, the status/legal-form badges and the two
  // company-level links. This page rendering them again would be a second,
  // duplicate header on every load. The layout's own copy is asserted in
  // tests/admin-se-company-area.test.tsx.
  it("carries no company header of its own: that moved to the area layout", () => {
    const html = render();
    expect(html).not.toContain("Company page");
    expect(html).not.toContain("Corrections ledger");
    expect(html).not.toContain('href="/company/se/5565200028"');
    expect(html).not.toContain(
      'href="/admin/se/company-info/corrections?companyId=5565200028"',
    );
    // The company name still appears -- inside the SCB artifact payload, which
    // is evidence, not a header.
    expect(html).not.toContain("<h1");
  });

  it("lays the page out as published version, then every source, then suggestions and corrections", () => {
    const html = render();
    for (const heading of [
      "Published version",
      "Sources",
      "SCB register",
      "Wikidata",
      "ESEF filing",
      "Model suggestions",
      "Corrections",
      "Override description",
      "Ledger",
    ]) {
      expect(html).toContain(heading);
    }
    // The published card is the active one, and says so.
    expect(html).toContain(">active<");
  });

  it("names the legal form on the active card, in both languages, code as tooltip", () => {
    // Task 19: legal_form_code mixes Bolagsverket text codes with SCB numbers,
    // so the published row carries what the code is CALLED and the card shows
    // that -- the Swedish official term with the English gloss beside it. It is
    // copied from the register: no correction form touches it.
    const html = render();
    expect(html).toContain("Legal form");
    expect(html).toContain('title="AB-ORGFO"');
    expect(html).toContain("Aktiebolag");
    expect(html).toContain(">Limited company (aktiebolag)<");
  });

  it("shows the bare code on the active card when the dictionary names nothing", () => {
    const html = render({
      ...detail,
      info: { ...detail.info, legal_form_label_sv: "", legal_form_label_en: "" },
    });
    expect(html).toContain('title="AB-ORGFO"');
    expect(html).toContain(">AB-ORGFO<");
  });

  it("shows every SCB payload column under its own label, empty ones as an em dash", () => {
    const html = render();
    for (const [label, value] of [
      ["Legal name", "Alpha AB"],
      ["Raw name", "ALPHA AB"],
      ["Registration date", "2001-02-03"],
      ["Activity description (sv)", "IT-konsulter."],
      ["Activity description (en)", "IT consultancy."],
      ["SNI", "62010"],
      ["NACE", "62.01"],
    ]) {
      expect(html).toContain(label);
      expect(html).toContain(value);
    }
    // dissolution_date is empty on this fixture.
    expect(html).toContain("Dissolution date");
    expect(html).toContain("—");
  });

  it("shows every Wikidata payload column, links the id to wikidata.org, and keeps unknown columns", () => {
    const html = render();
    expect(html).toContain('href="https://www.wikidata.org/wiki/Q1"');
    for (const value of [
      "Swedish fintech company",
      "aktiebolag",
      "financial technology",
      "Stockholm",
      "120",
    ]) {
      expect(html).toContain(value);
    }
    for (const label of ["Label", "Official name", "Headquarters", "Employees"]) {
      expect(html).toContain(label);
    }
    // A column this app has never seen still renders, labelled from its name.
    expect(html).toContain("Future column");
    expect(html).toContain("kept anyway");
  });

  it("shows the ESEF payload including its JSON blobs as lists", () => {
    const html = render();
    for (const label of [
      "Entity name",
      "LEI",
      "Fiscal year",
      "Source document",
      "Description language",
      "Description confidence",
      "Products &amp; services",
      "Business segments",
    ]) {
      expect(html).toContain(label);
    }
    expect(html).toContain("2025");
    // Each item leads with its name (as the public company page does) and
    // keeps the rest of the object beside it, in prose rather than raw JSON.
    expect(html).toContain("<li>Payment terminals");
    expect(html).toContain("confidence 0.9 · E0010");
    expect(html).not.toContain("&quot;confidence&quot;");
    expect(html).toContain("<li>Card issuing</li>");
  });

  it("badges only the sources whose record actually contributed to the published description", () => {
    const html = render();
    expect(html.match(/contributes to description/g)).toHaveLength(1);
    // scb:1 is the contributing uid on this fixture, so the badge sits inside
    // the SCB group -- between its heading and the next source's.
    const scbGroup = html.slice(
      html.indexOf("SCB register"),
      html.indexOf("Wikidata", html.indexOf("SCB register")),
    );
    expect(scbGroup).toContain("contributes to description");
  });

  it("renders a source card even when its payload is empty (every label, every value an em dash)", () => {
    const html = render({
      ...detail,
      artifacts: [
        {
          source: "scb",
          source_record_uid: "scb:1",
          observed_at: "2026-08-01 00:00:00.000",
          evidence_hash: "a".repeat(64),
          payload: {},
        },
      ],
    });
    expect(html).toContain("Legal name");
    expect(html).toContain("—");
  });

  it("says so when a company has no artifacts at all", () => {
    const html = render({ ...detail, artifacts: [] });
    expect(html).toContain("No source artifacts.");
    // ...and the strip at the top says the same thing, rather than claiming a
    // register the page then fails to show a card for.
    expect(html).toContain('data-source-strip=""');
  });

  it("opens with the same Sources strip the other four tabs do, from the artifact legs", () => {
    // Task 20: derived from `artifacts`, which the hub already loaded -- the
    // strip added no query. Named in the profile catalog's order (S before E
    // before W), not in the order ClickHouse's UNION handed the legs over.
    const html = render();
    expect(html).toContain('data-source-strip="SCB,ESEF,Wikidata"');
    // The strip is ABOVE the per-source cards it summarises.
    expect(html.indexOf("data-source-strip")).toBeLessThan(
      html.indexOf("SCB register"),
    );
  });

  it("annotates the English text with its language and every contributing source", () => {
    // Task 17: no winning-source label to leave out of the list any more --
    // the sources ARE the list, and the LLM flag is a badge of its own.
    const html = render();
    expect(html).toContain("(en · wikidata, scb)");
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

  it("guards non-string suggestion fields instead of crashing SSR", () => {
    const badShape: SeCompanyInfoDetail = {
      ...detail,
      suggestions: [
        {
          ...detail.suggestions[0],
          suggestion: '{"description":{"nested":"object"},"language":42}',
        },
      ],
    };
    expect(() => render(badShape)).not.toThrow();
    const html = render(badShape);
    expect(html).not.toContain("Language:");
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

  it("shows an undo form on the current non-undo correction, and its 8-char id in the row", () => {
    const html = render(overriddenDetail);
    const undoForm = formContaining(html, 'name="correction_kind" value="undo"');
    expect(undoForm).toContain(
      `name="supersedes_correction_id" value="${OVERRIDE_CORRECTION_ID}"`,
    );
    expect(undoForm).not.toContain("evidence_hash");
    expect(html).toContain(OVERRIDE_CORRECTION_ID.slice(0, 8));
  });

  it("P7: disables approve/reject and points at the override by its 8-char id, via liveOverrideRefusal", () => {
    const html = render(overriddenDetail);
    const approveForm = formContaining(
      html,
      'name="correction_kind" value="approve_suggestion"',
    );
    const rejectForm = formContaining(
      html,
      'name="correction_kind" value="reject_suggestion"',
    );
    expect(approveForm).toContain('disabled=""');
    expect(rejectForm).toContain('disabled=""');
    expect(html).toContain(
      `Undo the current override first (${OVERRIDE_CORRECTION_ID.slice(0, 8)}).`,
    );
  });

  it("gives the reason/note inputs an aria-label", () => {
    const html = render(overriddenDetail);
    expect(html).toContain('aria-label="Reason"');
    expect(html).toContain('aria-label="Note"');
    expect(html).toContain('aria-label="Why undo"');
  });
});

describe("admin-se-company-info action (P7 -- server-side refusal, mocked server module)", () => {
  beforeEach(() => {
    server.loadSeCompanyInfoDetail.mockReset();
    server.appendSeCompanyInfoCorrection.mockReset();
  });

  function detailWithCorrections(
    corrections: SeCompanyInfoDetail["corrections"],
  ): SeCompanyInfoDetail {
    return { ...detail, corrections };
  }

  function postAction(entries: Record<string, string>) {
    const form = new FormData();
    for (const [key, value] of Object.entries(entries)) form.append(key, value);
    const request = new Request(
      `http://localhost/admin/se/company/${COMPANY_ID}/info`,
      { method: "POST", body: form },
    );
    return action({
      request,
      params: { companyId: COMPANY_ID },
    } as unknown as Parameters<typeof action>[0]);
  }

  it("refuses approve_suggestion while a live override stands, without writing", async () => {
    server.loadSeCompanyInfoDetail.mockResolvedValue(
      detailWithCorrections([
        {
          correction_id: OVERRIDE_CORRECTION_ID,
          correction_kind: "override_field",
          payload: '{"description":"x"}',
          evidence_hash: EVIDENCE_HASH,
          reason: "r",
          decided_by: "backoffice",
          supersedes_correction_id: null,
          created_at: "2026-08-22 12:00:00.000",
          is_current: 1,
          is_stale: 0,
          is_applied: 1,
        },
      ]),
    );

    const result = await postAction({
      correction_kind: "approve_suggestion",
      suggestion_id: SUGGESTION_ID,
      evidence_hash: EVIDENCE_HASH,
      reason: "Matches SCB",
    });

    expect(result).toEqual({
      ok: false,
      error: `Undo the current override first (${OVERRIDE_CORRECTION_ID.slice(0, 8)}).`,
    });
    expect(server.appendSeCompanyInfoCorrection).not.toHaveBeenCalled();
  });

  it("calls appendSeCompanyInfoCorrection with the expected SeInfoCorrectionInput once no live override stands", async () => {
    server.loadSeCompanyInfoDetail.mockResolvedValue(detailWithCorrections([]));
    server.appendSeCompanyInfoCorrection.mockResolvedValue({
      correctionId: "66666666-6666-4666-8666-666666666666",
    });

    const result = await postAction({
      correction_kind: "approve_suggestion",
      suggestion_id: SUGGESTION_ID,
      evidence_hash: EVIDENCE_HASH,
      reason: "Matches SCB",
    });

    expect(result).toEqual({
      ok: true,
      correctionId: "66666666-6666-4666-8666-666666666666",
    });
    expect(server.loadSeCompanyInfoDetail).toHaveBeenCalledWith(COMPANY_ID);
    expect(server.appendSeCompanyInfoCorrection).toHaveBeenCalledWith({
      companyId: COMPANY_ID,
      kind: "approve_suggestion",
      payload: { suggestion_id: SUGGESTION_ID },
      evidenceHash: EVIDENCE_HASH,
      reason: "Matches SCB",
      supersedesCorrectionId: null,
    });
  });

  it("undo posts the zero evidence hash and the superseded id, skipping the live-override check entirely", async () => {
    server.appendSeCompanyInfoCorrection.mockResolvedValue({
      correctionId: "77777777-7777-4777-8777-777777777777",
    });

    const result = await postAction({
      correction_kind: "undo",
      supersedes_correction_id: OVERRIDE_CORRECTION_ID,
      reason: "Wrong call",
    });

    expect(result).toEqual({
      ok: true,
      correctionId: "77777777-7777-4777-8777-777777777777",
    });
    // Undo is neither approve_suggestion nor reject_suggestion, so the
    // action never needs the current detail to decide.
    expect(server.loadSeCompanyInfoDetail).not.toHaveBeenCalled();
    expect(server.appendSeCompanyInfoCorrection).toHaveBeenCalledWith({
      companyId: COMPANY_ID,
      kind: "undo",
      payload: {},
      evidenceHash: ZERO_EVIDENCE_HASH,
      reason: "Wrong call",
      supersedesCorrectionId: OVERRIDE_CORRECTION_ID,
    });
  });
});
