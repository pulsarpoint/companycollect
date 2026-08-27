import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { SePersonReviewWorkspace } from "~/components/admin/se-person-review-workspace";
import type { CompanyPersonRoleType } from "~/lib/company-roles.server";
import type {
  SeCollisionCandidateGroup,
  SeCompanyPersonDetail,
} from "~/lib/se-company-person.server";
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";
import {
  buildCorrectionInput,
  payloadFor,
} from "~/lib/se-person-review-form";

const EVIDENCE_HASH = "a".repeat(64);
const SUGGESTION_ID = "77777777-7777-4777-8777-777777777777";
const CORRECTION_ID = "88888888-8888-4888-8888-888888888888";
const PERSON_ID = "43234b7d-0184-16b5-de47-dc086a2b0ed9";

const detail = {
  person: {
    person_id: PERSON_ID,
    company_id: "5565200028",
    name: "David Mindus",
    description: null,
    draft_ids: ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
    draft_set_hash: EVIDENCE_HASH,
    correction_ids: [],
    suggestion_id: null,
    merged_into_person_id: null,
    model_provider: "deterministic",
    model_name: "single-source:bolagsverket",
    prompt_version: "single-source-copy-v2",
    updated_at: "2026-08-22 09:00:00.000",
  },
  drafts: [
    {
      draft_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      source: "bolagsverket",
      name: "David Mindus",
      role_original: "Verkställande direktör",
      fiscal_year: 2024,
      source_observed_at: "2026-08-01 00:00:00.000",
      source_value_json: "{}",
    },
  ],
  roles: [],
  suggestions: [],
  corrections: [],
};

/** The same person once a model has suggested a profile and one correction landed. */
const reviewedDetail: SeCompanyPersonDetail = {
  ...detail,
  suggestions: [
    {
      suggestion_id: SUGGESTION_ID,
      input_hash: "b".repeat(64),
      draft_ids: detail.person.draft_ids,
      suggestion: '{"displayName":"David Mindus"}',
      model_provider: "deepseek",
      model_name: "deepseek-flash-v4",
      prompt_version: "person-profile-v3",
      created_at: "2026-08-22 10:00:00.000",
      is_published: 0,
      is_current: 1,
    },
  ],
  corrections: [
    {
      correction_id: CORRECTION_ID,
      correction_kind: "override_field",
      subject_person_id: PERSON_ID,
      target_person_id: null,
      draft_ids: [],
      payload: '{"name":"David Mindus"}',
      evidence_hash: EVIDENCE_HASH,
      reason: "Spelling",
      decided_by: "backoffice",
      supersedes_correction_id: null,
      created_at: "2026-08-22 11:00:00.000",
      is_current: 1,
      is_stale: 0,
      is_applied: 1,
    },
  ],
};

function renderWorkspace(
  workspaceDetail: Parameters<typeof SePersonReviewWorkspace>[0]["detail"],
  result: Parameters<typeof SePersonReviewWorkspace>[0]["result"] = null,
  collisionGroups: SeCollisionCandidateGroup[] = [],
) {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <SePersonReviewWorkspace
            detail={workspaceDetail}
            activeRoleCodes={["board_member", "chief_executive_officer"]}
            collisionGroups={collisionGroups}
            result={result}
          />
        ),
        action: () => null,
      },
    ],
    { initialEntries: [`/admin/se/people/person/5565200028/${PERSON_ID}`] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

function render(
  result: Parameters<typeof SePersonReviewWorkspace>[0]["result"] = null,
) {
  return renderWorkspace(detail, result);
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

describe("Sweden company-person review page", () => {
  it("shows evidence, provenance and every correction form with the evidence hash", () => {
    const html = render();

    expect(html).toContain("David Mindus");
    expect(html).toContain("Verkställande direktör");
    expect(html).toContain("deterministic");
    for (const kind of ["override_field", "merge_persons", "reassign_draft", "split_person", "set_role", "remove_role"]) {
      expect(html).toContain(`value="${kind}"`);
    }
    expect(html).toContain(`name="evidence_hash" value="${"a".repeat(64)}"`);
    expect(html).toContain("chief_executive_officer");
  });

  it("confirms a saved correction and says Dagster will re-run the company", () => {
    const html = render({ ok: true, correctionId: "55555555-5555-4555-8555-555555555555" });

    expect(html).toContain("Saved");
    expect(html).toContain("re-run company 5565200028");
  });

  it("shows a validation error", () => {
    const html = render({ ok: false, error: "The evidence changed while you were reviewing." });

    expect(html).toContain("The evidence changed while you were reviewing.");
  });

  it("offers the source payload and a role placeholder that cannot be submitted", () => {
    const html = render();

    expect(html).toContain("<summary");
    expect(html).toContain("payload");
    expect(html).toContain('<option value="">Select a role…</option>');
  });

  it("posts the reviewed values with the override so untouched fields stay out", () => {
    const overrideForm = formContaining(
      render(),
      'name="correction_kind" value="override_field"',
    );

    expect(overrideForm).toContain('name="original_name" value="David Mindus"');
    expect(overrideForm).toContain('name="original_description" value=""');
    expect(overrideForm).toContain('name="clear_description"');
    expect(overrideForm).toContain("Clear the description");
  });

  it("carries the suggestion id on both approve and reject", () => {
    const html = renderWorkspace(reviewedDetail);

    for (const kind of ["approve_suggestion", "reject_suggestion"]) {
      const form = formContaining(html, `name="correction_kind" value="${kind}"`);
      expect(form).toContain(`name="suggestion_id" value="${SUGGESTION_ID}"`);
      expect(form).toContain(`name="evidence_hash" value="${EVIDENCE_HASH}"`);
    }
  });

  it("offers no decision on a suggestion whose evidence is superseded", () => {
    const supersededDetail: SeCompanyPersonDetail = {
      ...reviewedDetail,
      suggestions: [{ ...reviewedDetail.suggestions[0], is_current: 0 }],
    };

    const html = renderWorkspace(supersededDetail);

    expect(html).toContain("superseded evidence");
    expect(html).not.toContain('value="approve_suggestion"');
    expect(html).not.toContain('value="reject_suggestion"');
    // The suggestion itself stays readable; only the decision is withdrawn.
    expect(html).toContain("David Mindus");
  });

  it("undoes a ledger row by superseding it rather than by evidence hash", () => {
    const undoForm = formContaining(
      renderWorkspace(reviewedDetail),
      'name="correction_kind" value="undo"',
    );

    expect(undoForm).toContain(
      `name="supersedes_correction_id" value="${CORRECTION_ID}"`,
    );
    expect(undoForm).not.toContain("evidence_hash");
  });

  it("shows no open collision candidates when the company has none", () => {
    const html = render();
    expect(html).toContain("No open collision candidates for this company.");
  });

  it("offers approve/keep-separate on an undecided group with a merge suggestion", () => {
    const groups: SeCollisionCandidateGroup[] = [
      {
        candidate_group_id: "grp-1",
        members: [
          { person_key: "anna svensson", full_name: "Anna Svensson", source: "bolagsverket", source_record_uid: "u1" },
          { person_key: "anna maria svensson", full_name: "Anna Maria Svensson", source: "esef", source_record_uid: "u2" },
        ],
        suggestion: {
          suggestion_id: "99999999-9999-4999-8999-999999999999",
          decision: "merge",
          confidence: 0.9,
          rationale: "Same person, middle name variant.",
          into_person_id: PERSON_ID,
          from_person_ids: ["11111111-1111-4111-8111-111111111111"],
          member_person_ids: [PERSON_ID, "11111111-1111-4111-8111-111111111111"],
          created_at: "2026-08-27 09:00:00.000",
        },
        is_decided: false,
      },
    ];
    const html = renderWorkspace(detail, null, groups);

    expect(html).toContain("grp-1");
    expect(html).toContain("Anna Svensson");
    expect(html).toContain("Anna Maria Svensson");
    expect(html).toContain("Same person, middle name variant.");

    const approveForm = formContaining(
      html,
      'name="correction_kind" value="approve_merge_suggestion"',
    );
    expect(approveForm).toContain(
      'name="suggestion_id" value="99999999-9999-4999-8999-999999999999"',
    );
    const keepSeparateForm = formContaining(
      html,
      'name="correction_kind" value="keep_separate_suggestion"',
    );
    expect(keepSeparateForm).toContain(
      'name="suggestion_id" value="99999999-9999-4999-8999-999999999999"',
    );
  });

  it("offers no decision on an already-decided group", () => {
    const groups: SeCollisionCandidateGroup[] = [
      {
        candidate_group_id: "grp-decided",
        members: [
          { person_key: "anna svensson", full_name: "Anna Svensson", source: "bolagsverket", source_record_uid: "u1" },
        ],
        suggestion: {
          suggestion_id: "99999999-9999-4999-8999-999999999999",
          decision: "keep_separate",
          confidence: 0.4,
          rationale: "",
          into_person_id: PERSON_ID,
          from_person_ids: ["11111111-1111-4111-8111-111111111111"],
          member_person_ids: [PERSON_ID, "11111111-1111-4111-8111-111111111111"],
          created_at: "2026-08-27 09:00:00.000",
        },
        is_decided: true,
      },
    ];
    const html = renderWorkspace(detail, null, groups);

    expect(html).toContain("grp-decided");
    expect(html).toContain("decided");
    expect(html).not.toContain("approve_merge_suggestion");
    expect(html).not.toContain("keep_separate_suggestion");
  });

  it("shows a group with no suggestion yet, without decision buttons", () => {
    const groups: SeCollisionCandidateGroup[] = [
      {
        candidate_group_id: "grp-pending",
        members: [
          { person_key: "anna svensson", full_name: "Anna Svensson", source: "bolagsverket", source_record_uid: "u1" },
        ],
        suggestion: null,
        is_decided: false,
      },
    ];
    const html = renderWorkspace(detail, null, groups);

    expect(html).toContain("grp-pending");
    expect(html).toContain("No merge suggestion yet.");
    expect(html).not.toContain("approve_merge_suggestion");
    // Item 4: a link to launch a scoped merge run without copy-pasting the
    // company id -- the pipeline page's loader reads it back via `?company_id=`.
    expect(html).toContain(
      `href="/admin/se/people/pipeline?company_id=${detail.person.company_id}"`,
    );
  });
});

const roleTypes: CompanyPersonRoleType[] = [
  {
    role_code: "chief_executive_officer",
    display_name: "Chief executive officer",
    role_group: "executive",
    description: "",
    is_active: 1,
    created_at: "2026-01-01 00:00:00.000",
    updated_at: "2026-01-01 00:00:00.000",
  },
  {
    role_code: "retired_role",
    display_name: "Retired role",
    role_group: "other",
    description: "",
    is_active: 0,
    created_at: "2026-01-01 00:00:00.000",
    updated_at: "2026-01-01 00:00:00.000",
  },
];

const params = { companyId: "5565200028", personId: PERSON_ID };

function formData(entries: Record<string, string | string[]>): FormData {
  const form = new FormData();
  for (const [key, value] of Object.entries(entries)) {
    for (const item of Array.isArray(value) ? value : [value]) {
      form.append(key, item);
    }
  }
  return form;
}

function inputFor(entries: Record<string, string | string[]>) {
  const built = buildCorrectionInput(formData(entries), params, roleTypes);
  if (!built.ok) throw new Error(`expected an input, got: ${built.error}`);
  return built.input;
}

describe("Sweden company-person correction input", () => {
  const overrideBase = {
    correction_kind: "override_field",
    evidence_hash: EVIDENCE_HASH,
    reason: "Register spelling",
    original_name: "David Mindus",
    original_description: "",
  };

  it.each([
    [
      "override sends only the changed name",
      { ...overrideBase, name: "David Mindus Jr", description: "" },
      { name: "David Mindus Jr" },
    ],
    [
      "override ignores whitespace-only edits",
      { ...overrideBase, name: "  David Mindus  ", description: "Swedish executive." },
      { description: "Swedish executive." },
    ],
    [
      "override clears the description on request",
      {
        ...overrideBase,
        name: "David Mindus",
        description: "",
        clear_description: "yes",
      },
      { description: null },
    ],
    [
      "reject keeps a note",
      {
        correction_kind: "reject_suggestion",
        evidence_hash: EVIDENCE_HASH,
        reason: "Hallucinated employer",
        suggestion_id: SUGGESTION_ID,
        note: "  Invented a board seat.  ",
      },
      { suggestion_id: SUGGESTION_ID, note: "Invented a board seat." },
    ],
    [
      "approve drops a stray note",
      {
        correction_kind: "approve_suggestion",
        evidence_hash: EVIDENCE_HASH,
        reason: "Matches the annual report",
        suggestion_id: SUGGESTION_ID,
        note: "ignored",
      },
      { suggestion_id: SUGGESTION_ID },
    ],
    [
      "set_role keeps a fiscal year",
      {
        correction_kind: "set_role",
        evidence_hash: EVIDENCE_HASH,
        reason: "CEO in 2024",
        role_code: "chief_executive_officer",
        fiscal_year: "2024",
        draft_id: detail.person.draft_ids,
      },
      { role_code: "chief_executive_officer", fiscal_year: 2024 },
    ],
    [
      "set_role omits a blank fiscal year",
      {
        correction_kind: "set_role",
        evidence_hash: EVIDENCE_HASH,
        reason: "CEO, undated",
        role_code: "chief_executive_officer",
        fiscal_year: "  ",
        draft_id: detail.person.draft_ids,
      },
      { role_code: "chief_executive_officer" },
    ],
  ])("%s", (_label, entries, expectedPayload) => {
    expect(inputFor(entries).payload).toEqual(expectedPayload);
  });

  it("refuses an override where nothing changed", () => {
    const built = buildCorrectionInput(
      formData({ ...overrideBase, name: "David Mindus", description: "" }),
      params,
      roleTypes,
    );

    expect(built).toEqual({ ok: false, error: "Nothing changed." });
  });

  it("sends undo with the superseded id and the zero evidence hash", () => {
    const input = inputFor({
      correction_kind: "undo",
      reason: "Wrong person",
      supersedes_correction_id: CORRECTION_ID,
    });

    expect(input.supersedesCorrectionId).toBe(CORRECTION_ID);
    expect(input.evidenceHash).toBe(ZERO_EVIDENCE_HASH);
  });

  it("never supersedes from a kind other than undo", () => {
    const input = inputFor({
      correction_kind: "merge_persons",
      evidence_hash: EVIDENCE_HASH,
      reason: "Same person",
      target_person_id: "99999999-9999-4999-8999-999999999999",
      supersedes_correction_id: CORRECTION_ID,
    });

    expect(input.supersedesCorrectionId).toBeNull();
    expect(input.evidenceHash).toBe(EVIDENCE_HASH);
    expect(input.targetPersonId).toBe("99999999-9999-4999-8999-999999999999");
  });

  it("offers only the active canonical roles to the validator", () => {
    const input = inputFor({
      correction_kind: "remove_role",
      evidence_hash: EVIDENCE_HASH,
      reason: "Never held it",
      draft_id: detail.person.draft_ids,
    });

    expect([...input.activeRoleCodes]).toEqual(["chief_executive_officer"]);
    expect(input.draftIds).toEqual(detail.person.draft_ids);
    expect(payloadFor(formData({}), "remove_role")).toEqual({});
  });
});
