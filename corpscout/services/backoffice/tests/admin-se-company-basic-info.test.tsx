import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({
  loadSeBasicInfoDetail: vi.fn(),
  appendSeBasicInfoRule: vi.fn(),
  appendSeBasicInfoDraft: vi.fn(),
  activateSeBasicInfoDraft: vi.fn(),
  discardSeBasicInfoDraft: vi.fn(),
  launchSeBasicInfoFold: vi.fn(),
  SeBasicInfoDecisionError: class SeBasicInfoDecisionError extends Error {},
}));
vi.mock("~/lib/se-basic-info.server", () => server);

import { action, loader } from "~/routes/admin-se-company-info";
import {
  DecisionDialogBody,
  SeBasicInfoNotFolded,
  SeBasicInfoWorkspace,
} from "~/components/admin/se-basic-info-workspace";
import type { SeBasicInfoDetail, SeBasicInfoSuggestionRow } from "~/lib/se-basic-info.server";

const COMPANY = "0113004022";

const bolagsverket: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "bolagsverket",
  source_record_uid: "abc",
  observed_at: "2026-09-03 18:16:21.117",
  suggested_at: "2026-09-04 17:46:53.852",
  legal_name: "Sportstugan upa",
  legal_form_code: "51",
  status: "inactive",
  incorporation_date: "1937-05-12",
  lei: "",
  wikidata_id: "",
  description: "Förvaltar fastigheter.",
  description_language: "sv",
  description_sv: "Förvaltar fastigheter.",
  decided_by: "",
  note: "",
  source_run_id: "run-b",
  extractor_version: "bolagsverket-v2",
};
const scb: SeBasicInfoSuggestionRow = {
  ...bolagsverket,
  source: "scb",
  legal_form_code: "51",
  status: "active",
  description: "",
  description_language: "",
  description_sv: "",
  suggested_at: "2026-09-04 11:20:00.000",
};
/** An empty-valued draft, as a real `reviewer_draft` row reads before the
 * reviewer has typed anything: every value column is `''`, not carried over
 * from another source's row. */
const emptyDraft: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "reviewer_draft",
  source_record_uid: "",
  observed_at: "2026-09-05 09:00:00.000",
  suggested_at: "2026-09-05 09:00:00.000",
  legal_name: "",
  legal_form_code: "",
  status: "",
  incorporation_date: "",
  lei: "",
  wikidata_id: "",
  description: "",
  description_language: "",
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
    legal_form_code: "51",
    legal_form_code_source: "scb",
    status: "active",
    status_source: "scb",
    incorporation_date: "1937-05-12",
    incorporation_date_source: "scb",
    lei: "",
    lei_source: "",
    wikidata_id: "",
    wikidata_id_source: "",
    description: "Förvaltar fastigheter.",
    description_source: "bolagsverket",
    description_language: "sv",
    description_sv: "Förvaltar fastigheter.",
    description_sv_source: "bolagsverket",
    folded_at: "2026-09-04 17:04:01.293",
    fold_version: "fold-v1",
    source_run_id: "run-f",
  },
  suggestions: [bolagsverket, scb],
  history: [],
  precedence: [
    { company_id: "", field: "status", source: "reviewer", precedence: 10000, removed: 0, decided_by: "", note: "", decided_at: "2026-01-01 00:00:00.000" },
    { company_id: "", field: "status", source: "scb", precedence: 1000, removed: 0, decided_by: "", note: "", decided_at: "2026-01-01 00:00:00.000" },
    { company_id: "", field: "status", source: "bolagsverket", precedence: 900, removed: 0, decided_by: "", note: "", decided_at: "2026-01-01 00:00:00.000" },
    { company_id: "", field: "status", source: "ratsit", precedence: 300, removed: 0, decided_by: "", note: "", decided_at: "2026-01-01 00:00:00.000" },
  ],
  rules: [
    { company_id: COMPANY, field: "status", source: "bolagsverket", precedence: 10000, removed: 0, decided_by: "backoffice", note: "register is right", decided_at: "2026-09-05 08:00:00.000" },
  ],
  legalFormLabels: { "51": { label_en: "Economic association (ekonomisk förening)", label_sv: "Ekonomisk förening" } },
  legalFormOptions: [],
  foldPending: true,
};

function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/info", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/info${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SeBasicInfoWorkspace", () => {
  it("lists every field with its winning source and marks the selected row", () => {
    const html = render(<SeBasicInfoWorkspace companyId={COMPANY} detail={detail} selectedField="status" result={null} />, "?field=status");
    for (const label of ["Legal name", "Legal form", "Status", "Incorporated", "LEI", "Wikidata", "Description", "Description (Swedish)"]) {
      expect(html).toContain(label);
    }
    expect(html).toContain("Ekonomisk förening");
    expect(html).toContain('aria-current="true"');
    expect(html).toContain("fold-v1");
    expect(html).toContain("2026-09-04 17:04:01.293");
  });

  it("orders the panel by effective precedence, letting the company rule outrank the global map", () => {
    const html = render(<SeBasicInfoWorkspace companyId={COMPANY} detail={detail} selectedField="status" result={null} />, "?field=status");
    const reviewerAt = html.indexOf('data-source="reviewer"');
    const bolagsverketAt = html.indexOf('data-source="bolagsverket"');
    const scbAt = html.indexOf('data-source="scb"');
    const ratsitAt = html.indexOf('data-source="ratsit"');
    // The company rule lifts bolagsverket to 10000, ahead of scb's global 1000,
    // even though the global map alone ranks scb above bolagsverket. The
    // reviewer, ranked 10000 globally but without a value here, sinks below
    // every source with an opinion instead of leading the list greyed out.
    expect(bolagsverketAt).toBeLessThan(scbAt);
    expect(scbAt).toBeLessThan(ratsitAt);
    expect(reviewerAt).toBeGreaterThan(ratsitAt);
    // Slice each row out of the document so these checks read only that row's
    // own markup -- toMatch/toContain over the whole string would happily
    // find another row's later button and pass for the wrong reason.
    const nextRowAt = html.indexOf('data-source=', ratsitAt + 1);
    const ratsitRow = html.slice(ratsitAt, nextRowAt === -1 ? undefined : nextRowAt);
    expect(ratsitRow).toContain("no opinion");
    // Bolagsverket holds the active rule: it is preferred, carries the rule's
    // note, and offers no Use this; the field-level Reset to default sits in
    // the panel header and the left card flags the field's custom order.
    const bolagsverketRow = html.slice(bolagsverketAt, scbAt);
    expect(bolagsverketRow).toContain("preferred by reviewer");
    expect(bolagsverketRow).toContain("register is right");
    expect(bolagsverketRow).not.toContain("Use this");
    expect(html).toContain("Reset to default");
    expect(html).toContain("custom order");
    const withoutRules = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={{ ...detail, rules: [] }} selectedField="status" result={null} />,
      "?field=status",
    );
    expect(withoutRules).not.toContain("Reset to default");
    expect(withoutRules).not.toContain("custom order");
    // SCB is still the fold's published winner (Active), but since it does not
    // hold the rule it still offers Use this -- a reviewer can prefer the
    // currently-active source explicitly too.
    const scbRow = html.slice(scbAt, ratsitAt);
    expect(scbRow).toContain("Active");
    expect(scbRow).toContain("Use this");
    expect(scbRow).not.toContain("preferred by reviewer");
  });

  it("offers Reset to default for a typed reviewer value even with no company rule, and hides it when neither exists", () => {
    const reviewerRow: SeBasicInfoSuggestionRow = { ...bolagsverket, source: "reviewer", status: "inactive" };
    const typedNoRule: SeBasicInfoDetail = { ...detail, rules: [], suggestions: [...detail.suggestions, reviewerRow] };
    const html = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={typedNoRule} selectedField="status" result={null} />,
      "?field=status",
    );
    expect(html).toContain("Reset to default");

    const neither: SeBasicInfoDetail = { ...detail, rules: [] };
    const withoutEither = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={neither} selectedField="status" result={null} />,
      "?field=status",
    );
    expect(withoutEither).not.toContain("Reset to default");
  });

  it("confirms a reviewer-sourced Reset to default with the typed-value copy, not the company-rule copy", () => {
    const html = render(
      <DecisionDialogBody
        pending={{ intent: "reset", field: "status", source: "reviewer", value: "inactive", language: "" }}
        labels={detail.legalFormLabels}
        busy={false}
        onClose={() => {}}
      />,
    );
    expect(html).toContain("Reset status to default");
    // renderToStaticMarkup escapes the apostrophe as an HTML entity.
    expect(html).toContain("Reset to default clears the reviewer");
    expect(html).toContain("typed value for this field; the global order applies again at the next fold.");
    expect(html).not.toContain("Every company rule for this field is withdrawn");
  });

  it("marks a source with a value the precedence table does not rank as not ranked", () => {
    const wikidata: SeBasicInfoSuggestionRow = { ...bolagsverket, source: "wikidata", status: "active" };
    const withWikidata: SeBasicInfoDetail = { ...detail, suggestions: [...detail.suggestions, wikidata] };
    const html = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={withWikidata} selectedField="status" result={null} />,
      "?field=status",
    );
    const wikidataAt = html.indexOf('data-source="wikidata"');
    const nextRowAt = html.indexOf('data-source=', wikidataAt + 1);
    const wikidataRow = html.slice(wikidataAt, nextRowAt === -1 ? undefined : nextRowAt);
    expect(wikidataRow).toContain("not ranked");
    expect(wikidataRow).toContain("Use this");
    const scbAt = html.indexOf('data-source="scb"');
    const ratsitAt = html.indexOf('data-source="ratsit"');
    const scbRow = html.slice(scbAt, ratsitAt);
    expect(scbRow).not.toContain("not ranked");
  });

  it("shows only the reviewer row's value -- never Use this, Release, or not ranked", () => {
    const reviewerRow: SeBasicInfoSuggestionRow = { ...bolagsverket, source: "reviewer", status: "inactive" };
    const withReviewer: SeBasicInfoDetail = { ...detail, suggestions: [...detail.suggestions, reviewerRow] };
    const html = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={withReviewer} selectedField="status" result={null} />,
      "?field=status",
    );
    const reviewerAt = html.indexOf('data-source="reviewer"');
    const nextRowAt = html.indexOf('data-source=', reviewerAt + 1);
    const reviewerRowHtml = html.slice(reviewerAt, nextRowAt === -1 ? undefined : nextRowAt);
    expect(reviewerRowHtml).toContain("inactive");
    expect(reviewerRowHtml).not.toContain("Use this");
    expect(reviewerRowHtml).not.toContain("Release");
    expect(reviewerRowHtml).not.toContain("not ranked");
  });

  it("shows the fold-pending alert with Fold now, and the poller after a launch", () => {
    const html = render(<SeBasicInfoWorkspace companyId={COMPANY} detail={detail} selectedField="legal_name" result={null} />);
    expect(html).toContain("Fold pending");
    expect(html).toContain('value="fold-now"');
    const launched = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={detail} selectedField="legal_name" result={{ ok: true, intent: "fold-now", launched: { runId: "run-9", url: null } }} />,
    );
    expect(launched).toContain("run-9");
    const settled = render(<SeBasicInfoWorkspace companyId={COMPANY} detail={{ ...detail, foldPending: false }} selectedField="legal_name" result={null} />);
    expect(settled).not.toContain("Fold pending");
  });

  it("picks the success copy from the result's intent", () => {
    const at = (intent: string, decidedAt: string) =>
      render(
        <SeBasicInfoWorkspace
          companyId={COMPANY}
          detail={detail}
          selectedField="lei"
          result={{ ok: true, intent, decidedAt }}
        />,
      );
    expect(at("edit", "2026-09-05 09:00:00.000")).toContain("Draft saved at 2026-09-05 09:00:00.000.");
    expect(at("discard", "2026-09-05 09:02:00.000")).toContain("Draft discarded at 2026-09-05 09:02:00.000.");
    expect(at("activate", "2026-09-05 09:01:00.000")).toContain(
      "Reviewer value activated at 2026-09-05 09:01:00.000. Fold now to publish it.",
    );
    expect(at("reset", "2026-09-05 09:03:00.000")).toContain("Reset at 2026-09-05 09:03:00.000. Fold now to publish it.");
    expect(at("use-this", "2026-09-04 19:30:00.123")).toContain(
      "Rule written at 2026-09-04 19:30:00.123. Fold now to publish it.",
    );
  });

  it("shows a use-this refusal in the panel's own alert, not inside the edit sheet's form", () => {
    const html = render(
      <SeBasicInfoWorkspace
        companyId={COMPANY}
        detail={{ ...detail, foldPending: false }}
        selectedField="lei"
        result={{ ok: false, intent: "use-this", error: "SCB has no LEI for this company." }}
      />,
    );
    expect(html).toContain("Not saved");
    expect(html).toContain("SCB has no LEI for this company.");
    // The sheet's own footer paragraph (SeBasicInfoEditForm's error line) also
    // renders role="alert", so a second occurrence here would mean the
    // refusal reached the sheet too; a use-this refusal must produce exactly
    // the panel's own "Not saved" alert and nothing else.
    expect((html.match(/role="alert"/g) ?? []).length).toBe(1);
  });

  it("renders an error result and the not-folded state", () => {
    expect(render(<SeBasicInfoWorkspace companyId={COMPANY} detail={detail} selectedField="lei" result={{ ok: false, intent: "use-this", error: "Unknown source." }} />)).toContain("Unknown source.");
    expect(renderToStaticMarkup(<SeBasicInfoNotFolded companyId={COMPANY} />)).toContain("not in se_company_basic_info yet");
  });

  it("offers Use this when the company has no main row", () => {
    const html = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={{ ...detail, info: null, foldPending: true }} selectedField="status" result={null} />,
      "?field=status",
    );
    expect(html).toContain("Not folded yet");
    const scbAt = html.indexOf('data-source="scb"');
    const ratsitAt = html.indexOf('data-source="ratsit"');
    const scbRow = html.slice(scbAt, ratsitAt);
    expect(scbRow).not.toContain("Active");
    expect(scbRow).toContain("Use this");
  });

  it("keeps the note out of the panel until a decision is being confirmed", () => {
    const html = render(<SeBasicInfoWorkspace companyId={COMPANY} detail={detail} selectedField="status" result={null} />, "?field=status");
    expect(html).not.toContain('name="note"');
    expect(html).not.toContain("Why this value");
  });

  it("confirms a Use this with the value, the source, the hidden intent fields and the note", () => {
    const html = render(
      <DecisionDialogBody
        pending={{ intent: "use-this", field: "status", source: "bolagsverket", value: "inactive", language: "" }}
        labels={detail.legalFormLabels}
        busy={false}
        onClose={() => {}}
      />,
    );
    expect(html).toContain("Use this status");
    expect(html).toContain("Bolagsverket");
    expect(html).toContain("inactive");
    expect(html).toContain('name="intent"');
    expect(html).toContain('value="use-this"');
    expect(html).toContain('value="status"');
    expect(html).toContain('name="source"');
    expect(html).toContain('value="bolagsverket"');
    expect(html).toContain('name="note"');
    expect(html).toContain("Why this value");
    expect(html).toContain("Cancel");
  });

  it("confirms a Reset to default with the ruled source shown and no source field posted", () => {
    const html = render(
      <DecisionDialogBody
        pending={{ intent: "reset", field: "description", source: "bolagsverket", value: "Kept text", language: "sv" }}
        labels={detail.legalFormLabels}
        busy={false}
        onClose={() => {}}
      />,
    );
    expect(html).toContain("Reset description to default");
    expect(html).toContain("Bolagsverket");
    expect(html).toContain("Kept text");
    expect(html).toContain('value="reset"');
    expect(html).not.toContain('name="source"');
    expect(html).toContain("Every company rule for this field is withdrawn");
    expect(html).toContain("Why reset (optional)");
  });

  it("confirms an Activate with the draft's copy, a note input and no source field", () => {
    const html = render(
      <DecisionDialogBody
        pending={{ intent: "activate", field: "status", source: "reviewer_draft", value: "inactive", language: "" }}
        labels={detail.legalFormLabels}
        busy={false}
        onClose={() => {}}
      />,
    );
    // renderToStaticMarkup escapes the apostrophe as an HTML entity.
    expect(html).toContain("Activate the draft for this field: the reviewer");
    expect(html).toContain("outranks every source and rule at the next fold.");
    expect(html).toContain('value="activate"');
    expect(html).toContain('value="status"');
    expect(html).not.toContain('name="source"');
    expect(html).toContain('name="note"');
    expect(html).toContain(">Activate<");
  });

  it("confirms a Discard with the draft's copy, no note input and no source field", () => {
    const html = render(
      <DecisionDialogBody
        pending={{ intent: "discard", field: "status", source: "reviewer_draft", value: "inactive", language: "" }}
        labels={detail.legalFormLabels}
        busy={false}
        onClose={() => {}}
      />,
    );
    expect(html).toContain("Discard the draft value for this field.");
    expect(html).toContain('value="discard"');
    expect(html).toContain('value="status"');
    expect(html).not.toContain('name="source"');
    expect(html).not.toContain('name="note"');
    expect(html).toContain(">Discard<");
  });

  it("gives every left-card row an Edit button and marks a field with a draft value", () => {
    const draftRow: SeBasicInfoSuggestionRow = { ...emptyDraft, status: "inactive" };
    const withDraft: SeBasicInfoDetail = { ...detail, suggestions: [...detail.suggestions, draftRow] };
    const html = render(<SeBasicInfoWorkspace companyId={COMPANY} detail={withDraft} selectedField="status" result={null} />, "?field=status");
    expect((html.match(/>Edit</g) ?? []).length).toBe(8);
    const statusRowAt = html.indexOf(">Status<");
    const nextFieldAt = html.indexOf("</li>", statusRowAt);
    const statusRow = html.slice(statusRowAt, nextFieldAt);
    expect(statusRow).toContain(">draft<");
    // A field the draft has no opinion on gets no draft badge.
    const nameRowAt = html.indexOf(">Legal name<");
    const nameRowEnd = html.indexOf("</li>", nameRowAt);
    expect(html.slice(nameRowAt, nameRowEnd)).not.toContain(">draft<");
  });

  it("lists a Draft row with Activate and Discard only when the draft has a value for the field, and never marks it active", () => {
    const draftRow: SeBasicInfoSuggestionRow = { ...emptyDraft, status: "inactive" };
    const withDraft: SeBasicInfoDetail = { ...detail, suggestions: [...detail.suggestions, draftRow] };
    const html = render(<SeBasicInfoWorkspace companyId={COMPANY} detail={withDraft} selectedField="status" result={null} />, "?field=status");
    const draftAt = html.indexOf('data-source="reviewer_draft"');
    expect(draftAt).toBeGreaterThan(-1);
    const draftRowHtml = html.slice(draftAt, html.indexOf("</li>", draftAt));
    expect(draftRowHtml).toContain("Draft");
    expect(draftRowHtml).toContain("inactive");
    expect(draftRowHtml).toContain("Activate");
    expect(draftRowHtml).toContain("Discard");
    expect(draftRowHtml).not.toContain("Active<");
    expect(draftRowHtml).not.toContain("Use this");
    // Every row after the ranked sources is the Draft row -- it always sits last.
    expect(html.lastIndexOf("data-source=")).toBe(draftAt);

    const withoutDraftValue = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={detail} selectedField="status" result={null} />,
      "?field=status",
    );
    expect(withoutDraftValue).not.toContain('data-source="reviewer_draft"');
  });

  it("labels an active reviewer value 'typed by reviewer' instead of a rank caption", () => {
    const reviewerRow: SeBasicInfoSuggestionRow = { ...bolagsverket, source: "reviewer", status: "inactive" };
    const withReviewer: SeBasicInfoDetail = { ...detail, suggestions: [...detail.suggestions, reviewerRow] };
    const html = render(
      <SeBasicInfoWorkspace companyId={COMPANY} detail={withReviewer} selectedField="status" result={null} />,
      "?field=status",
    );
    const reviewerAt = html.indexOf('data-source="reviewer"');
    const nextRowAt = html.indexOf('data-source=', reviewerAt + 1);
    const reviewerRowHtml = html.slice(reviewerAt, nextRowAt === -1 ? undefined : nextRowAt);
    expect(reviewerRowHtml).toContain("typed by reviewer");
    expect(reviewerRowHtml).not.toContain("not ranked");
  });
});

describe("admin-se-company-info route", () => {
  beforeEach(() => {
    server.loadSeBasicInfoDetail.mockReset().mockResolvedValue(detail);
    server.appendSeBasicInfoRule.mockReset().mockResolvedValue({ decidedAt: "2026-09-04 19:30:00.123" });
    server.appendSeBasicInfoDraft.mockReset().mockResolvedValue({ decidedAt: "2026-09-05 09:00:00.000" });
    server.activateSeBasicInfoDraft.mockReset().mockResolvedValue({ decidedAt: "2026-09-05 09:01:00.000" });
    server.discardSeBasicInfoDraft.mockReset().mockResolvedValue({ decidedAt: "2026-09-05 09:02:00.000" });
    server.launchSeBasicInfoFold.mockReset().mockResolvedValue({ runId: "run-9", url: null });
  });

  it("loads the detail and the selected field from the URL", async () => {
    const response = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/info?field=status`),
      params: { companyId: COMPANY },
    } as never);
    expect(response.data).toEqual({ detail, selectedField: "status" });
    expect(response.init?.status).toBeUndefined();
    server.loadSeBasicInfoDetail.mockResolvedValueOnce(null);
    const missing = await loader({ request: new Request("http://x/info"), params: { companyId: COMPANY } } as never);
    expect(missing.data).toEqual({ detail: null, selectedField: "legal_name" });
    expect(missing.init?.status).toBe(404);
  });

  it("writes a precedence rule, launches a fold, and reports refusals", async () => {
    const post = (entries: Record<string, string>) => {
      const body = new FormData();
      for (const [key, value] of Object.entries(entries)) body.set(key, value);
      return action({ request: new Request("http://x/info", { method: "POST", body }), params: { companyId: COMPANY } } as never);
    };
    expect(await post({ intent: "use-this", field: "status", source: "bolagsverket" })).toEqual({ ok: true, intent: "use-this", decidedAt: "2026-09-04 19:30:00.123" });
    expect(server.appendSeBasicInfoRule).toHaveBeenCalledWith(COMPANY, { intent: "use-this", field: "status", source: "bolagsverket", note: "" });
    expect(await post({ intent: "fold-now" })).toEqual({ ok: true, intent: "fold-now", launched: { runId: "run-9", url: null } });
    expect(await post({ intent: "use-this", field: "status", source: "reviewer" })).toEqual({ ok: false, intent: "use-this", error: "Use this needs a source other than the reviewer." });
    expect(await post({ intent: "use-this", field: "status", source: "reviewer_draft" })).toEqual({ ok: false, intent: "use-this", error: "Use this needs a source other than the reviewer." });
    server.appendSeBasicInfoRule.mockRejectedValueOnce(new server.SeBasicInfoDecisionError("SCB has no LEI for this company."));
    expect(await post({ intent: "use-this", field: "lei", source: "scb" })).toEqual({ ok: false, intent: "use-this", error: "SCB has no LEI for this company." });
    server.appendSeBasicInfoRule.mockRejectedValueOnce(new Error("clickhouse down"));
    await expect(post({ intent: "reset", field: "lei" })).rejects.toThrow("clickhouse down");
    expect(server.appendSeBasicInfoRule).toHaveBeenLastCalledWith(COMPANY, { intent: "reset", field: "lei", note: "" });
  });

  it("maps edit, activate and discard to their own store writes, and surfaces their refusals", async () => {
    const post = (entries: Record<string, string>) => {
      const body = new FormData();
      for (const [key, value] of Object.entries(entries)) body.set(key, value);
      return action({ request: new Request("http://x/info", { method: "POST", body }), params: { companyId: COMPANY } } as never);
    };
    expect(
      await post({ intent: "edit", field: "lei", value: "5493001KJTIIGC8Y1R12", language: "", note: "typed from the register" }),
    ).toEqual({ ok: true, intent: "edit", decidedAt: "2026-09-05 09:00:00.000" });
    expect(server.appendSeBasicInfoDraft).toHaveBeenCalledWith(COMPANY, {
      intent: "edit",
      field: "lei",
      value: "5493001KJTIIGC8Y1R12",
      language: "",
      note: "typed from the register",
    });

    expect(await post({ intent: "activate", field: "lei", note: "" })).toEqual({ ok: true, intent: "activate", decidedAt: "2026-09-05 09:01:00.000" });
    expect(server.activateSeBasicInfoDraft).toHaveBeenCalledWith(COMPANY, { intent: "activate", field: "lei", note: "" });

    expect(await post({ intent: "discard", field: "lei" })).toEqual({ ok: true, intent: "discard", decidedAt: "2026-09-05 09:02:00.000" });
    expect(server.discardSeBasicInfoDraft).toHaveBeenCalledWith(COMPANY, { intent: "discard", field: "lei" });

    server.activateSeBasicInfoDraft.mockRejectedValueOnce(new server.SeBasicInfoDecisionError("No draft value for LEI to activate."));
    expect(await post({ intent: "activate", field: "lei", note: "" })).toEqual({ ok: false, intent: "activate", error: "No draft value for LEI to activate." });

    server.discardSeBasicInfoDraft.mockRejectedValueOnce(new server.SeBasicInfoDecisionError("No draft value for LEI to discard."));
    expect(await post({ intent: "discard", field: "lei" })).toEqual({ ok: false, intent: "discard", error: "No draft value for LEI to discard." });
  });

  it("refuses to act on a malformed company id before any write or launch", async () => {
    const body = new FormData();
    body.set("intent", "reset");
    body.set("field", "lei");
    const result = await action({
      request: new Request("http://x/info", { method: "POST", body }),
      params: { companyId: "abc" },
    } as never);
    expect(result).toEqual({ ok: false, intent: "", error: "Company id must be 10 or 12 digits." });
    expect(server.appendSeBasicInfoRule).not.toHaveBeenCalled();
    expect(server.launchSeBasicInfoFold).not.toHaveBeenCalled();
  });
});
