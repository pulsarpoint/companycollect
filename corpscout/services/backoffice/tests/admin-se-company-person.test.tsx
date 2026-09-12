import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({
  loadSePersonDetail: vi.fn(),
  loadSePersonRoleOptions: vi.fn(),
  saveSePersonDraft: vi.fn(),
  activateSePersonDraft: vi.fn(),
  discardSePersonDraft: vi.fn(),
  removeSePerson: vi.fn(),
  mergeSePersons: vi.fn(),
  splitSePersonSlots: vi.fn(),
  resetSePersonRules: vi.fn(),
  launchSePersonFold: vi.fn(),
  SePersonDecisionError: class SePersonDecisionError extends Error {},
}));
vi.mock("~/lib/se-company-person-entity.server", () => server);

import { action, loader } from "~/routes/admin-se-company-person";
import {
  PersonDecisionDialogBody,
  SePersonWorkspace,
  type PendingPersonDecision,
} from "~/components/admin/se-person-workspace";
import { Dialog } from "~/components/ui/dialog";
import type { SePersonDetail, SePersonPublished } from "~/lib/se-company-person-entity.server";
import { matchNote } from "~/lib/se-person-match";

const COMPANY = "5560125220";
const KEY = "a".repeat(64);
const OTHER = "b".repeat(64);
const SLOT = "r20260910120000123";
const ROLE_OPTIONS = [{ code: "board_member", label: "Board member", group: "governance" }];
const EMPTY_DETAIL: SePersonDetail = {
  published: [], drafts: [], history: [], rules: [], precedence: [],
  possibleMatches: [], foldPending: false,
};
const row = {
  company_id: COMPANY, person_key: KEY, display_name: "Anna Svensson",
  first_name: "Anna", last_name: "Svensson", birth_year: "1975", wikidata_id: "",
  sources: ["bolagsverket", "esef"], slots: ["uid-1:sig-1", "doc-9:cand-1"],
  normalized_ids: ["n1", "n2"],
  member_sources: ["bolagsverket", "esef"], member_slots: ["uid-1:sig-1", "doc-9:cand-1"],
  member_names: ["Anna Svensson", "Anna Maria Svensson"], member_birth_years: ["1975", ""],
  member_wikidata_ids: ["", ""], member_data: ["{}", "{}"],
  role_codes: ["board_member"], role_years: [2025], role_sources: [["bolagsverket"]],
  current_roles: ["board_member"], first_year: "2025", last_year: "2025",
  text_source: "bolagsverket", data: "{}", active: 1, inactive_reason: "",
  folded_at: "2026-09-10 09:00:00.000", fold_version: "se-person-fold-v1",
  source_run_id: "run-fold",
};
const published: SePersonPublished = {
  row,
  members: [
    {
      source: "bolagsverket", slot: "uid-1:sig-1", normalizedId: "n1", name: "Anna Svensson",
      birthYear: "1975", wikidataId: "", data: "{}", current: null, raw: null,
      refoldPending: false, precedence: 900, match: null,
    },
    {
      source: "esef", slot: "doc-9:cand-1", normalizedId: "n2", name: "Anna Maria Svensson",
      birthYear: "", wikidataId: "", data: "{}", current: null, raw: null,
      refoldPending: false, precedence: 400, match: null,
    },
  ],
  roles: [{ code: "board_member", year: 2025, sources: ["bolagsverket"] }],
  // Fresh: each row's folded_at matches the person row's own -- not older than it, so
  // not stale (F2).
  roleRows: [
    {
      company_id: COMPANY, person_key: KEY, role_code: "board_member", role_year: 2025,
      role_from: "", role_to: "", source: "bolagsverket", slot: "uid-1:sig-1",
      normalized_id: "n1", is_current: 1, folded_at: "2026-09-10 09:00:00.000",
    },
    {
      company_id: COMPANY, person_key: KEY, role_code: "auditor", role_year: 0,
      role_from: "2019-05-01", role_to: "2021-03-31", source: "wikidata",
      slot: "Q1:P169:Q7", normalized_id: "n3", is_current: 0,
      folded_at: "2026-09-10 09:00:00.000",
    },
  ],
  spellingReason: "precedence",
  rules: [],
  matchedBy: [],
};
const detail: SePersonDetail = { ...EMPTY_DETAIL, published: [published] };
/** Plan `2026-09-12-se-company-person-5-roles.md`'s readout person, Swedbank's Erik Bo
 * Bengtsson: board member 2021 and 2022 (ESEF), executive 2023 and 2024 (ESEF), legal
 * representative 2026 (Ratsit, the person's own current role) -- five role-year rows
 * across three role codes, the shape the grouped Roles section (owner request
 * 2026-09-12) folds down to one line per role. Fresh: every row's folded_at matches the
 * person row's own. */
const ERIK_ROLE_ROWS = [
  { company_id: COMPANY, person_key: KEY, role_code: "board_member", role_year: 2021, role_from: "", role_to: "", source: "esef", slot: "doc-1:cand-1", normalized_id: "n1", is_current: 0, folded_at: row.folded_at },
  { company_id: COMPANY, person_key: KEY, role_code: "board_member", role_year: 2022, role_from: "", role_to: "", source: "esef", slot: "doc-2:cand-1", normalized_id: "n1", is_current: 0, folded_at: row.folded_at },
  { company_id: COMPANY, person_key: KEY, role_code: "executive", role_year: 2023, role_from: "", role_to: "", source: "esef", slot: "doc-3:cand-1", normalized_id: "n1", is_current: 0, folded_at: row.folded_at },
  { company_id: COMPANY, person_key: KEY, role_code: "executive", role_year: 2024, role_from: "", role_to: "", source: "esef", slot: "doc-4:cand-1", normalized_id: "n1", is_current: 0, folded_at: row.folded_at },
  { company_id: COMPANY, person_key: KEY, role_code: "legal_representative", role_year: 2026, role_from: "", role_to: "", source: "ratsit", slot: "doc-5:cand-1", normalized_id: "n1", is_current: 1, folded_at: row.folded_at },
];
const ERIK_ROLES = [
  { code: "board_member", year: 2021, sources: ["esef"] },
  { code: "board_member", year: 2022, sources: ["esef"] },
  { code: "executive", year: 2023, sources: ["esef"] },
  { code: "executive", year: 2024, sources: ["esef"] },
  { code: "legal_representative", year: 2026, sources: ["ratsit"] },
];
const erikPublished: SePersonPublished = {
  ...published,
  row: { ...row, current_roles: ["legal_representative"] },
  roles: ERIK_ROLES,
  roleRows: ERIK_ROLE_ROWS,
};
/** The strongest pair naming the Bolagsverket observation, as the loader derives it. */
const MATCH = {
  nameA: "Anna Svensson", nameB: "Anna Maria Svensson", confidence: 0.93,
  reason: "call name", members: ["n1", "n2"],
};
/** What `fold.py::_with_llm_match` wrote onto the published row. */
const FOLD_DATA =
  '{"llm_match":{"model":"deepseek-v4-flash","pairs":[{"a":"Anna Svensson","b":"Anna Maria Svensson","confidence":0.93,"reason":"call name"}],"prompt_version":"se-person-match-v1"},"role_kind":"board_member"}';

function post(body: Record<string, string>, repeated: [string, string][] = []): Request {
  const form = new URLSearchParams();
  for (const [key, value] of Object.entries(body)) form.set(key, value);
  for (const [key, value] of repeated) form.append(key, value);
  return new Request(`http://x/admin/se/company/${COMPANY}/people`, {
    method: "POST",
    body: form,
    headers: { "content-type": "application/x-www-form-urlencoded" },
  });
}
function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/people", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/people${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("admin-se-company-person route", () => {
  beforeEach(() => {
    server.loadSePersonDetail.mockReset().mockResolvedValue(detail);
    server.loadSePersonRoleOptions.mockReset().mockResolvedValue(ROLE_OPTIONS);
    server.saveSePersonDraft.mockReset().mockResolvedValue({ decidedAt: "s", slot: SLOT });
    server.activateSePersonDraft.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.discardSePersonDraft.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.removeSePerson.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.mergeSePersons.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.splitSePersonSlots.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.resetSePersonRules.mockReset().mockResolvedValue({ decidedAt: "s" });
    server.launchSePersonFold.mockReset().mockResolvedValue({ runId: "run-9", url: null });
  });

  it("loads the detail with the role catalog, and opens on an empty one for a company with no persons", async () => {
    const response = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/people`),
      params: { companyId: COMPANY },
    } as never);
    expect(response).toEqual({ detail, roleOptions: ROLE_OPTIONS, selectedKey: null });

    // No 404: Add person must stay reachable. The company layout 404s an unknown company.
    server.loadSePersonDetail.mockResolvedValueOnce(null);
    const missing = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/people`),
      params: { companyId: COMPANY },
    } as never);
    expect(missing.detail).toEqual(EMPTY_DETAIL);
  });

  it("passes the selected key from ?person=, ignoring anything malformed", async () => {
    const selected = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/people?person=${KEY}`),
      params: { companyId: COMPANY },
    } as never);
    expect(selected.selectedKey).toBe(KEY);
    const malformed = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/people?person=nope`),
      params: { companyId: COMPANY },
    } as never);
    expect(malformed.selectedKey).toBeNull();
  });

  it("refuses a bad company id and an unparseable post before touching the store", async () => {
    expect(await action({ request: post({ intent: "fold-now" }), params: { companyId: "12" } } as never)).toEqual({
      ok: false, intent: "", error: "Company id must be 10 or 12 digits.",
    });
    expect(await action({ request: post({ intent: "remove", person_key: "zz" }), params: { companyId: COMPANY } } as never)).toEqual({
      ok: false, intent: "remove", error: "Unknown person.",
    });
    expect(server.removeSePerson).not.toHaveBeenCalled();
  });

  it("dispatches every intent to its function with the parsed decision", async () => {
    await action({ request: post({ intent: "remove", person_key: KEY, note: "gone" }), params: { companyId: COMPANY } } as never);
    expect(server.removeSePerson).toHaveBeenCalledWith(COMPANY, { intent: "remove", personKey: KEY, note: "gone" });
    await action({ request: post({ intent: "reset", person_key: KEY }), params: { companyId: COMPANY } } as never);
    expect(server.resetSePersonRules).toHaveBeenCalledWith(COMPANY, { intent: "reset", personKey: KEY, note: "" });
    await action({ request: post({ intent: "merge" }, [["person_key", KEY], ["person_key", OTHER]]), params: { companyId: COMPANY } } as never);
    expect(server.mergeSePersons).toHaveBeenCalledWith(COMPANY, { intent: "merge", personKeys: [KEY, OTHER], note: "" });
    await action({ request: post({ intent: "split" }, [["slot", "doc-9:cand-1"]]), params: { companyId: COMPANY } } as never);
    expect(server.splitSePersonSlots).toHaveBeenCalledWith(COMPANY, { intent: "split", slots: ["doc-9:cand-1"], note: "" });
    await action({ request: post({ intent: "discard", slot: SLOT }), params: { companyId: COMPANY } } as never);
    expect(server.discardSePersonDraft).toHaveBeenCalledWith(COMPANY, { intent: "discard", slot: SLOT });
    const saved = await action({
      request: post({ intent: "save-draft", first_name: "Anna", last_name: "Svensson" }, [["role_code", "board_member"], ["role_from", "2025"], ["role_to", "2025"]]),
      params: { companyId: COMPANY },
    } as never);
    expect(saved).toEqual({ ok: true, intent: "save-draft", slot: SLOT });
    expect(server.saveSePersonDraft).toHaveBeenCalledWith(COMPANY, expect.objectContaining({ intent: "save-draft", replacesKey: null }));
  });

  it("takes a possible match's Merge through the existing merge intent, note and all", async () => {
    // Exactly what PossibleMatchesCard posts: no new intent, two person keys, and the
    // note `matchNote` built. A note the parser refused would read to the reviewer as
    // a broken button, so the whole post is pinned here.
    await action({
      request: post({ intent: "merge", note: "LLM match 0.64: same surname" }, [
        ["person_key", KEY],
        ["person_key", OTHER],
      ]),
      params: { companyId: COMPANY },
    } as never);
    // Asserted through `matchNote` itself, not a hand-built string: if the note's
    // format ever changes, the literal payload above (what a real card would have
    // posted under the OLD format) stops matching this expectation and the test fails.
    expect(server.mergeSePersons).toHaveBeenCalledWith(COMPANY, {
      intent: "merge",
      personKeys: [KEY, OTHER],
      note: matchNote(0.64, "same surname"),
    });
  });

  it("launches the fold on Fold now and, per Ruling 6, on Activate too", async () => {
    expect(await action({ request: post({ intent: "fold-now" }), params: { companyId: COMPANY } } as never)).toEqual({
      ok: true, intent: "fold-now", runId: "run-9", url: null,
    });
    const activated = await action({ request: post({ intent: "activate", slot: SLOT }), params: { companyId: COMPANY } } as never);
    expect(server.activateSePersonDraft).toHaveBeenCalledWith(COMPANY, { intent: "activate", slot: SLOT, note: "" });
    expect(activated).toEqual({ ok: true, intent: "activate", runId: "run-9", url: null });
    expect(server.launchSePersonFold).toHaveBeenCalledTimes(2);
  });

  it("answers ok with a message when Activate's rows land but the fold launch fails", async () => {
    // Minor 1: the reviewer rows and the hide rule are already written by the time the
    // launch runs, so a launch failure must not read as a lost write -- and a second
    // Activate would only answer "No draft to activate.", the draft already cleared.
    server.launchSePersonFold.mockRejectedValueOnce(new Error("Dagster is down"));
    const activated = await action({ request: post({ intent: "activate", slot: SLOT }), params: { companyId: COMPANY } } as never);
    expect(activated).toEqual({
      ok: true, intent: "activate", runId: undefined,
      message: "Rows written; the fold launch failed: Dagster is down. Use Fold now.",
    });
  });

  it("turns a store refusal into a form error and lets anything else through", async () => {
    server.removeSePerson.mockRejectedValueOnce(new server.SePersonDecisionError("Already hidden."));
    expect(await action({ request: post({ intent: "remove", person_key: KEY }), params: { companyId: COMPANY } } as never)).toEqual({
      ok: false, intent: "remove", error: "Already hidden.",
    });
    server.removeSePerson.mockRejectedValueOnce(new Error("ClickHouse is down"));
    await expect(action({ request: post({ intent: "remove", person_key: KEY }), params: { companyId: COMPANY } } as never)).rejects.toThrow("ClickHouse is down");
  });

  it("renders the workspace: the persons, their sources, roles and the fold state", () => {
    const html = render(
      <SePersonWorkspace companyId={COMPANY} detail={detail} roleOptions={ROLE_OPTIONS} selectedKey={KEY} result={null} />,
    );
    expect(html).toContain("Anna Svensson");
    expect(html).toContain("Bolagsverket");
    expect(html).toContain("ESEF");
    expect(html).toContain("Board member");
    expect(html).toContain("Add person");
    expect(html).toContain('aria-current="true"');
    const pending = render(
      <SePersonWorkspace companyId={COMPANY} detail={{ ...detail, foldPending: true }} roleOptions={ROLE_OPTIONS} selectedKey={null} result={null} />,
    );
    expect(pending).toContain("Fold pending");
    const empty = render(
      <SePersonWorkspace companyId={COMPANY} detail={EMPTY_DETAIL} roleOptions={ROLE_OPTIONS} selectedKey={null} result={null} />,
    );
    expect(empty).toContain("No people published yet");
    expect(empty).toContain("Add person");
  });

  it("lists the roles view's rows in the panel, and falls back to the row's arrays when the view has none", () => {
    const html = render(
      <SePersonWorkspace companyId={COMPANY} detail={detail} roleOptions={ROLE_OPTIONS} selectedKey={KEY} result={null} />,
    );
    // A stored row per (role, year, source, slot): the catalog label, the year, the
    // span when the observation carried one, and the source that saw it.
    expect(html).toContain("Board member");
    expect(html).toContain("2025");
    expect(html).toContain("2019-05-01");
    expect(html).toContain("2021-03-31");
    expect(html).toContain("Wikidata");
    expect(html).not.toContain("the roles view has not rebuilt");
    // F6: the filled badge (the current role, per code) explains what "current" means
    // here -- the person's own latest observed year, not "now". renderToStaticMarkup
    // HTML-escapes the apostrophe in the attribute value.
    expect(html).toContain('title="On the person&#x27;s latest observed year"');

    // No row yet -- a person folded since the view's last :20 rebuild. The panel shows
    // the person row's own arrays and says so, instead of claiming the person has no
    // role at all.
    const fallback = render(
      <SePersonWorkspace
        companyId={COMPANY}
        detail={{ ...detail, published: [{ ...published, roleRows: [] }] }}
        roleOptions={ROLE_OPTIONS}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(fallback).toContain("Board member");
    expect(fallback).toContain("the roles view has not rebuilt");
  });

  it("groups a person's roles by role, years compressed into ranges -- for the roles view's rows and the array fallback alike", () => {
    // Erik's five rows fold into three lines: board_member 2021-2022 as one compressed
    // range, executive 2023-2024, legal_representative 2026 -- not five per-year rows.
    const html = render(
      <SePersonWorkspace
        companyId={COMPANY}
        detail={{ ...detail, published: [erikPublished] }}
        roleOptions={ROLE_OPTIONS}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(html).toContain("2021–2022");
    expect((html.match(/Board member/g) ?? []).length).toBe(1);
    expect(html).toContain("2026");
    expect(html).toContain("legal_representative");
    expect(html).not.toContain("the roles view has not rebuilt");

    // The array fallback (no roles-view rows yet) groups the very same way.
    const fallback = render(
      <SePersonWorkspace
        companyId={COMPANY}
        detail={{ ...detail, published: [{ ...erikPublished, roleRows: [] }] }}
        roleOptions={ROLE_OPTIONS}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(fallback).toContain("2021–2022");
    expect((fallback.match(/Board member/g) ?? []).length).toBe(1);
    expect(fallback).toContain("2026");
    expect(fallback).toContain("the roles view has not rebuilt");

    // F2's stale-rows fallback still renders grouped and still says stale.
    const stale = render(
      <SePersonWorkspace
        companyId={COMPANY}
        detail={{
          ...detail,
          published: [
            {
              ...erikPublished,
              roleRows: erikPublished.roleRows.map((role) => ({
                ...role,
                folded_at: "2019-01-01 00:00:00.000",
              })),
            },
          ],
        }}
        roleOptions={ROLE_OPTIONS}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(stale).toContain("2021–2022");
    expect(stale).toContain("the roles view has not rebuilt");

    // F3's inactive-person fallback still renders grouped and gets the OTHER note.
    const inactive = render(
      <SePersonWorkspace
        companyId={COMPANY}
        detail={{
          ...detail,
          published: [
            {
              ...erikPublished,
              row: { ...erikPublished.row, active: 0, inactive_reason: "hidden" },
              roleRows: [],
            },
          ],
        }}
        roleOptions={ROLE_OPTIONS}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(inactive).toContain("2021–2022");
    expect(inactive).toContain("hidden and withdrawn persons are not in the roles view");
    expect(inactive).not.toContain("the roles view has not rebuilt");
  });

  it("F2: rows that predate the current fold are treated as stale, same fallback as no rows at all", () => {
    // The view rebuilds at :20 and holds whatever it saw at the last rebuild -- a row
    // stamped BEFORE the person's own current folded_at describes the person as they
    // were before this fold, not as the panel is showing them right now.
    const stale = render(
      <SePersonWorkspace
        companyId={COMPANY}
        detail={{
          ...detail,
          published: [
            {
              ...published,
              roleRows: published.roleRows.map((role) => ({
                ...role,
                folded_at: "2026-09-10 08:00:00.000",
              })),
            },
          ],
        }}
        roleOptions={ROLE_OPTIONS}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(stale).toContain("Board member");
    expect(stale).toContain("the roles view has not rebuilt");
  });

  it("F3: an inactive person never gets the stale-view note -- the view holds no row for one by design", () => {
    // The view's own WHERE is p.active = 1, so a hidden or withdrawn person's empty
    // roleRows is not staleness: it is the view working as designed, and the note must
    // say so instead of implying the view is behind.
    const html = render(
      <SePersonWorkspace
        companyId={COMPANY}
        detail={{
          ...detail,
          published: [
            { ...published, row: { ...row, active: 0, inactive_reason: "hidden" }, roleRows: [] },
          ],
        }}
        roleOptions={ROLE_OPTIONS}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(html).toContain("Board member");
    expect(html).toContain("hidden and withdrawn persons are not in the roles view");
    expect(html).not.toContain("the roles view has not rebuilt");
  });

  it("badges the matched member, reads the llm_match record as a list, and offers a Merge for a possible match", () => {
    const matched: SePersonDetail = {
      ...detail,
      published: [
        {
          ...published,
          row: { ...row, data: FOLD_DATA },
          members: [{ ...published.members[0], match: MATCH }, published.members[1]],
          matchedBy: [MATCH],
        },
      ],
      possibleMatches: [
        {
          personKeyA: KEY, personKeyB: OTHER, nameA: "Anna Maria Svensson",
          nameB: "Carl von Essen", confidence: 0.64, reason: "same surname",
        },
      ],
    };
    const html = render(
      <SePersonWorkspace
        companyId={COMPANY} detail={matched} roleOptions={ROLE_OPTIONS}
        selectedKey={KEY} result={null}
      />,
    );
    // The badge and its reason. Asserted in two pieces, not as one string: React puts
    // the score in its own text node, so the rendered markup may separate them.
    expect(html).toContain("matched by LLM");
    expect(html).toContain("0.93");
    expect(html).toContain('title="call name"');
    // The fold's record reads as a list -- the model and the prompt version included --
    // and the raw key never reaches the Data block, while the rest of `data` still does.
    expect(html).toContain("deepseek-v4-flash");
    expect(html).toContain("se-person-match-v1");
    expect(html).not.toContain("llm_match");
    expect(html).toContain("role_kind");
    // The card and one Merge, posting the EXISTING merge intent with both keys and the
    // note spec section 5 prescribes. Sliced to the card's OWN markup so the assertion
    // fails if the card's intent, its keys or its note format ever change, not merely
    // if `merge` appears anywhere else on the page.
    expect(html).toContain("Possible matches");
    const cardHtml = html.slice(html.indexOf("Possible matches"));
    expect(cardHtml).toContain("same surname");
    expect(cardHtml).toContain("0.64");
    expect(cardHtml).toContain('name="intent" value="merge"');
    expect(cardHtml).toContain(`name="person_key" value="${KEY}"`);
    expect(cardHtml).toContain(`name="person_key" value="${OTHER}"`);
    expect(cardHtml).toContain(`name="note" value="${matchNote(0.64, "same surname")}"`);
    // F6: an accessible name on the Merge button, the file's `PersonLine` precedent.
    expect(cardHtml).toContain('aria-label="Merge Anna Maria Svensson and Carl von Essen"');
    // A company with no pairs shows no card at all.
    expect(
      render(
        <SePersonWorkspace
          companyId={COMPANY} detail={detail} roleOptions={ROLE_OPTIONS}
          selectedKey={KEY} result={null}
        />,
      ),
    ).not.toContain("Possible matches");
  });

  it("marks the LLM match section stale once no current pair still names it", () => {
    // The fold recorded llm_match, but the CURRENT pairs (matchedBy) no longer confirm
    // it -- the candidate list moved on since the last fold.
    const stale: SePersonDetail = {
      ...detail,
      published: [{ ...published, row: { ...row, data: FOLD_DATA }, matchedBy: [] }],
    };
    const staleHtml = render(
      <SePersonWorkspace companyId={COMPANY} detail={stale} roleOptions={ROLE_OPTIONS} selectedKey={KEY} result={null} />,
    );
    expect(staleHtml).toContain(
      "Recorded by the last fold; the current pairs no longer name these observations.",
    );

    // The same llm_match record, but a current pair still names it: no stale note.
    const current: SePersonDetail = {
      ...detail,
      published: [{ ...published, row: { ...row, data: FOLD_DATA }, matchedBy: [MATCH] }],
    };
    const currentHtml = render(
      <SePersonWorkspace companyId={COMPANY} detail={current} roleOptions={ROLE_OPTIONS} selectedKey={KEY} result={null} />,
    );
    expect(currentHtml).not.toContain(
      "Recorded by the last fold; the current pairs no longer name these observations.",
    );
  });

  it("posts a split's checked slots, note and intent, with the caveat in the dialog's own words", () => {
    // Minor 2: nothing rendered `PersonDecisionDialogBody` before, so a renamed
    // `name=` here would have passed every hand-built-FormData parser test and broken
    // the Split dialog in production. `Dialog` (the root, no `DialogContent` portal) is
    // what `DialogTitle` inside the body needs to render at all.
    const pending: PendingPersonDecision = {
      intent: "split",
      personKey: "",
      slot: "",
      line: "Anna Svensson",
      members: published.members,
      reviewerOnly: false,
    };
    const html = render(
      <Dialog open>
        <PersonDecisionDialogBody pending={pending} busy={false} onClose={() => {}} />
      </Dialog>,
    );
    expect(html).toContain('name="intent" value="split"');
    // A split names slots through its own checkboxes, never through a hidden `slot`.
    expect(html).not.toContain('name="slot" value=""');
    expect(html).toContain('type="checkbox"');
    expect(html).toContain('name="slot" value="uid-1:sig-1"');
    expect(html).toContain('name="slot" value="doc-9:cand-1"');
    expect(html).toContain('name="note"');
    // The literal fragment, not the `SPLIT_CAVEAT` export -- the point is that the
    // dialog actually renders it, not that the constant matches itself.
    expect(html).toContain("Bolagsverket mints a new slot per annual report");
    // renderToStaticMarkup escapes the apostrophe as an HTML entity.
    expect(html).toContain("may need writing again after next year");
  });
});
