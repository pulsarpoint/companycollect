import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

// Hoisted mock of the ClickHouse-backed server module the route imports --
// see tests/people.corrections.test.ts / tests/se-company-info.server.test.ts
// for the same vi.hoisted + vi.mock("...server", ...) idiom, applied one
// layer up here so `action`'s calls to loadSeCompanyInfoDetail /
// appendSeCompanyInfoFieldValues are directly assertable without a live
// ClickHouse.
const server = vi.hoisted(() => ({
  loadSeCompanyInfoDetail: vi.fn(),
  appendSeCompanyInfoFieldValues: vi.fn(),
}));
vi.mock("~/lib/se-company-info.server", () => server);

const registryModule = vi.hoisted(() => ({ loadFieldRegistry: vi.fn() }));
vi.mock("~/lib/se-company-field-registry.server", () => registryModule);

import { action } from "~/routes/admin-se-company-info";
import {
  SeCompanyInfoNotPublished,
  SeCompanyInfoReviewWorkspace,
} from "~/components/admin/se-company-info-review-workspace";
import type { SeCompanyInfoDetail } from "~/lib/se-company-info.server";
import { SeInfoFieldValueValidationError } from "~/lib/se-info-field-values";
import { SeCompanyFieldResolveError } from "~/lib/se-company-field-resolve.server";
import { REGISTRY_FIXTURE } from "./se-field-registry.fixture";

const COMPANY_ID = "5565200028";
const EVIDENCE_HASH = "e".repeat(64);
const SUGGESTION_ID = "11111111-1111-4111-8111-111111111111";
const LIVE_VALUE_ID = "22222222-2222-4222-8222-222222222222";
const RELEASED_VALUE_ID = "33333333-3333-4333-8333-333333333333";

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
        customer_markets_json:
          '[{"name":"Corporate customers","confidence":0.9,"evidence_ids":["E0007"]}]',
        operating_geographies_json:
          '[{"name":"Nordics","confidence":0.85,"evidence_ids":["E0009"]}]',
        business_segments_json: "[]",
        // The shape the group-relationships extractor actually writes: no
        // `name` key, the display name lives under `related_company_name`.
        material_group_relationships_json:
          '[{"confidence":0.95,"evidence_ids":["E0008"],"jurisdiction":"Sweden","ownership_percentage":100.0,"related_company_name":"SEKETT AB","relationship_type":"subsidiary"}]',
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
  // Two decisions on the English field, newest first: the register's text was
  // taken after an earlier release handed the field back to the pipeline, so
  // the history carries a live row AND a released one.
  fieldValues: [
    {
      value_id: LIVE_VALUE_ID,
      field: "description",
      value: "Alpha builds payment software.",
      source: "scb",
      source_ref: "scb:1",
      source_at: "2026-08-01 00:00:00.000",
      decided_by: "backoffice",
      note: "Copied from the register.",
      created_at: "2026-08-23 09:00:00.000",
      is_live: 1,
    },
    {
      value_id: RELEASED_VALUE_ID,
      field: "description",
      value: null,
      source: "reviewer",
      source_ref: "",
      source_at: null,
      decided_by: "backoffice",
      note: "",
      created_at: "2026-08-22 10:00:00.000",
      is_live: 0,
    },
  ],
  naceLabel: "Computer programming activities",
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
  it("shows the merged row, its sources and the published suggestion, and posts an intent on every form", () => {
    const html = render();
    expect(html).toContain("Alpha AB");
    expect(html).toContain("IT-konsulter.");
    expect(html).toContain("Swedish fintech company");
    expect(html).toContain("published");
    // Four intents, no correction ledger anywhere: the page decides values.
    for (const intent of ["use-source", "use-suggestion", "edit", "release"]) {
      expect(html).toContain(`name="intent" value="${intent}"`);
    }
    expect(html).not.toContain("correction_kind");
    // No form carries an evidence hash any more: the latest value wins, so
    // there is no staleness for one to guard against.
    expect(html).not.toContain('name="evidence_hash"');
  });

  // Each source option offers its text as a value: the field it decides is the
  // one the language on screen belongs to, and the row it came from travels
  // with it as source_ref/source_at so Dagster keeps the provenance.
  it("offers Use this on every source option, posting the field the shown block decides", () => {
    const html = render();

    const scbForm = formContaining(html, 'name="source_ref" value="scb:1"');
    expect(scbForm).toContain('name="intent" value="use-source"');
    expect(scbForm).toContain('name="field" value="description"');
    expect(scbForm).toContain('name="source" value="scb"');
    expect(scbForm).toContain('name="value" value="IT consultancy."');
    expect(scbForm).toContain(
      'name="source_at" value="2026-08-01 00:00:00.000"',
    );
    expect(scbForm).toContain("Use this");

    // Wikidata's description carries no language marker, so it decides the
    // same field english does rather than the Swedish one.
    const wikidataForm = formContaining(
      html,
      'name="source_ref" value="wikidata:Q1"',
    );
    expect(wikidataForm).toContain('name="source" value="wikidata"');
    expect(wikidataForm).toContain('name="field" value="description"');
    expect(wikidataForm).toContain('name="value" value="Swedish fintech company"');

    // The published Final option is not a source: it is edited, not copied.
    expect(html).not.toContain('name="source" value="final"');
  });

  // Task 14: the published row holds both languages (migration 000301), so the
  // page shows both and the Final option's inline editor edits both.
  it("puts an inline editor behind Edit on the Final option, diffed against both originals", () => {
    const html = render();
    expect(html).toContain("Alpha builds payment software.");
    expect(html).toContain("Alpha bygger betalprogramvara.");
    expect(html).toContain(">Edit<");

    const editForm = formContaining(html, 'name="intent" value="edit"');
    for (const field of [
      'name="original_description"',
      'name="description"',
      'name="clear_description"',
      'name="original_description_sv"',
      'name="description_sv"',
      'name="clear_description_sv"',
      'name="note"',
    ]) {
      expect(editForm).toContain(field);
    }
    // The originals are what the edit is diffed against server-side; an absent
    // one makes the builder skip the field entirely, so both are always posted.
    expect(editForm).toContain(
      'name="original_description" value="Alpha builds payment software."',
    );
    expect(editForm).toContain(
      'name="original_description_sv" value="Alpha bygger betalprogramvara."',
    );
    // Both clear boxes post the literal the builder reads.
    expect(editForm.match(/value="yes"/g)).toHaveLength(2);
    // ...and the reviewer is told that emptying a box is not how you clear one.
    expect(editForm).toContain(
      "To remove this text, tick Clear — an emptied box is refused.",
    );
  });

  // The company with nothing published is exactly the one a reviewer opens the
  // page to fix, so the editor may not depend on there being text already: the
  // Final option is offered for every published row, empty or not.
  it("keeps the editor reachable on a company with no published description at all", () => {
    const html = render({
      ...detail,
      info: { ...detail.info, description: null, description_sv: null },
    });

    expect(html).toContain("Final (LLM)");
    expect(html).toContain("No description published.");
    expect(html).toContain(">Edit<");
    const editForm = formContaining(html, 'name="intent" value="edit"');
    expect(editForm).toContain('name="original_description" value=""');
    expect(editForm).toContain('name="original_description_sv" value=""');
  });

  it("says so when a company has no Swedish text at all", () => {
    const html = render({
      ...detail,
      info: { ...detail.info, description_sv: null },
    });
    expect(html).toContain("No Swedish description.");
    // The empty edit field still posts the original it was diffed against.
    expect(html).toContain('name="original_description_sv" value=""');
  });

  it("shows the suggestion's Swedish half beside its English one, each labelled", () => {
    const html = render();
    expect(html).toContain("English: Alpha builds payment software.");
    expect(html).toContain(
      "Swedish: Alpha AB bygger betalprogramvara i Sverige.",
    );
  });

  // Task 13 round 1: the artifact stamp means "when the pipeline recorded this
  // version", not a register date -- the label has to say so.
  it("explains what an artifact's observed stamp means", () => {
    const html = render();
    expect(html).toContain('title="when the pipeline recorded this version"');
  });

  it("keeps provenance and review metadata inside the closed additional-information disclosure", () => {
    const html = render();
    // Task 17: no description_source label anywhere -- the published card says
    // whether the model wrote the text and which sources fed it, without the
    // leftover "ClickHouse" label.
    expect(html).not.toContain("ClickHouse");
    expect(html).toContain("Additional information");
    expect(html).toContain('aria-expanded="false"');
    expect(html).toContain("LLM enhanced");
    expect(html).toContain(">yes<");
    expect(html).toContain("Description sources");
    expect(html).toContain("wikidata, scb");
    // Review state is no longer a row of ids in this disclosure: the Value
    // history card below says which decisions this company has, in full.
    expect(html).not.toContain("Correction ids");
  });

  it("says LLM no for a description that was copied from one input", () => {
    // The deterministic pick, a reviewer override and a rejected suggestion all
    // land here: the published text is not the model's.
    const html = render({
      ...detail,
      info: { ...detail.info, llm_enhanced: 0, description_sources: ["scb"] },
    });
    expect(html).toContain("LLM enhanced");
    expect(html).toContain(">no<");
    expect(html).toContain("Description sources");
    expect(html).toContain(">scb<");
  });

  // Task 18: Info is one tab of the company area, and the area's layout owns
  // the company header -- the name, the status/legal-form badges and the
  // company-page link. This page rendering them again would be a second,
  // duplicate header on every load. The layout's own copy is asserted in
  // tests/admin-se-company-area.test.tsx.
  it("carries no company header of its own: that moved to the area layout", () => {
    const html = render();
    expect(html).not.toContain("Company page");
    expect(html).not.toContain('href="/company/se/5565200028"');
    // The company name still appears -- inside the SCB artifact payload, which
    // is evidence, not a header.
    expect(html).not.toContain("<h1");
  });

  it("offers the published LLM description as the first About-the-company option", () => {
    const html = render();
    const menuStart = html.indexOf("About the company");
    expect(menuStart).toBeGreaterThan(-1);
    const finalOption = html.indexOf("Final (LLM)", menuStart);
    expect(finalOption).toBeGreaterThan(-1);
    // First option in the menu: it precedes every source option.
    expect(finalOption).toBeLessThan(html.indexOf("SCB register", menuStart));
    // Its content is the published english + swedish pair.
    expect(html).toContain("Alpha builds payment software.");
    expect(html).toContain("Alpha bygger betalprogramvara.");
  });

  it("shows a company-facts card under the description card", () => {
    const html = render();
    const menuStart = html.indexOf("About the company");
    const card = html.slice(menuStart, html.indexOf("Published version"));
    expect(card).toContain("Company facts");
    expect(card).toContain("Status");
    expect(card).toContain("active");
    expect(card).toContain("Incorporated");
    expect(card).toContain("2001-02-03");
    expect(card).toContain("Legal form");
    expect(card).toContain("Limited company (aktiebolag)");
    expect(card).toContain("NACE");
    expect(card).toContain("62.01");
    expect(card).toContain("Computer programming activities");
    expect(card).toContain("SNI");
    expect(card).toContain("62010");
  });

  it("lays the page out as description, facts, published version, sources, suggestions, value history", () => {
    const html = render();
    let cursor = -1;
    for (const heading of [
      "About the company",
      "Company facts",
      "Published version",
      "Sources",
      "Model suggestions",
      "Value history",
    ]) {
      const at = html.indexOf(heading, cursor + 1);
      expect(at, `${heading} in order`).toBeGreaterThan(cursor);
      cursor = at;
    }
    for (const heading of ["SCB register", "Wikidata", "ESEF filing"]) {
      expect(html).toContain(heading);
    }
    // The correction-era section headings are gone with the ledger.
    for (const gone of ["Override description", "Ledger", "No corrections yet"]) {
      expect(html).not.toContain(gone);
    }
    // The published row's status remains available in its closed details.
    expect(html).toContain("Status");
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
      info: {
        ...detail.info,
        legal_form_label_sv: "",
        legal_form_label_en: "",
      },
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
    for (const label of [
      "Label",
      "Official name",
      "Headquarters",
      "Employees",
    ]) {
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
      "Customer markets",
      "Operating geographies",
      "Group relationships",
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
    expect(html).toContain("<li>Corporate customers");
    expect(html).toContain("<li>Nordics");
    // Group-relationship items key their display name as
    // `related_company_name` rather than `name` -- ITEM_NAME_KEYS must find
    // it, and the rest of the object (minus that key) still shows as prose.
    expect(html).toContain("<li>SEKETT AB");
    expect(html).toContain("confidence 0.95 · E0008");
    expect(html).toContain("jurisdiction: Sweden");
    expect(html).toContain("ownership_percentage: 100");
    expect(html).toContain("relationship_type: subsidiary");
  });

  it("badges only the sources whose record actually contributed to the published description", () => {
    const html = render();
    expect(html.match(/contributes to description/g)).toHaveLength(1);
    // scb:1 is the contributing uid on this fixture, so the badge sits inside
    // the SCB group -- between its heading and the next source's. Anchor
    // inside the Sources section: the description carousel above it also
    // names "SCB register".
    const sourcesSection = html.indexOf("Every artifact row connected");
    const scbGroup = html.slice(
      html.indexOf("SCB register", sourcesSection),
      html.indexOf("Wikidata", html.indexOf("SCB register", sourcesSection)),
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

  it("shows the two descriptions cleanly and moves their provenance to dedicated fields", () => {
    const html = render();
    expect(html).toContain('lang="en"');
    expect(html).toContain('lang="sv"');
    expect(html).toContain("Sources");
    expect(html).toContain("SCB");
    expect(html).toContain("Wikidata");
    expect(html).not.toContain("(en · wikidata, scb)");
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

  it("confirms a save that resolved, and renders the not-published state", () => {
    const html = render(detail, {
      ok: true,
      valueIds: [
        "22222222-2222-4222-8222-222222222222",
        "33333333-3333-4333-8333-333333333333",
      ],
      resolved: ["description", "description_sv"],
      skipped: [],
    });
    expect(html).toContain("Saved and resolved.");
    expect(html).toContain("2 value rows saved");
    expect(html).not.toContain("on the next run.");
    expect(
      renderToStaticMarkup(
        <SeCompanyInfoNotPublished companyId="5565200028" />,
      ),
    ).toContain("not published");
  });

  // A python_only field is Dagster's alone (spec 9): the row is saved, the
  // page says when it lands. One field says "applies", several say "apply".
  it("names the fields that apply on the next run", () => {
    const one = render(detail, {
      ok: true,
      valueIds: ["22222222-2222-4222-8222-222222222222"],
      resolved: [],
      skipped: [{ field: "website", reason: "python_only" }],
    });
    expect(one).toContain("Saved. website applies on the next run.");
    expect(one).toContain("1 value row saved");
    expect(one).not.toContain("Saved and resolved.");

    const two = render(detail, {
      ok: true,
      valueIds: [
        "22222222-2222-4222-8222-222222222222",
        "33333333-3333-4333-8333-333333333333",
      ],
      resolved: [],
      skipped: [
        { field: "website", reason: "python_only" },
        { field: "employee_count", reason: "python_only" },
      ],
    });
    expect(two).toContain(
      "Saved. website, employee_count apply on the next run.",
    );
  });

  // A statement that wrote nothing (a release with no candidate left) is NOT
  // "applies on the next run": no run changes it, the field kept its value.
  it("says a field that resolved to no row kept what it had", () => {
    const html = render(detail, {
      ok: true,
      valueIds: ["22222222-2222-4222-8222-222222222222"],
      resolved: [],
      skipped: [{ field: "description", reason: "no_row" }],
    });
    expect(html).toContain(
      "Saved. description kept its previous value: nothing to resolve.",
    );
    expect(html).not.toContain("Saved and resolved.");
    expect(html).not.toContain("on the next run.");
  });

  it("keeps one sentence per reason when a decision hits several", () => {
    const html = render(detail, {
      ok: true,
      valueIds: [
        "22222222-2222-4222-8222-222222222222",
        "33333333-3333-4333-8333-333333333333",
      ],
      resolved: [],
      skipped: [
        { field: "website", reason: "python_only" },
        { field: "description", reason: "no_row" },
      ],
    });
    expect(html).toContain(
      "Saved. website applies on the next run. description kept its previous value: nothing to resolve.",
    );
  });

  it("tells a saved-but-unresolved decision apart from a refused one", () => {
    const unresolved = render(detail, {
      ok: false,
      error: "Saved, but not resolved: Memory limit. The decision is kept and applies on the next pipeline run.",
      valueIds: ["22222222-2222-4222-8222-222222222222"],
    });
    expect(unresolved).toContain("Saved, not resolved");
    expect(unresolved).toContain("The decision is kept");
    expect(unresolved).not.toContain("Not saved");

    const refused = render(detail, { ok: false, error: "Nothing changed." });
    expect(refused).toContain("Not saved");
    expect(refused).toContain("Nothing changed.");
  });

  it("renders a validation error", () => {
    const html = render(detail, {
      ok: false,
      error: "The evidence changed while you were reviewing.",
    });
    expect(html).toContain("The evidence changed while you were reviewing.");
  });

  // One button per suggestion, on every suggestion: a field value is a value,
  // not an approval, so the model's older wording is as usable as its newest.
  it("offers Use this suggestion on every suggestion, and no approve/reject anywhere", () => {
    const html = render();
    const useForm = formContaining(html, 'name="intent" value="use-suggestion"');
    expect(useForm).toContain(`name="suggestion_id" value="${SUGGESTION_ID}"`);
    expect(useForm).toContain("Use this suggestion");
    expect(html).not.toContain("approve_suggestion");
    expect(html).not.toContain("reject_suggestion");
    expect(html).not.toContain("Approve");

    const supersededDetail: SeCompanyInfoDetail = {
      ...detail,
      suggestions: [{ ...detail.suggestions[0], is_newest: 0 }],
    };
    const superseded = render(supersededDetail);
    expect(superseded).toContain("superseded evidence");
    expect(superseded).toContain("Use this suggestion");
    expect(superseded).toContain("Alpha builds payment software.");
  });

  // Every button on this page is one of many identical-looking ones, so each
  // needs an accessible name that says which text it writes where.
  it("names each Use-this and Use-this-suggestion button by what it decides", () => {
    const html = render();
    expect(html).toContain(
      'aria-label="Use the SCB register text as the English description"',
    );
    expect(html).toContain(
      `aria-label="Use suggestion ${SUGGESTION_ID.slice(0, 8)}"`,
    );

    // A repeated source's options are told apart by the meta line the menu
    // disambiguates them with, and the Swedish half is named as such.
    const twoFilings = render({
      ...detail,
      artifacts: [
        detail.artifacts[2],
        {
          ...detail.artifacts[2],
          source_record_uid: "esef:doc-2",
          payload: {
            ...detail.artifacts[2].payload,
            fiscal_year: "2024",
            company_description: "Alpha levererar betalinfrastruktur.",
            description_language: "sv",
          },
        },
      ],
    });
    expect(twoFilings).toContain(
      'aria-label="Use the ESEF filing (fiscal 2025) text as the English description"',
    );
    expect(twoFilings).toContain(
      'aria-label="Use the ESEF filing (fiscal 2024) text as the Swedish description"',
    );
  });

  it("does not offer a suggestion the model left without a description", () => {
    // The server refuses this one ("That suggestion has no description."); the
    // button says so first rather than spending a round trip on it.
    const html = render({
      ...detail,
      suggestions: [
        { ...detail.suggestions[0], suggestion: '{"language":"en"}' },
      ],
    });
    expect(
      formContaining(html, 'name="intent" value="use-suggestion"'),
    ).toContain('disabled=""');
    // ...and a usable one is not disabled.
    expect(
      formContaining(render(), 'name="intent" value="use-suggestion"'),
    ).not.toContain('disabled=""');
  });

  it("shows the value history, its live row and its releases, with a Release button per field", () => {
    const html = render();
    const history = html.slice(html.indexOf("Value history"));
    expect(history).toContain(
      "Every value decided for this company, newest first.",
    );
    // The live row is badged, and it is the only one.
    expect(history.match(/>live</g)).toHaveLength(1);
    // A NULL value is a release, said in words rather than shown as a blank.
    expect(history).toContain("released to pipeline");
    expect(history).toContain("Alpha builds payment software.");
    expect(history).toContain("scb:1");
    expect(history).toContain("Copied from the register.");
    expect(history).toContain("backoffice · 2026-08-23 09:00:00.000");

    // One release form per field, whatever the history holds.
    expect(html.match(/name="intent" value="release"/g)).toHaveLength(2);
    const svRelease = formContaining(
      html,
      'name="field" value="description_sv"',
    );
    expect(svRelease).toContain('name="intent" value="release"');
    expect(svRelease).toContain("Release to pipeline");
  });

  it("says so when nothing has been decided for a company yet", () => {
    const html = render({ ...detail, fieldValues: [] });
    expect(html).toContain("No values decided yet.");
    // The release buttons stand whether or not anything was decided before.
    expect(html.match(/name="intent" value="release"/g)).toHaveLength(2);
  });
});

describe("admin-se-company-info action (field-value intents, mocked server module)", () => {
  beforeEach(() => {
    server.loadSeCompanyInfoDetail.mockReset();
    server.appendSeCompanyInfoFieldValues.mockReset();
    server.loadSeCompanyInfoDetail.mockResolvedValue(detail);
    server.appendSeCompanyInfoFieldValues.mockResolvedValue({
      valueIds: [],
      resolved: [],
      skipped: [],
    });
    registryModule.loadFieldRegistry.mockReset();
    registryModule.loadFieldRegistry.mockResolvedValue(REGISTRY_FIXTURE);
  });

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

  it("writes one artifact's text and returns the ids", async () => {
    server.appendSeCompanyInfoFieldValues.mockResolvedValue({
      valueIds: ["66666666-6666-4666-8666-666666666666"],
      resolved: ["description"],
      skipped: [],
    });

    const result = await postAction({
      intent: "use-source",
      field: "description",
      value: "Alpha builds payment software.",
      source: "scb",
      source_ref: "scb:1",
      source_at: "2026-08-01 00:00:00.000",
    });

    expect(result).toEqual({
      ok: true,
      valueIds: ["66666666-6666-4666-8666-666666666666"],
      resolved: ["description"],
      skipped: [],
    });
    expect(server.loadSeCompanyInfoDetail).toHaveBeenCalledWith(COMPANY_ID);
    expect(server.appendSeCompanyInfoFieldValues).toHaveBeenCalledTimes(1);
    expect(server.appendSeCompanyInfoFieldValues).toHaveBeenCalledWith([
      {
        companyId: COMPANY_ID,
        field: "description",
        value: "Alpha builds payment software.",
        source: "scb",
        sourceRef: "scb:1",
        sourceAt: "2026-08-01 00:00:00.000",
      },
    ], { registry: REGISTRY_FIXTURE });
  });

  it("builds both languages from the suggestion the page is showing", async () => {
    server.appendSeCompanyInfoFieldValues.mockResolvedValue({
      valueIds: [
        "66666666-6666-4666-8666-666666666666",
        "77777777-7777-4777-8777-777777777777",
      ],
      resolved: ["description", "description_sv"],
      skipped: [],
    });

    const result = await postAction({
      intent: "use-suggestion",
      suggestion_id: SUGGESTION_ID,
    });

    expect(result).toEqual({
      ok: true,
      valueIds: [
        "66666666-6666-4666-8666-666666666666",
        "77777777-7777-4777-8777-777777777777",
      ],
      resolved: ["description", "description_sv"],
      skipped: [],
    });
    const [inputs] = server.appendSeCompanyInfoFieldValues.mock.calls[0] as [
      Array<{ field: string }>,
    ];
    expect(inputs).toHaveLength(2);
    expect(inputs.map((input) => input.field)).toEqual([
      "description",
      "description_sv",
    ]);
    expect(inputs[0]).toMatchObject({
      companyId: COMPANY_ID,
      source: "llm",
      sourceRef: SUGGESTION_ID,
      sourceAt: detail.suggestions[0].created_at,
    });
  });

  it("refuses an edit that changed nothing, without writing", async () => {
    const result = await postAction({
      intent: "edit",
      description: detail.info.description ?? "",
      original_description: detail.info.description ?? "",
      description_sv: detail.info.description_sv ?? "",
      original_description_sv: detail.info.description_sv ?? "",
      note: "",
    });

    expect(result).toEqual({ ok: false, error: "Nothing changed." });
    expect(server.appendSeCompanyInfoFieldValues).not.toHaveBeenCalled();
  });

  it("sends an emptied textarea to the store rather than swallowing it", async () => {
    // Clearing a field is the clear box's job. An emptied textarea is a change
    // like any other, so the action must not read it as "Nothing changed." --
    // it reaches the store as an empty value, which the real validator refuses
    // (pinned in tests/se-info-field-values.test.ts; the mock stands in for it
    // here, so this case pins the handoff and the message, not the rule).
    server.appendSeCompanyInfoFieldValues.mockRejectedValue(
      new SeInfoFieldValueValidationError("Value cannot be empty."),
    );

    const result = await postAction({
      intent: "edit",
      description: "   ",
      original_description: detail.info.description ?? "",
      description_sv: detail.info.description_sv ?? "",
      original_description_sv: detail.info.description_sv ?? "",
      note: "",
    });

    expect(server.appendSeCompanyInfoFieldValues).toHaveBeenCalledWith([
      {
        companyId: COMPANY_ID,
        field: "description",
        value: "",
        source: "reviewer",
        note: "",
      },
    ], { registry: REGISTRY_FIXTURE });
    expect(result).toEqual({ ok: false, error: "Value cannot be empty." });
  });

  it("hands the store's own refusal back to the reviewer", async () => {
    server.appendSeCompanyInfoFieldValues.mockRejectedValue(
      new SeInfoFieldValueValidationError("This company is not published."),
    );

    const result = await postAction({
      intent: "release",
      field: "description",
    });

    expect(result).toEqual({
      ok: false,
      error: "This company is not published.",
    });
  });

  it("rethrows anything that is not a validation refusal", async () => {
    server.appendSeCompanyInfoFieldValues.mockRejectedValue(
      new Error("ClickHouse is down"),
    );

    await expect(
      postAction({ intent: "release", field: "description" }),
    ).rejects.toThrow("ClickHouse is down");
  });

  it("reports a resolve failure as saved-but-not-resolved, with the ids", async () => {
    server.appendSeCompanyInfoFieldValues.mockRejectedValue(
      new SeCompanyFieldResolveError(
        ["66666666-6666-4666-8666-666666666666"],
        new Error("Code: 241. DB::Exception: Memory limit"),
      ),
    );

    const result = await postAction({ intent: "release", field: "description" });

    expect(result).toEqual({
      ok: false,
      error:
        "Saved, but not resolved: Code: 241. DB::Exception: Memory limit. The decision is kept and applies on the next pipeline run.",
      valueIds: ["66666666-6666-4666-8666-666666666666"],
    });
  });

  it("checks the posted field against the registry, not a built-in list", async () => {
    server.appendSeCompanyInfoFieldValues.mockResolvedValue({
      valueIds: ["66666666-6666-4666-8666-666666666666"],
      resolved: ["legal_name"],
      skipped: [],
    });

    const known = await postAction({
      intent: "use-source",
      field: "legal_name",
      value: "Alpha AB",
      source: "scb",
      source_ref: "scb:1",
    });
    expect(known).toMatchObject({ ok: true });

    const unknown = await postAction({
      intent: "release",
      field: "not_a_field",
    });
    expect(unknown).toEqual({ ok: false, error: "Unknown field." });
    expect(registryModule.loadFieldRegistry).toHaveBeenCalledTimes(2);
  });
});
