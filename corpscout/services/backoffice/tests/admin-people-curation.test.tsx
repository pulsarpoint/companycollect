import { renderToStaticMarkup } from "react-dom/server";
import {
  createMemoryRouter,
  MemoryRouter,
  RouterProvider,
} from "react-router";
import { describe, expect, it } from "vitest";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import {
  DraftTwoRowsTable,
  peopleDraftUrl,
} from "~/components/admin/people-draft-tables";
import { PeopleDraftTwoBuilder } from "~/components/admin/people-draft-two-builder";
import { PeopleCurationWorkspace } from "~/components/admin/people-curation-workspace";
import { SidebarProvider } from "~/components/ui/sidebar";

function rowsPage<Row>(rows: Row[], totalRows = rows.length) {
  return {
    rows,
    page: 1,
    pageSize: 5,
    totalRows,
    totalPages: Math.ceil(totalRows / 5),
  };
}

const EMPTY_DRAFT_TABLE_PROPS = {
  draftTwoStatus: { tableExists: false, rowCount: 0 },
  draftTwoJob: null,
  draftOnePage: rowsPage([]),
  draftTwoPage: rowsPage([]),
  filter: {
    input: "",
    companyId: "",
    error: "",
    draftOneView: "all" as const,
    draftTwoView: "all" as const,
    draftTwoSources: [],
    draftTwoHasLlmSuggestion: false,
    draftOnePage: 1,
    draftTwoPage: 1,
    currentStep: "draft-1" as const,
  },
};

describe("people processing wizard", () => {
  it("builds stable URLs for wizard, filter, and pagination state", () => {
    expect(
      peopleDraftUrl({
        currentStep: "draft-2",
        companyId: "5565200028",
        draftOneView: "unmapped",
        draftTwoView: "multiple-sources",
        draftTwoSources: ["wikidata", "bolagsverket"],
        draftTwoHasLlmSuggestion: true,
        draftOnePage: 3,
        draftTwoPage: 4,
      }),
    ).toBe(
      "/admin/se/people?step=draft-2&company_id=5565200028&draft_1=unmapped&draft_2=multiple-sources&draft_2_source=bolagsverket&draft_2_source=wikidata&draft_2_llm=suggestion&draft_1_page=3&draft_2_page=4",
    );
  });

  it("starts with the Draft 1 table status and all wizard steps", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <PeopleCurationWorkspace
              {...EMPTY_DRAFT_TABLE_PROPS}
              draftStatus={{ tableExists: false, rowCount: 0 }}
              initializationJob={null}
            />
          ),
          action: () => null,
        },
      ],
      { initialEntries: ["/admin/se/people"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("People processing");
    expect(html).toContain("Step 1 of 3");
    expect(html).toContain("1. Draft 1");
    expect(html).toContain("2. Draft 2");
    expect(html).toContain("3. Final");
    expect(html).toContain("Prepare immutable source observations");
    expect(html).toContain("Draft 1 has no imported observations");
    expect(html).toContain("Initialize Draft 1");
    expect(html).not.toContain("Draft 2 is not connected yet");
  });

  it("renders the wizard step selected by the URL-backed filter", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <PeopleCurationWorkspace
              {...EMPTY_DRAFT_TABLE_PROPS}
              filter={{
                ...EMPTY_DRAFT_TABLE_PROPS.filter,
                draftTwoView: "multiple-sources",
                currentStep: "draft-2",
              }}
              draftStatus={{ tableExists: true, rowCount: 10 }}
              initializationJob={null}
              draftTwoStatus={{ tableExists: true, rowCount: 5 }}
            />
          ),
          action: () => null,
        },
      ],
      {
        initialEntries: [
          "/admin/se/people?step=draft-2&draft_2=multiple-sources",
        ],
      },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("Step 2 of 3");
    expect(html).toContain("Build normalized person candidates");
    expect(html).not.toContain("Prepare immutable source observations");
  });

  it("shows a guarded reinitialize action for an existing draft table", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <PeopleCurationWorkspace
              {...EMPTY_DRAFT_TABLE_PROPS}
              draftStatus={{ tableExists: true, rowCount: 1234 }}
              initializationJob={null}
              draftOnePage={rowsPage([
                {
                  observation_id: "draft-observation-1",
                  company_id: "5565200028",
                  name: "Staffan Salén",
                  source: "esef",
                  role_original: "Styrelsens ordförande",
                  fiscal_year: 2024,
                  description: null,
                  source_entity_id: "esef-candidate-1",
                  source_record_uid: "esef-record-1",
                  source_profile_hash: "profile-hash",
                  source_role_hash: "role-hash",
                  source_payload_json: "{}",
                  source_observed_at: "2026-08-20T12:00:00.000Z",
                },
              ])}
            />
          ),
          action: () => null,
        },
      ],
      { initialEntries: ["/admin/se/people"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("Draft ready · 1,234 rows");
    expect(html).toContain("people_draft_step_1");
    expect(html).toContain("1,234");
    expect(html).toContain("Local DuckDB");
    expect(html).toContain("Draft 1 observations");
    expect(html).toContain("All");
    expect(html).toContain("Unmapped");
    expect(html).toContain("Staffan Salén");
    expect(html).toContain("Styrelsens ordförande");
    expect(html).toContain("Rebuild Draft 1");
    expect(html).toContain("Continue to Draft 2");
    expect(html).not.toContain("max-w-4xl");
    expect(html).not.toContain("Delete and reinitialize");
  });

  it("shows the unmapped Draft 1 view without changing source values", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <PeopleCurationWorkspace
              {...EMPTY_DRAFT_TABLE_PROPS}
              filter={{
                input: "5565200028",
                companyId: "5565200028",
                error: "",
                draftOneView: "unmapped",
                draftTwoView: "all",
                draftTwoSources: [],
                draftTwoHasLlmSuggestion: false,
                draftOnePage: 1,
                draftTwoPage: 1,
                currentStep: "draft-1",
              }}
              draftStatus={{ tableExists: true, rowCount: 5_560_054 }}
              initializationJob={null}
              draftOnePage={rowsPage([
                {
                  observation_id: "unmapped-observation",
                  company_id: "5565200028",
                  name: "Original Person",
                  source: "bolagsverket",
                  role_original: "Unchanged original role",
                  fiscal_year: 2024,
                  description: null,
                  source_entity_id: "source-entry",
                  source_record_uid: "source-record",
                  source_profile_hash: "profile-hash",
                  source_role_hash: "role-hash",
                  source_payload_json: "{}",
                  source_observed_at: "2026-08-20T12:00:00.000Z",
                },
              ])}
            />
          ),
          action: () => null,
        },
      ],
      { initialEntries: ["/admin/se/people?draft_1=unmapped"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("Unmapped Draft 1 observations");
    expect(html).toContain("1 filtered immutable source");
    expect(html).toContain("Unchanged original role");
    expect(html).toContain('value="unmapped"');
  });

  it("shows persisted background import progress", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <PeopleCurationWorkspace
              {...EMPTY_DRAFT_TABLE_PROPS}
              draftStatus={{ tableExists: true, rowCount: 0 }}
              initializationJob={{
                jobId: "draft-job-1",
                workflowId: "backoffice-se-people-draft-1-draft-job-1",
                status: "running",
                phase: "importing",
                currentSource: "bolagsverket",
                processedRows: 2_250_000,
                totalRows: 5_560_060,
                insertedRows: 2_249_500,
                progressPercent: 41,
                message: "Importing Bolagsverket observations",
                errorMessage: "",
                createdAt: "2026-08-20T12:00:00.000Z",
                startedAt: "2026-08-20T12:00:01.000Z",
                completedAt: null,
                updatedAt: "2026-08-20T12:05:00.000Z",
              }}
            />
          ),
          action: () => null,
        },
      ],
      { initialEntries: ["/admin/se/people"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("Importing Bolagsverket observations");
    expect(html).toContain("41%");
    expect(html).toContain("2,250,000");
    expect(html).toContain("5,560,060");
    expect(html).toContain("2,249,500 inserted");
    expect(html).not.toContain("Initialize Draft 1</button>");
  });

  it("shows Draft 2 progress and per-source evidence", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <div>
              <PeopleDraftTwoBuilder
                status={{ tableExists: false, rowCount: 0 }}
                initialJob={{
                  jobId: "draft-two-job",
                  workflowId: "backoffice-se-people-draft-2-draft-two-job",
                  status: "running",
                  phase: "matching",
                  processedRows: 750,
                  totalRows: 1_000,
                  outputRows: 0,
                  skippedRolelessRows: 250,
                  skippedUnmappedRows: 2_911,
                  unmappedRoleExamples: [
                    "bolagsverket:other:Styrelseledarmot",
                    "bolagsverket:other:2022-01-25",
                  ],
                  progressPercent: 72,
                  message: "Matching people and positions across sources",
                  errorMessage: "",
                  createdAt: "2026-08-20T12:00:00.000Z",
                  startedAt: "2026-08-20T12:00:01.000Z",
                  completedAt: null,
                  updatedAt: "2026-08-20T12:05:00.000Z",
                }}
              />
              <DraftTwoRowsTable
                filter={{
                  input: "",
                  companyId: "",
                  error: "",
                  draftOneView: "all",
                  draftTwoView: "multiple-sources",
                  draftTwoSources: [],
                  draftTwoHasLlmSuggestion: false,
                  draftOnePage: 1,
                  draftTwoPage: 1,
                  currentStep: "draft-2",
                }}
                page={rowsPage([
                  {
                    draft_2_id: "draft-two-row",
                    company_id: "5565200028",
                    name: "David Mindus",
                    position: "chief_executive_officer",
                    start_year: 2020,
                    end_year: null,
                    source_count: 2,
                    observation_count: 4,
                    bolagsverket_source_ids: ["bolags-1", "bolags-2"],
                    bolagsverket_descriptions: [],
                    esef_source_ids: ["esef-1"],
                    esef_descriptions: [],
                    wikidata_source_ids: ["wiki-1"],
                    wikidata_descriptions: ["Swedish business executive"],
                    has_llm_suggestion: true,
                  },
                ])}
                rows={[
                  {
                    draft_2_id: "draft-two-row",
                    company_id: "5565200028",
                    name: "David Mindus",
                    position: "chief_executive_officer",
                    start_year: 2020,
                    end_year: null,
                    source_count: 2,
                    observation_count: 4,
                    bolagsverket_source_ids: ["bolags-1", "bolags-2"],
                    bolagsverket_descriptions: [],
                    esef_source_ids: ["esef-1"],
                    esef_descriptions: [],
                    wikidata_source_ids: ["wiki-1"],
                    wikidata_descriptions: ["Swedish business executive"],
                    has_llm_suggestion: true,
                  },
                ]}
              />
            </div>
          ),
          action: () => null,
        },
      ],
      { initialEntries: ["/admin/se/people"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("Matching people and positions across sources");
    expect(html).toContain("72%");
    expect(html).toContain("2,911 unmapped observations");
    expect(html).toContain("bolagsverket:other:Styrelseledarmot");
    expect(html).toContain("remain unchanged in Draft 1");
    expect(html).toContain("Multiple-source Draft 2 rows");
    expect(html).not.toContain('aria-label="Draft 2 source view"');
    expect(html).toContain("1 filtered person-position");
    expect(html).toContain("2 sources · 4 rows");
    expect(html).toContain("chief_executive_officer");
    expect(html).toContain("Swedish business executive");
    expect(html).toContain("Wikidata · 1");
    expect(html).toContain("LLM suggestion");
    expect(html).toContain("Saved");
    expect(html).toContain("Filters");
  });

  it("uses a collapsible country navigation with Sweden people active", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/admin/se/people"]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );

    expect(html).toContain("Countries");
    expect(html).toContain("General");
    expect(html).toContain("Sweden");
    expect(html).toContain("People");
    expect(html).toContain("Sources");
    expect(html).toContain('href="/admin/se/people"');
    expect(html).toContain('data-open=""');
  });

  it("opens the General section and links to the canonical roles page", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/admin/general/roles"]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );

    expect(html).toContain("General");
    expect(html).toContain("Roles");
    expect(html).toContain('href="/admin/general/roles"');
    expect(html).toContain('data-open=""');
  });
});
