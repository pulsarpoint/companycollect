/**
 * The Pipeline sheet on the companies list: what it renders from the resource
 * route's data, what it says a launch covers, and what it posts.
 *
 * The sheet's own chrome is a Base UI dialog, which renders through a portal and
 * so produces nothing at all under `renderToStaticMarkup` -- the panel inside it
 * is therefore rendered directly, exactly as the filter sheet's fields are. Its
 * `Form` is a plain `<form>` here: what matters is the fields that would be
 * posted, and the route they would be posted to.
 *
 * The last two blocks are about the page this sheet replaced: the list's loader
 * must not have inherited the change scan, and the retired page must be gone
 * from the sidebar and the breadcrumbs.
 */
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, MemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import AdminLayout from "~/routes/admin-layout";
import { SidebarProvider } from "~/components/ui/sidebar";
import {
  NOTHING_FETCHED,
  openSheet,
  SeCompanyInfoPipelinePanel,
  withFetched,
  type PipelineFormComponent,
  type PipelineResult,
  type PipelineView,
} from "~/components/admin/se-company-info-pipeline";
import type { SeCompanyInfoPipelineStats } from "~/lib/se-company-info-pipeline.server";

const clickhouse = vi.hoisted(() => ({
  // Typed with the reader's own parameters so the recorded calls can be read
  // back as SQL: what this block asserts is which statements the list's loader
  // sent, not just how many.
  query: vi.fn(async (_sql: string, _params?: Record<string, unknown>) => []),
}));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

const pipelineStats = vi.hoisted(() => ({ load: vi.fn() }));
vi.mock("~/lib/se-company-info-pipeline.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/se-company-info-pipeline.server")>()),
  loadSeCompanyInfoPipelineStats: pipelineStats.load,
}));

const STATS: SeCompanyInfoPipelineStats = {
  selection: {
    companyCount: 3_500_000,
    changedCount: 1_240,
    changedWithoutModelCount: 900,
    wouldCallModelCount: 340,
    neverPublishedCount: 12,
    newEvidenceCounts: { scb: 800, esef: 40, wikidata: 60 },
    ledgerPendingCount: 5,
    pendingModelCount: 410,
  },
  artifacts: [
    { source: "scb", latestObservedAt: "2026-08-22 03:00:00.000", rowCount: 7_000_000 },
  ],
  models: [
    {
      modelName: "deepseek-v4-flash",
      callCount: 1_200,
      promptTokens: 640,
      completionTokens: 240,
    },
  ],
};

const VIEW: PipelineView = {
  kind: "view",
  stats: STATS,
  statsError: "",
  profiles: [
    {
      profileId: "0f8f2a3e-6c1e-4a0f-9c2b-9c9a7f4b1d20",
      name: "DeepSeek production",
      provider: "deepseek",
      model: "deepseek-v4-flash",
      baseUrl: "https://api.deepseek.com",
      isActive: true,
      apiKeyEnvironmentVariable: "DEEPSEEK_API_KEY",
      dagsterApiKeyVariable: "DEEPSEEK_API_KEY",
    },
  ],
  runs: [
    {
      runId: "9d1f0a2b3c4d5e6f",
      status: "SUCCESS",
      jobName: "se_company_info_review_job",
      startTime: 1_770_000_000,
      endTime: 1_770_000_120,
      tags: { pilot: "backoffice" },
      url: "https://dagster.example/runs/9d1f0a2b3c4d5e6f",
      numbers: { selected_company_count: 1_240, inserted_count: 1_200, llm_request_count: 340 },
    },
  ],
  instigators: {
    schedules: [
      { name: "se_company_info_schedule", status: "RUNNING", cronSchedule: "0 4 * * *" },
    ],
    sensors: [{ name: "se_company_info_field_value_sensor", status: "STOPPED" }],
  },
  dagsterError: "",
};

const PIPELINE_PATH = "/admin/se/companies/pipeline";
const PICKED = ["5560125220", "5565200028"];

/** The panel's forms, as plain markup: the fetcher's own `Form` posts to the
 * same `action`, which is what these assertions read. */
const FakeForm: PipelineFormComponent = ({ children, ...props }) => (
  <form {...props}>{children}</form>
);

function renderPanel({
  view = VIEW,
  result = null,
  selectedIds = [],
  loading = false,
}: {
  view?: PipelineView | null;
  result?: PipelineResult | null;
  selectedIds?: string[];
  loading?: boolean;
} = {}) {
  return renderToStaticMarkup(
    <MemoryRouter initialEntries={["/admin/se/companies"]}>
      <SeCompanyInfoPipelinePanel
        view={view}
        result={result}
        selectedIds={selectedIds}
        loading={loading}
        Form={FakeForm}
      />
    </MemoryRouter>,
  );
}

describe("the pipeline sheet's panel", () => {
  it("renders the change-scan counts, the artifacts and the observed cost", () => {
    const html = renderPanel();
    // Selected, and the model-off count beside it.
    expect(html).toContain("1,240");
    expect(html).toContain("900 without the model term");
    // Per-reason, exactly as the retired page showed them.
    expect(html).toContain("never published");
    expect(html).toContain("new esef");
    expect(html).toContain("ledger");
    // The two model numbers, the artifact freshness and the token averages.
    expect(html).toContain("340");
    expect(html).toContain("410");
    expect(html).toContain("2026-08-22 03:00:00.000");
    expect(html).toContain("1,200 calls · 640 prompt · 240 completion");
    // The automation badges and the recent-runs row.
    expect(html).toContain("se_company_info_schedule");
    expect(html).toContain("0 4 * * *");
    expect(html).toContain("9d1f0a2b");
    expect(html).toContain("SUCCESS");
  });

  it("offers the three runs, each confirming through the pipeline route", () => {
    const html = renderPanel();
    expect(html).toContain("Re-resolve changed companies");
    expect(html).toContain("Run the model pass");
    expect(html).toContain("Refresh an artifact");
    for (const intent of ["confirm-resolve", "confirm-model-pass", "confirm-artifact"]) {
      expect(html).toContain(`name="intent" value="${intent}"`);
    }
    // Every form posts to the resource route, never to the list's own action.
    expect(html.match(new RegExp(`action="${PIPELINE_PATH}"`, "g"))).toHaveLength(3);
    expect(html).toContain('method="post"');
    // The model config the run is parameterised by.
    expect(html).toContain('name="max_companies"');
    expect(html).toContain('name="concurrency"');
    expect(html).toContain('name="use_model"');
    expect(html).toContain("DeepSeek production");
  });

  it("says a launch covers ALL changed companies while nothing is picked", () => {
    const html = renderPanel();
    expect(html).toContain("all changed companies");
    expect(html).toContain("Nothing is ticked on the list");
    expect(html).not.toMatch(/\d+ selected compan/);
    // The scope field is still posted, empty: one field, read the same way in
    // both cases.
    expect(html).toContain('name="company_ids" value=""');
  });

  it('says "N selected companies" once the list has picks, and posts their ids', () => {
    const html = renderPanel({ selectedIds: PICKED });
    expect(html).toContain("2 selected companies");
    expect(html).toContain("including the ones the current filter or page does not show");
    // Both scoped launches carry the same ids; the artifact refresh does not
    // take a scope at all, so there are two of these and not three.
    const scope = html.match(/name="company_ids" value="5560125220,5565200028"/g);
    expect(scope).toHaveLength(2);
    expect(html).toContain("takes no company scope");
  });

  it("states the scope BEFORE any button that could launch something", () => {
    // Reviewer ruling: a pick can lie outside the current filter, so what a
    // launch covers is stated, never implied -- and stated above every control
    // that starts one, the confirmation panel's included.
    //
    // Asserted on the scope block's OWN marker: the confirmation's lines talk
    // about the scope too, so a search for the phrase would find that line and
    // pass no matter where the block itself sits.
    const html = renderPanel({
      selectedIds: PICKED,
      result: {
        kind: "result",
        ok: true,
        error: "",
        confirmation: {
          intent: "launch-resolve",
          title: "Re-resolve changed companies",
          lines: ["Only the ones the change scan still selects are resolved."],
          fields: { company_ids: PICKED.join(","), max_companies: "1000" },
        },
        launched: null,
      },
    });
    const scope = html.indexOf('data-slot="pipeline-scope"');
    expect(scope).toBeGreaterThan(-1);
    // The block itself says the scope, and it comes before both launch paths.
    expect(html.slice(scope, scope + 400)).toContain("2 selected companies");
    expect(scope).toBeLessThan(html.indexOf("Launch this run"));
    expect(scope).toBeLessThan(html.indexOf("Review…"));
  });

  it("withdraws the launch when the picks no longer match what was confirmed", () => {
    // A confirmation names counts, not ids: reviewed for A and B, it reads
    // exactly like one reviewed for C and D. So the button goes away rather
    // than launching a paid run over companies nobody looked at.
    const html = renderPanel({
      selectedIds: ["5567890123", "5569999999"],
      result: {
        kind: "result",
        ok: true,
        error: "",
        confirmation: {
          intent: "launch-resolve",
          title: "Re-resolve changed companies",
          lines: ["Scoped to 2 selected companies."],
          fields: { company_ids: PICKED.join(","), max_companies: "1000" },
        },
        launched: null,
      },
    });
    expect(html).toContain('data-slot="pipeline-scope-changed"');
    expect(html).toContain("The picks changed since this was reviewed");
    // Neither the button nor the fields it would have posted survive.
    expect(html).not.toContain("Launch this run");
    expect(html).not.toContain('name="intent" value="launch-resolve"');
    expect(html).not.toContain('value="5560125220,5565200028"');
  });

  it("leaves an artifact refresh launchable: it has no scope to go stale", () => {
    const html = renderPanel({
      selectedIds: PICKED,
      result: {
        kind: "result",
        ok: true,
        error: "",
        confirmation: {
          intent: "launch-artifact",
          title: "Refresh the esef artifact",
          lines: ["Runs se_company_info_esef_clickhouse and nothing else."],
          fields: { artifact: "esef" },
        },
        launched: null,
      },
    });
    expect(html).toContain("Launch this run");
    expect(html).toContain('name="artifact" value="esef"');
    expect(html).not.toContain('data-slot="pipeline-scope-changed"');
  });

  it("replays the confirmed fields into the launch, scope and all", () => {
    const html = renderPanel({
      selectedIds: PICKED,
      result: {
        kind: "result",
        ok: true,
        error: "",
        confirmation: {
          intent: "launch-model-pass",
          title: "Run the model pass",
          lines: ["Scoped to 2 selected companies", "No model is called."],
          fields: { company_ids: PICKED.join(","), max_companies: "500", concurrency: "2" },
        },
        launched: null,
      },
    });
    expect(html).toContain("Run the model pass — confirm");
    expect(html).toContain('name="intent" value="launch-model-pass"');
    expect(html).toContain('name="company_ids" value="5560125220,5565200028"');
    expect(html).toContain('name="max_companies" value="500"');
    expect(html).toContain("Nothing has been launched yet.");
  });

  it("shows a launched run, and a refusal, without losing the numbers", () => {
    const launched = renderPanel({
      result: {
        kind: "result",
        ok: true,
        error: "",
        confirmation: null,
        launched: {
          runId: "run-1",
          url: "https://dagster.example/runs/run-1",
          job: "se_company_info_review_job",
        },
      },
    });
    expect(launched).toContain("Launched on se_company_info_review_job");
    expect(launched).toContain('href="https://dagster.example/runs/run-1"');
    // The view is still on screen: one fetcher carries both answers, and the
    // action's reply must not blank the sheet.
    expect(launched).toContain("1,240");

    const refused = renderPanel({
      result: {
        kind: "result",
        ok: false,
        error: "Dagster refused: no such job",
        confirmation: null,
        launched: null,
      },
    });
    expect(refused).toContain("That did not run");
    expect(refused).toContain("Dagster refused: no such job");
    expect(refused).toContain("1,240");
  });

  it("says what it does not know: no counts, no Dagster", () => {
    const html = renderPanel({
      view: {
        ...VIEW,
        stats: null,
        statsError: "ClickHouse: ILLEGAL_AGGREGATION",
        dagsterError: "connect ECONNREFUSED",
      },
    });
    expect(html).toContain("Selection counts unavailable");
    expect(html).toContain("ClickHouse: ILLEGAL_AGGREGATION");
    expect(html).toContain("Dagster is unreachable");
    // The actions are still there -- the action itself refuses to confirm
    // without counts, which is where that guard belongs.
    expect(html).toContain("Review…");
  });

  it("renders the scope and nothing else until the fetcher has answered", () => {
    // What the sheet shows in the moment after it is opened: no numbers have
    // been read yet, and none are invented.
    const html = renderPanel({ view: null, loading: true, selectedIds: PICKED });
    expect(html).toContain("2 selected companies");
    expect(html).toContain("Reading the change scan…");
    expect(html).not.toContain("Re-resolve changed companies");
    expect(html).not.toContain("1,240");
  });
});

describe("one opening of the sheet never shows the last one's answers", () => {
  const CONFIRMED: PipelineResult = {
    kind: "result",
    ok: true,
    error: "",
    confirmation: {
      intent: "launch-resolve",
      title: "Re-resolve changed companies",
      lines: ["Scoped to 2 selected companies."],
      fields: { company_ids: PICKED.join(","), max_companies: "1000" },
    },
    launched: null,
  };

  it("drops the previous opening's confirmation the moment the sheet reopens", () => {
    // The sequence that would otherwise launch a paid run over the wrong
    // companies: pick A and B, Review, close, pick C and D, reopen. For the
    // seconds the change scan takes, the old confirmation would still be on
    // screen with a live Launch button, under a scope block already saying
    // "2 selected companies" -- about C and D.
    const fetcher = { reset: vi.fn() };
    let opening = withFetched(NOTHING_FETCHED, VIEW);
    opening = withFetched(opening, CONFIRMED);
    // First opening: the launch is live and belongs to A and B.
    expect(renderPanel({ ...opening, selectedIds: PICKED })).toContain("Launch this run");

    // ... close, re-pick, reopen. This is the sheet's own open handler.
    opening = openSheet(fetcher);
    expect(fetcher.reset).toHaveBeenCalledTimes(1);
    expect(opening).toEqual(NOTHING_FETCHED);

    const reopened = renderPanel({
      ...opening,
      selectedIds: ["5567890123", "5569999999"],
      loading: true,
    });
    expect(reopened).not.toContain("Launch this run");
    expect(reopened).not.toContain("Re-resolve changed companies — confirm");
    // ... and it says what it is doing instead, for the new picks.
    expect(reopened).toContain("Reading the change scan…");
    expect(reopened).toContain("2 selected companies");
  });

  it("keeps a confirmation beside the numbers, but never past a fresh scan", () => {
    // One fetcher carries both answers: a confirmation must not blank the
    // counts it is about ...
    const withResult = withFetched(withFetched(NOTHING_FETCHED, VIEW), CONFIRMED);
    expect(withResult.view).toBe(VIEW);
    expect(withResult.result).toBe(CONFIRMED);
    // ... and a view loaded afterwards supersedes it: those numbers are newer
    // than the confirmation, so the confirmation is no longer what they mean.
    expect(withFetched(withResult, VIEW).result).toBeNull();
    // Nothing fetched changes nothing.
    expect(withFetched(withResult, null)).toBe(withResult);
    expect(withFetched(NOTHING_FETCHED, undefined)).toBe(NOTHING_FETCHED);
  });
});

describe("the companies list pays nothing for the pipeline", () => {
  beforeEach(() => {
    clickhouse.query.mockClear();
    pipelineStats.load.mockClear();
  });

  it("runs no change-scan query in the LIST loader", async () => {
    const { loader } = await import("~/routes/admin-se-companies-info");
    await loader({
      request: new Request("http://backoffice/admin/se/companies"),
      params: {},
      context: {},
    } as unknown as Parameters<typeof loader>[0]);

    // The stats loader is the whole change scan; the sheet's fetcher calls it,
    // this loader may not.
    expect(pipelineStats.load).not.toHaveBeenCalled();
    expect(clickhouse.query).toHaveBeenCalled();
    const sql = clickhouse.query.mock.calls.map(([statement]) => String(statement)).join("\n");
    // The scan's own fingerprints: the observation table, the pending-model
    // predicate and the per-source freshness maxIf. None of them belongs to a
    // page of companies.
    expect(sql).not.toContain("se_company_info_enrichment_observation");
    expect(sql).not.toContain("pending_model");
    expect(sql).not.toContain("maxIf(source_observed_at");
  });
});

describe("the standalone Pipeline page is retired", () => {
  it("is gone from the Sweden sidebar", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/admin/se/companies"]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );
    // The tabbed list it folded into is still there ...
    expect(html).toContain('href="/admin/se/companies"');
    // ... and the page the pipeline used to be is not in the sidebar.
    expect(html).not.toContain('href="/admin/se/companies/pipeline"');
    expect(html).not.toContain(">Pipeline<");
  });

  it("has no breadcrumb branch left at the old URL", () => {
    const router = createMemoryRouter(
      [{ path: "*", element: <AdminLayout /> }],
      { initialEntries: ["/admin/se/companies/pipeline"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    expect(html).not.toContain(">Pipeline<");
    expect(html).not.toContain('href="/admin/se/companies/pipeline"');
  });
});
