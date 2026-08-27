/**
 * The redesigned Simple Sync sheet content: headline stats, per-source
 * breakdown (zero-count sources muted, not hidden), the sample Table, and the
 * launched success panel. These are exported pure components
 * (se-people-actions.tsx), so each is rendered directly with fixed props via
 * `renderToStaticMarkup` -- the same static-HTML-string convention
 * esef-operations-workspace.test.tsx uses -- rather than driving the stateful
 * `SePeopleSimpleSyncSheet` open (this project has no jsdom/testing-library,
 * see vitest.config.ts: no `environment` set, so there is no DOM to click
 * into). The route action's own job/run-config contract (which job launches,
 * what the run config carries) is covered by admin-se-people.test.ts and
 * se-company-person-pipeline.server.test.ts -- not re-asserted here.
 */
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import {
  LaunchedPanel,
  PreviewBody,
  SampleTable,
  type SimpleSyncActionResult,
  type SimpleSyncPreviewView,
} from "~/components/admin/se-people-actions";

const PREVIEW: SimpleSyncPreviewView = {
  companyCount: 1234,
  personCount: 5678,
  bySource: [
    { source: "bolagsverket", companyCount: 1000, personCount: 4500 },
    { source: "esef", companyCount: 234, personCount: 1178 },
    { source: "wikidata", companyCount: 0, personCount: 0 },
  ],
  sample: [
    { name: "Anna Svensson", companyId: "5560125220", source: "bolagsverket" },
    { name: "Erik Karlsson", companyId: "5565200028", source: "esef" },
  ],
  sampleSize: 20,
};

function renderWithRouter(element: React.ReactElement): string {
  const router = createMemoryRouter(
    [{ path: "*", element }],
    { initialEntries: ["/admin/se/people"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("PreviewBody", () => {
  it("shows the two headline stat cards with big tabular numbers", () => {
    const html = renderWithRouter(<PreviewBody preview={PREVIEW} />);

    expect(html).toContain("Companies to sync");
    expect(html).toContain("1,234");
    expect(html).toContain("People to sync");
    expect(html).toContain("5,678");
    expect(html).toContain("tabular-nums");
  });

  it("lists all three sources, even the zero-count one -- muted, not dropped", () => {
    const html = renderWithRouter(<PreviewBody preview={PREVIEW} />);

    expect(html).toContain("Bolagsverket");
    expect(html).toContain("ESEF");
    expect(html).toContain("Wikidata");
    // Wikidata's row (0 companies/0 people) still renders its counts and gets
    // the muted treatment other non-zero rows don't.
    const wikidataRowIndex = html.indexOf("Wikidata");
    const surrounding = html.slice(Math.max(0, wikidataRowIndex - 400), wikidataRowIndex + 50);
    expect(surrounding).toContain("text-muted-foreground");
  });

  it("renders the sample table with mono company-id links and source badges", () => {
    const html = renderWithRouter(<PreviewBody preview={PREVIEW} />);

    expect(html).toContain("Anna Svensson");
    expect(html).toContain('href="/company/SE/5560125220"');
    expect(html).toContain("5560125220");
    expect(html).toContain("Erik Karlsson");
    expect(html).toContain('href="/company/SE/5565200028"');
  });

  it("captions the sample with the count and the true total it stands in for", () => {
    const html = renderWithRouter(<PreviewBody preview={PREVIEW} />);

    expect(html).toContain("Sample of 20");
    expect(html).toContain("5,678 people");
  });
});

describe("SampleTable", () => {
  it("says nothing is pending when the sample is empty, instead of an empty table", () => {
    const html = renderWithRouter(
      <SampleTable sample={[]} sampleSize={20} personCount={0} />,
    );

    expect(html).toContain("Nothing pending");
    expect(html).not.toContain("Sample of 20");
  });
});

describe("LaunchedPanel", () => {
  const LAUNCHED: Extract<SimpleSyncActionResult, { kind: "launched" }> = {
    kind: "launched",
    runId: "run-abc123",
    url: "https://dagster.example.com/runs/run-abc123",
    job: "se_company_person_job",
  };

  it("names the bundled job and states it publishes both people and role assignments", () => {
    const html = renderWithRouter(
      <LaunchedPanel result={LAUNCHED} tasksHref="/admin/se/people?tab=tasks" />,
    );

    expect(html).toContain("se_company_person_job");
    expect(html).toContain("run-abc123");
    expect(html).toContain("people and their role assignments");
  });

  it("links to the Tasks tab and, when present, the Dagster run", () => {
    const html = renderWithRouter(
      <LaunchedPanel result={LAUNCHED} tasksHref="/admin/se/people?tab=tasks" />,
    );

    expect(html).toContain('href="/admin/se/people?tab=tasks"');
    expect(html).toContain("Tasks tab");
    expect(html).toContain('href="https://dagster.example.com/runs/run-abc123"');
    expect(html).toContain("open the run in Dagster");
  });

  it("omits the Dagster link when no run url is known", () => {
    const html = renderWithRouter(
      <LaunchedPanel result={{ ...LAUNCHED, url: null }} tasksHref="/admin/se/people?tab=tasks" />,
    );

    expect(html).not.toContain("open the run in Dagster");
  });
});
