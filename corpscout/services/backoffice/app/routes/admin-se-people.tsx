import { Link } from "react-router";
import type { Route } from "./+types/admin-se-people";
import {
  SePeopleSimpleSyncSheet,
  type SimpleSyncActionResult,
} from "~/components/admin/se-people-actions";
import { SePeopleSourcesTable } from "~/components/admin/se-people-sources-table";
import {
  DagsterError,
  dagsterRunUrl,
  launchRun,
  SE_COMPANY_PERSON_JOB,
} from "~/lib/dagster.server";
import { buildSimpleSyncRunConfig } from "~/lib/se-company-person-pipeline.server";
import {
  DEFAULT_COMPANY_BATCH_SIZE,
  DEFAULT_MAX_COMPANIES,
  PILOT_TAG_KEY,
  PILOT_TAG_VALUE,
} from "~/lib/se-company-person-pipeline";
import { loadSimpleSyncPreview } from "~/lib/se-people-simple-sync.server";
import {
  parseSePeopleSourceFilters,
  parseSePeopleSourceTab,
  parseSePeopleSourceView,
  sePeopleSourcesSearch,
} from "~/lib/se-people-sources";
import { loadSePeopleSourcePage } from "~/lib/se-people-sources.server";

// The old Draft 1/Draft 2 SQLite/DuckDB/Temporal curation workspace this
// route used to host is retired, superseded by the ClickHouse company-person
// model (se/people/pipeline, se/people/person/:id, se/people/stale-
// corrections). The route stays -- it is still the sidebar's "People" entry
// and the breadcrumb "Admin"/"People" link every other admin page points at
// (see admin-layout.tsx's AdminBreadcrumbs) -- but its own content is now a
// tabbed browser over the three SE person SOURCE views, the resolved
// se_company_person table, and a Tasks tab of every people asset/job's
// latest run, so a reviewer can see what each source actually published (and
// what has run) without leaving this page. The Actions menu's "Simple sync"
// sheet previews and launches the COMBINED role_draft+person+role cascade
// (`SE_COMPANY_PERSON_JOB` + `buildSimpleSyncRunConfig`,
// `se-company-person-pipeline.server.ts`) -- deliberately NOT the pipeline
// page's own clean-copy launch (`SE_COMPANY_PERSON_PUBLISH_JOB` +
// `buildCleanCopyRunConfig`, person table only, untouched by this route):
// Simple Sync's one click needs to publish both people AND their role
// assignments.

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const tab = parseSePeopleSourceTab(url);
  const filters = parseSePeopleSourceFilters(url);
  const view = parseSePeopleSourceView(url);
  const page = await loadSePeopleSourcePage(tab, filters, view.page, view.pageSize);
  return { page, filters, view };
}

export async function action({ request }: Route.ActionArgs): Promise<SimpleSyncActionResult> {
  const form = await request.formData();
  const intent = form.get("intent");

  try {
    if (intent === "confirm-simple-sync") {
      const preview = await loadSimpleSyncPreview();
      return { kind: "preview", preview };
    }
    if (intent === "launch-simple-sync") {
      const run = await launchRun({
        job: SE_COMPANY_PERSON_JOB,
        runConfig: buildSimpleSyncRunConfig({
          companyIds: [],
          maxCompanies: DEFAULT_MAX_COMPANIES,
          companyBatchSize: DEFAULT_COMPANY_BATCH_SIZE,
        }),
        tags: { [PILOT_TAG_KEY]: PILOT_TAG_VALUE },
      });
      return {
        kind: "launched",
        runId: run.runId,
        url: dagsterRunUrl(run.runId),
        job: SE_COMPANY_PERSON_JOB,
      };
    }
    return { kind: "error", error: "Unknown action." };
  } catch (error) {
    if (error instanceof DagsterError) {
      return { kind: "error", error: error.message };
    }
    throw error;
  }
}

export function meta() {
  return [{ title: "Sweden people | CompanyCollect" }];
}

export default function AdminSwedenPeople({ loaderData }: Route.ComponentProps) {
  const { page, filters, view } = loaderData;
  const tasksHref = sePeopleSourcesSearch(new URLSearchParams(), { tab: "tasks" });
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-3">
        <div className="flex items-center justify-between gap-4">
          <h1 className="text-2xl font-semibold tracking-tight">Sweden people</h1>
          <SePeopleSimpleSyncSheet tasksHref={tasksHref} />
        </div>
        <p className="text-sm text-muted-foreground">
          The three raw person source reads (Bolagsverket, ESEF, Wikidata) and
          the resolved <code>se_company_person</code> table they feed. To run
          normalization, resolution or merge suggestions, use{" "}
          <Link to="/admin/se/people/pipeline" className="underline underline-offset-2">
            the pipeline page
          </Link>
          .
        </p>
      </header>
      <SePeopleSourcesTable page={page} filters={filters} view={view} />
    </div>
  );
}
