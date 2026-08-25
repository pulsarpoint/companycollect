import type { Route } from "./+types/admin-se-company-info-geocoding";
import { GeocodeAnalysisAgent } from "~/components/admin/geocode-analysis-agent";
import { SeCompanyGeocodingTable } from "~/components/admin/se-company-geocoding-table";
import { parseListView } from "~/lib/se-company-info-filters";
import {
  GEOCODE_STATUS_PARAM,
  parseGeocodeListFilter,
} from "~/lib/se-company-geocoding-filters";
import {
  countForFilter,
  listSeCompanyGeocodingPage,
  loadSeCompanyGeocodingCounts,
} from "~/lib/se-company-geocoding-list.server";
import {
  geocodeAgentCountry,
  loadGeocodeAgentPanel,
  startGeocodeAnalysisRun,
  UnsupportedGeocodeAgentCountryError,
} from "~/agents/geocode-analysis.server";
import {
  GeocodeAgentRunActiveError,
  setGeocodeAgentSuggestionStatus,
} from "~/lib/geocode-agent-store.server";
import { GEOCODE_AGENT_SUGGESTION_STATUSES } from "~/agents/geocode-analysis-contract";
import type { GeocodeAgentSuggestionStatus } from "~/agents/geocode-analysis-contract";

// Only server-only exports (`loader`, `action`), `meta` and the component live
// here, same discipline as admin-se-company-info-table.tsx: any OTHER export
// touching `~/lib/*.server` or `~/agents/*.server` would keep that module in
// the client bundle and break the production build (see CLAUDE.md).
// `parseListView` is reused rather than re-spelled -- it already clamps
// page/pageSize the same way every other admin list does.

/** This tab is Sweden's; the agent itself is country-parametrised. */
const COUNTRY_CODE = "SE";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filter = parseGeocodeListFilter(url.searchParams.get(GEOCODE_STATUS_PARAM));
  const view = parseListView(url);

  const [listPage, counts, agent] = await Promise.all([
    listSeCompanyGeocodingPage({ filter, page: view.page, pageSize: view.pageSize }),
    // One scan of `published` covers the strip's six numbers AND this page's
    // own pagination total (countForFilter) -- no separate count() query.
    loadSeCompanyGeocodingCounts(),
    // Never throws: an unconfigured or unreachable review-queue database
    // leaves the list intact and explains itself inside the agent panel.
    loadGeocodeAgentPanel(COUNTRY_CODE),
  ]);

  return {
    listPage,
    counts,
    total: countForFilter(counts, filter),
    page: view.page,
    pageSize: view.pageSize,
    filter,
    agent,
  };
}

function isSuggestionStatus(value: string): value is GeocodeAgentSuggestionStatus {
  return (GEOCODE_AGENT_SUGGESTION_STATUSES as readonly string[]).includes(value);
}

/**
 * Two intents, both of them thin: start a run (which returns the moment the
 * queued row exists -- the agent keeps working in the background) and record a
 * reviewer's decision on one suggestion.
 *
 * Nothing here writes ClickHouse or the geocode store: an accepted suggestion
 * is a work item for a golden-gated Dagster policy bump, not a serving change.
 */
export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const intent = String(form.get("intent") ?? "");

  try {
    if (intent === "start_geocode_analysis") {
      // Validated before anything is inserted: this action is reachable by
      // anyone who can reach the admin area, and an unwired country would
      // otherwise accumulate run rows (or surface the CHECK constraint's raw
      // text) for a run that could never do any work.
      const requested = String(form.get("country") ?? COUNTRY_CODE)
        .trim()
        .toUpperCase();
      const profile = geocodeAgentCountry(requested);
      if (!profile) {
        return {
          ok: false as const,
          error: `No geocode analysis profile is wired for "${requested}".`,
        };
      }
      const run = await startGeocodeAnalysisRun({
        countryCode: profile.countryCode,
        focus: String(form.get("focus") ?? ""),
      });
      return { ok: true as const, intent: "start" as const, run };
    }

    if (intent === "set_geocode_suggestion_status") {
      const id = String(form.get("suggestion_id") ?? "").trim();
      const status = String(form.get("status") ?? "");
      if (id === "" || !isSuggestionStatus(status)) {
        return { ok: false as const, error: "Unknown suggestion or status." };
      }
      const suggestion = await setGeocodeAgentSuggestionStatus(id, status, {
        decidedBy: process.env.GEOCODE_AGENT_REVIEWER?.trim() ?? "",
        // Only the "mark implemented" form sends one; an empty value leaves
        // whatever version is already recorded untouched (see the store).
        policyVersion: String(form.get("policy_version") ?? "")
          .trim()
          .slice(0, 64),
      });
      if (!suggestion) {
        return { ok: false as const, error: "That suggestion no longer exists." };
      }
      return { ok: true as const, intent: "decide" as const, suggestion };
    }

    return { ok: false as const, error: "Unknown geocoding action." };
  } catch (error) {
    if (error instanceof UnsupportedGeocodeAgentCountryError) {
      return { ok: false as const, error: error.message };
    }
    if (error instanceof GeocodeAgentRunActiveError) {
      return {
        ok: false as const,
        error: "An analysis run is already active for this country. Wait for it to finish.",
      };
    }
    return {
      ok: false as const,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export function meta() {
  return [{ title: "Geocoding | CompanyCollect" }];
}

export default function AdminSeCompanyInfoGeocoding({
  loaderData,
}: Route.ComponentProps) {
  const { listPage, counts, total, page, pageSize, filter, agent } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">Geocoding</h1>
        <p className="text-sm text-muted-foreground">
          Every Swedish company with a published address (se_company_address),
          and that address's own geocode outcome. Defaults to companies that
          need attention: an address with no successful geocode mapping.
        </p>
      </header>
      <GeocodeAnalysisAgent panel={agent} />
      <SeCompanyGeocodingTable
        rows={listPage.rows}
        total={total}
        page={page}
        pageSize={pageSize}
        filter={filter}
        counts={counts}
      />
    </div>
  );
}
