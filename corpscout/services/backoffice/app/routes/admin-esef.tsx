import type { Route } from "./+types/admin-esef";
import { EsefOperationsWorkspace } from "~/components/admin/esef-operations-workspace";
import { dagsterRunUrl } from "~/lib/dagster.server";
import { loadEsefOperationsStatus } from "~/lib/esef-operations.server";

export async function loader(_: Route.LoaderArgs) {
  try {
    const status = await loadEsefOperationsStatus();
    return {
      status,
      error: "",
      runUrls: Object.fromEntries(
        status.recentEnrichmentRuns.flatMap((run) => {
          const url = dagsterRunUrl(run.runId);
          return url ? [[run.runId, url]] : [];
        }),
      ),
    };
  } catch (error) {
    return {
      status: null,
      error:
        error instanceof Error
          ? error.message
          : "Dagster status could not be loaded.",
      runUrls: {},
    };
  }
}

export function meta() {
  return [{ title: "ESEF processing | CompanyCollect" }];
}

export default function AdminEsef({ loaderData }: Route.ComponentProps) {
  return <EsefOperationsWorkspace {...loaderData} />;
}
