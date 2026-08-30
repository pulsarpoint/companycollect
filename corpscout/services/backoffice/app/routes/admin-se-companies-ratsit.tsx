import type { Route } from "./+types/admin-se-companies-ratsit";
import {
  SeRatsitRequestInspector,
  SeRatsitRequestList,
} from "~/components/admin/se-ratsit-request-browser";
import { parseListView } from "~/lib/se-company-info-filters";
import {
  loadSeRatsitRequestDetail,
  listSeRatsitRequests,
} from "~/lib/se-ratsit-results.server";
import { parseSeRatsitRequestSelection } from "~/lib/se-ratsit-results";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const selection = parseSeRatsitRequestSelection(url);
  if (selection) {
    return {
      mode: "detail" as const,
      detail: await loadSeRatsitRequestDetail(selection),
    };
  }

  const view = parseListView(url);
  return {
    mode: "list" as const,
    page: await listSeRatsitRequests(view),
  };
}

export function meta() {
  return [{ title: "Companies · Ratsit | CompanyCollect" }];
}

export default function AdminSeCompaniesRatsit({
  loaderData,
}: Route.ComponentProps) {
  return loaderData.mode === "detail" ? (
    <SeRatsitRequestInspector detail={loaderData.detail} />
  ) : (
    <SeRatsitRequestList page={loaderData.page} />
  );
}
