import type { Route } from "./+types/admin-se-company-jobs";
import { SeCompanyJobsTab } from "~/components/admin/se-company-jobs";
import {
  loadSeCompanyJobAdDetail,
  loadSeCompanyJobs,
} from "~/lib/se-company-jobs.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params, request }: Route.LoaderArgs) {
  // `?ad=` selects one ad for the detail card. The loader never trusts the
  // raw value: loadSeCompanyJobAdDetail reads it keyed by THIS company and
  // returns null for an id the company does not own, so a foreign or
  // malformed param just renders the plain list -- no 404, no leak.
  const adId = new URL(request.url).searchParams.get("ad")?.trim() ?? "";
  const [jobs, adDetail] = await Promise.all([
    loadSeCompanyJobs(params.companyId),
    adId === ""
      ? Promise.resolve(null)
      : loadSeCompanyJobAdDetail(params.companyId, adId),
  ]);
  return { jobs, adDetail };
}

export default function AdminSwedenCompanyJobs({
  loaderData,
}: Route.ComponentProps) {
  return (
    <SeCompanyJobsTab jobs={loaderData.jobs} adDetail={loaderData.adDetail} />
  );
}
