import type { Route } from "./+types/admin-se-company-jobs";
import { SeCompanyJobsTab } from "~/components/admin/se-company-jobs";
import { loadSeCompanyJobs } from "~/lib/se-company-jobs.server";

// Only `loader`, `meta` and the component live here -- see
// admin-se-company-layout.tsx for why.

export async function loader({ params }: Route.LoaderArgs) {
  return { jobs: await loadSeCompanyJobs(params.companyId) };
}

export default function AdminSwedenCompanyJobs({
  loaderData,
}: Route.ComponentProps) {
  return <SeCompanyJobsTab jobs={loaderData.jobs} />;
}
