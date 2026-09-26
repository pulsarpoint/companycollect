import { Link } from "react-router";
import type { Route } from "./+types/admin-brave-results";
import { BraveResults } from "~/components/admin/brave-results";
import { Button } from "~/components/ui/button";
import { braveTaskResultsPath } from "~/lib/brave-results";
import { loadBraveResults } from "~/lib/brave-results.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  return { results: await loadBraveResults({ taskId: params.taskId }, new URL(request.url).searchParams) };
}

export function meta() { return [{ title: "Brave task results | CompanyCollect" }]; }

export default function AdminBraveResults({ loaderData, params }: Route.ComponentProps) {
  return <div className="flex flex-col gap-6 p-4 md:p-6">
    <header className="flex flex-wrap items-start justify-between gap-3">
      <div className="flex flex-col gap-1"><h1 className="text-2xl font-semibold">Brave task results</h1><p className="break-all text-sm text-muted-foreground">Task {params.taskId}</p></div>
      <Button variant="outline" nativeButton={false} render={<Link to="/admin/queues/brave" />}>Back to Brave queue</Button>
    </header>
    <BraveResults results={loaderData.results} basePath={braveTaskResultsPath(params.taskId)} showCompany />
  </div>;
}
