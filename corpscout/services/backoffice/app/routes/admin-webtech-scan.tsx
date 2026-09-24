import { Link } from "react-router";
import type { Route } from "./+types/admin-webtech-scan";
import { WebtechSection } from "~/components/detail/webtech-section";
import { parseWebtechScanIdentity, webtechDomainPath, webtechListPath } from "~/lib/webtech";
import { getWebtechScan } from "~/lib/webtech.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const domain = params.domain.trim().toLowerCase();
  const identity = parseWebtechScanIdentity(new URL(request.url).searchParams);
  if (!domain || domain.length > 253 || !identity) throw new Response("Scan not found", { status: 404 });
  const data = await getWebtechScan(domain, identity);
  if (!data) throw new Response("Scan not found", { status: 404 });
  return data;
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [{ title: `${loaderData?.domain ?? "Domain"} · Webtech scan | CompanyCollect` }];
}

export default function AdminWebtechScan({ loaderData }: Route.ComponentProps) {
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <Link to={webtechListPath(webtechDomainPath(loaderData.domain), "history")} className="w-fit text-sm underline underline-offset-2">Back to domain scan history</Link>
        <h1 className="break-all font-mono text-2xl font-semibold">{loaderData.domain}</h1>
        <p className="text-sm text-muted-foreground">Recorded Webtech scan. Browse latest results to see the current detections for each page.</p>
        <Link to={webtechDomainPath(loaderData.domain)} className="w-fit text-sm underline underline-offset-2">Latest results</Link>
      </header>
      <WebtechSection data={loaderData} historical linkTechnologies />
    </div>
  );
}
