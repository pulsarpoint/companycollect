import { Link, useNavigation } from "react-router";
import type { Route } from "./+types/admin-webtech-domain";
import { WebtechScansTable, WebtechViewTabs } from "~/components/admin/webtech-scans-table";
import { WebtechSection } from "~/components/detail/webtech-section";
import { parseWebtechSearch, webtechDomainPath } from "~/lib/webtech";
import { getDomainWebtech, listWebtechScans } from "~/lib/webtech.server";

export async function loader({ params, request }: Route.LoaderArgs) {
  const domain = params.domain.trim().toLowerCase();
  if (!domain || domain.length > 253) throw new Response("Domain not found", { status: 404 });
  const { view, page } = parseWebtechSearch(new URL(request.url).searchParams);
  return {
    domain, view, page,
    latest: view === "latest" ? await getDomainWebtech(domain) : null,
    history: view === "history" ? await listWebtechScans({ domain, view, page }) : null,
  };
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [{ title: `${loaderData?.domain ?? "Domain"} · Webtech | CompanyCollect` }];
}

export default function AdminWebtechDomain({ loaderData }: Route.ComponentProps) {
  const { domain, view, page, latest, history } = loaderData;
  const busy = useNavigation().state !== "idle";
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6" aria-busy={busy}>
      <header className="flex flex-col gap-2">
        <Link to="/admin/webtech" className="w-fit text-sm underline underline-offset-2">Back to Webtech</Link>
        <h1 className="break-all font-mono text-2xl font-semibold">{domain}</h1>
        <p className="text-sm text-muted-foreground">Webtech detections by requested page, with each scan retained in history.</p>
      </header>
      <WebtechViewTabs basePath={webtechDomainPath(domain)} view={view} />
      {latest ? <WebtechSection data={latest} linkTechnologies /> : null}
      {history ? <WebtechScansTable result={history} basePath={webtechDomainPath(domain)} view={view} page={page} /> : null}
    </div>
  );
}
