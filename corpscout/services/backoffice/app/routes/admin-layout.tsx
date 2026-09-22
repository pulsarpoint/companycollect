import { Link, Outlet, useLocation, useRouteLoaderData } from "react-router";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "~/components/ui/breadcrumb";
import { Separator } from "~/components/ui/separator";
import {
  SidebarInset,
  SidebarProvider,
  SidebarTrigger,
} from "~/components/ui/sidebar";
// Type-only: erased at build, so the company layout's shell shape is shared
// here without pulling its ClickHouse module into the client bundle.
import type { SeCompanyShell } from "~/lib/se-company-shell.server";
import {
  seCompanyIdFromPath,
  seCompanyTabFromPath,
  seCompanyTabLabel,
} from "~/lib/se-company-tabs";
import {
  seCompaniesTabFromPath,
  seCompaniesTabLabel,
  seCompaniesTabPath,
} from "~/lib/se-companies-tabs";

/**
 * The company crumb reads the company layout's own loader data rather than
 * re-querying: the breadcrumbs render above the company route tree, where no
 * loader data reaches them as props. An id whose layout has not resolved
 * (or that 404s) falls back to the id itself, which is still a true label.
 */
function useSeCompanyLabel(companyId: string): string {
  const data = useRouteLoaderData("routes/admin-se-company-layout") as
    | { shell: SeCompanyShell | null }
    | undefined;
  return data?.shell?.legal_name ?? companyId;
}

/**
 * The technology crumb reads the detail route's own loader data (same
 * reasoning as `useSeCompanyLabel`); a slug whose loader has not resolved
 * (or that 404s) falls back to the decoded slug, still a true label.
 */
function useTechnologyLabel(pathname: string): string {
  const data = useRouteLoaderData("routes/admin-technology-detail") as
    | { technology: { technology: string } }
    | undefined;
  const slug = pathname.slice("/admin/technologies/".length).split("/")[0] ?? "";
  return data?.technology.technology ?? decodeURIComponent(slug);
}

function useCommonCrawlDomain(pathname: string): string {
  const data = useRouteLoaderData("routes/admin-common-crawl-domain") as
    | { domain: string }
    | undefined;
  return (
    data?.domain ??
    (pathname.startsWith("/admin/common-crawl/")
      ? pathname.slice("/admin/common-crawl/".length)
      : "")
  );
}

function AdminBreadcrumbs() {
  const { pathname } = useLocation();
  const companyId = seCompanyIdFromPath(pathname);
  const companyLabel = useSeCompanyLabel(companyId);
  const technologyLabel = useTechnologyLabel(pathname);
  const commonCrawlDomain = useCommonCrawlDomain(pathname);
  const onCommonCrawlIndexPage = pathname === "/admin/common-crawl";
  const onCommonCrawlDetailPage = pathname.startsWith("/admin/common-crawl/");
  const onTechnologiesIndexPage = pathname === "/admin/technologies";
  const onTechnologyDetailPage = pathname.startsWith("/admin/technologies/");
  const onGeneralRolesPage = pathname === "/admin/general/roles";
  const onDomainPromptsPage = pathname === "/admin/settings/domain-prompts";
  const onPeoplePromptsPage = pathname === "/admin/settings/people-prompts";
  const onLlmSettingsPage = pathname === "/admin/settings/llms" || onPeoplePromptsPage || onDomainPromptsPage;
  const onEsefPage = pathname === "/admin/esef";
  const onCompanyInfoPage = pathname.startsWith("/admin/se/company/");
  const onCompaniesPage =
    pathname === "/admin/se/companies" ||
    pathname.startsWith("/admin/se/companies/");
  const onPeoplePage = pathname === "/admin/se/people";

  const onNatsPage = pathname === "/admin/nats";

  if (onEsefPage || onNatsPage || pathname === "/admin/graph" || pathname.startsWith("/admin/crawls") || pathname.startsWith("/admin/browsers")) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbPage>{onEsefPage ? "ESEF" : onNatsPage ? "NATS" : pathname.startsWith("/admin/crawls") ? "Crawler" : pathname.startsWith("/admin/browsers") ? "Browsers" : "Graph"}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onCommonCrawlIndexPage || onCommonCrawlDetailPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            {onCommonCrawlIndexPage ? (
              <BreadcrumbPage>Common Crawl</BreadcrumbPage>
            ) : (
              <BreadcrumbLink render={<Link to="/admin/common-crawl" />}>
                Common Crawl
              </BreadcrumbLink>
            )}
          </BreadcrumbItem>
          {onCommonCrawlDetailPage ? (
            <>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>{commonCrawlDomain}</BreadcrumbPage>
              </BreadcrumbItem>
            </>
          ) : null}
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (pathname === "/admin/technology-proposals" || pathname.startsWith("/admin/technology-proposals/")) {
    return <Breadcrumb><BreadcrumbList>
      <BreadcrumbItem><BreadcrumbLink render={<Link to="/admin/technologies" />}>Technologies</BreadcrumbLink></BreadcrumbItem>
      <BreadcrumbSeparator />
      <BreadcrumbItem>{pathname === "/admin/technology-proposals"
        ? <BreadcrumbPage>Proposals</BreadcrumbPage>
        : <BreadcrumbLink render={<Link to="/admin/technology-proposals" />}>Proposals</BreadcrumbLink>}
      </BreadcrumbItem>
      {pathname !== "/admin/technology-proposals" && <><BreadcrumbSeparator /><BreadcrumbItem><BreadcrumbPage>Review</BreadcrumbPage></BreadcrumbItem></>}
    </BreadcrumbList></Breadcrumb>;
  }

  if (onTechnologiesIndexPage || onTechnologyDetailPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            {onTechnologiesIndexPage ? (
              <BreadcrumbPage>Technologies</BreadcrumbPage>
            ) : (
              <BreadcrumbLink render={<Link to="/admin/technologies" />}>
                Technologies
              </BreadcrumbLink>
            )}
          </BreadcrumbItem>
          {onTechnologyDetailPage ? (
            <>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>{technologyLabel}</BreadcrumbPage>
              </BreadcrumbItem>
            </>
          ) : null}
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onCompaniesPage) {
    // Companies > <Tab>. Info is the section index, so it is the leaf on the
    // bare root; every other tab shows Companies as a link back to Info.
    const tab = seCompaniesTabFromPath(pathname);
    // Domains > <domain>: the one tab with its own detail pages
    // (/admin/se/companies/domains/<domain>), so the tab becomes a link back.
    const domainLeaf =
      tab === "domains" ? decodeURIComponent(pathname.split("/")[5] ?? "") : "";
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbPage>Sweden</BreadcrumbPage>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            {tab === "info" ? (
              <BreadcrumbPage>Companies</BreadcrumbPage>
            ) : (
              <BreadcrumbLink render={<Link to="/admin/se/companies" />}>
                Companies
              </BreadcrumbLink>
            )}
          </BreadcrumbItem>
          {tab === "info" ? null : (
            <>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                {domainLeaf === "" ? (
                  <BreadcrumbPage>{seCompaniesTabLabel(tab)}</BreadcrumbPage>
                ) : (
                  <BreadcrumbLink render={<Link to={seCompaniesTabPath(tab)} />}>
                    {seCompaniesTabLabel(tab)}
                  </BreadcrumbLink>
                )}
              </BreadcrumbItem>
            </>
          )}
          {domainLeaf === "" ? null : (
            <>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage className="font-mono">{domainLeaf}</BreadcrumbPage>
              </BreadcrumbItem>
            </>
          )}
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onPeoplePage || pathname === "/admin/se/processing") {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbPage>Sweden</BreadcrumbPage>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbPage>{onPeoplePage ? "People" : "Processing"}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onCompanyInfoPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden md:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden md:block" />
          <BreadcrumbItem className="hidden md:block">
            <BreadcrumbPage>Sweden</BreadcrumbPage>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden md:block" />
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin/se/companies" />}>
              Companies
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbLink
              render={
                <Link
                  to={`/admin/se/company/${encodeURIComponent(companyId)}/info`}
                />
              }
            >
              {companyLabel}
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>
              {seCompanyTabLabel(seCompanyTabFromPath(pathname))}
            </BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onLlmSettingsPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbPage>Settings</BreadcrumbPage>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>{onDomainPromptsPage ? "Domain prompts" : onPeoplePromptsPage ? "People prompts" : "LLMs"}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onGeneralRolesPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbPage>General</BreadcrumbPage>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>Roles</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbPage>Admin</BreadcrumbPage>
        </BreadcrumbItem>
      </BreadcrumbList>
    </Breadcrumb>
  );
}

export default function AdminLayout() {
  return (
    <SidebarProvider>
      <AdminSidebar />
      <SidebarInset>
        <header className="flex h-16 shrink-0 items-center gap-2 border-b px-4 transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-12">
          <SidebarTrigger className="-ml-1" />
          <Separator orientation="vertical" className="mr-2 h-4" />
          <AdminBreadcrumbs />
        </header>
        <Outlet />
      </SidebarInset>
    </SidebarProvider>
  );
}
