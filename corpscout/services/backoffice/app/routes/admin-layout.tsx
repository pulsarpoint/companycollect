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

function AdminBreadcrumbs() {
  const { pathname } = useLocation();
  const companyId = seCompanyIdFromPath(pathname);
  const companyLabel = useSeCompanyLabel(companyId);
  const onGeneralRolesPage = pathname === "/admin/general/roles";
  const onLlmSettingsPage = pathname === "/admin/settings/llms";
  const onEsefPage = pathname === "/admin/esef";
  const onCompanyInfoPage = pathname.startsWith("/admin/se/company/");
  const onCompanyInfoCorrectionsPage =
    pathname === "/admin/se/company-info/corrections";
  const onCompaniesPage =
    pathname === "/admin/se/companies" ||
    pathname.startsWith("/admin/se/companies/");
  const onCompanyAddressCorrectionsPage =
    pathname === "/admin/se/company-address/corrections";

  if (onEsefPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin" />}>Admin</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbPage>ESEF</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onCompanyAddressCorrectionsPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin/se/people" />}>
              Admin
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbPage>Sweden</BreadcrumbPage>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbPage>Address corrections</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onCompanyInfoCorrectionsPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin/se/people" />}>
              Admin
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbPage>Sweden</BreadcrumbPage>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbLink render={<Link to="/admin/se/companies" />}>
              Companies
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>Info corrections</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onCompaniesPage) {
    // Companies > <Tab>. Info is the section index, so it is the leaf on the
    // bare root; every other tab shows Companies as a link back to Info.
    const tab = seCompaniesTabFromPath(pathname);
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden sm:block">
            <BreadcrumbLink render={<Link to="/admin/se/people" />}>
              Admin
            </BreadcrumbLink>
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
                <BreadcrumbPage>{seCompaniesTabLabel(tab)}</BreadcrumbPage>
              </BreadcrumbItem>
            </>
          )}
        </BreadcrumbList>
      </Breadcrumb>
    );
  }

  if (onCompanyInfoPage) {
    return (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem className="hidden md:block">
            <BreadcrumbLink render={<Link to="/admin/se/people" />}>
              Admin
            </BreadcrumbLink>
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
            <BreadcrumbLink render={<Link to="/admin/se/people" />}>
              Admin
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator className="hidden sm:block" />
          <BreadcrumbItem>
            <BreadcrumbPage>Settings</BreadcrumbPage>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>LLMs</BreadcrumbPage>
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
            <BreadcrumbLink render={<Link to="/admin/se/people" />}>
              Admin
            </BreadcrumbLink>
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
        <BreadcrumbItem className="hidden md:block">
          <BreadcrumbLink render={<Link to="/admin/se/people" />}>
            Admin
          </BreadcrumbLink>
        </BreadcrumbItem>
        <BreadcrumbSeparator className="hidden md:block" />
        <BreadcrumbItem className="hidden sm:block">
          <BreadcrumbLink render={<Link to="/admin/se/people" />}>
            Sweden
          </BreadcrumbLink>
        </BreadcrumbItem>
        <BreadcrumbSeparator className="hidden sm:block" />
        <BreadcrumbItem>
          <BreadcrumbPage>People</BreadcrumbPage>
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
