import { Link, useLocation } from "react-router";
import { Gavel, Globe2, Users } from "lucide-react";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "~/components/ui/sidebar";

const NAV_ITEMS = [
  { title: "Countries", to: "/countries", icon: Globe2 },
  { title: "Procurement", to: "/procurements", icon: Gavel },
  { title: "People", to: "/people", icon: Users },
];

/**
 * An item is active on an exact match or any of its sub-paths. Countries
 * additionally activates for `/company/:country/:id` detail pages, and
 * People for country-scoped person detail pages — both live outside their
 * list prefix. Legacy `/person/:name` routes redirect back to People search.
 */
function isNavItemActive(pathname: string, to: string): boolean {
  if (pathname === to || pathname.startsWith(`${to}/`)) return true;
  if (to === "/countries" && pathname.startsWith("/company/")) return true;
  return (
    to === "/people" &&
    (pathname.startsWith("/person/") ||
      (pathname.startsWith("/country/") && pathname.includes("/person/")))
  );
}

export function AppSidebar() {
  const { pathname } = useLocation();
  return (
    <Sidebar collapsible="offcanvas">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton render={<Link to="/countries" />} className="h-auto">
              <span className="font-semibold">CompanyCollect</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV_ITEMS.map((item) => (
                <SidebarMenuItem key={item.to}>
                  <SidebarMenuButton
                    isActive={isNavItemActive(pathname, item.to)}
                    render={<Link to={item.to} />}
                  >
                    <item.icon />
                    <span>{item.title}</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
