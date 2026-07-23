import { Link, useLocation } from "react-router";
import { Building2, ChartColumn, Globe2, Users } from "lucide-react";
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
  { title: "Companies", to: "/companies", icon: Building2 },
  { title: "Countries", to: "/countries", icon: Globe2 },
  { title: "People", to: "/people", icon: Users },
  { title: "Financials", to: "/financials", icon: ChartColumn },
];

/**
 * An item is active on an exact match or any of its sub-paths. Companies
 * additionally activates for `/company/:country/:id` detail pages, and
 * People for `/person/:name` pages — both live outside their list prefix.
 */
function isNavItemActive(pathname: string, to: string): boolean {
  if (pathname === to || pathname.startsWith(`${to}/`)) return true;
  if (to === "/companies" && pathname.startsWith("/company/")) return true;
  return to === "/people" && pathname.startsWith("/person/");
}

export function AppSidebar() {
  const { pathname } = useLocation();
  return (
    <Sidebar collapsible="offcanvas">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton render={<Link to="/companies" />} className="h-auto">
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
