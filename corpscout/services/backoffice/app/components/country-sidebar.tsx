import { Link, useLocation } from "react-router";
import { Building2, LayoutDashboard } from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
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

interface NavItem {
  title: string;
  to: string;
  icon: typeof LayoutDashboard;
  /** Match the path exactly (Overview) rather than as a prefix (Companies). */
  end: boolean;
}

function isNavItemActive(pathname: string, item: NavItem): boolean {
  if (item.end) return pathname === item.to;
  return pathname === item.to || pathname.startsWith(`${item.to}/`);
}

export function CountrySidebar({ country }: { country: CountryConfig }) {
  const location = useLocation();
  const items: NavItem[] = [
    { title: "Overview", to: `/${country.code}`, icon: LayoutDashboard, end: true },
    { title: "Companies", to: `/${country.code}/companies`, icon: Building2, end: false },
  ];

  return (
    <Sidebar collapsible="offcanvas">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton className="h-auto" render={<Link to="/" />}>
              <span className="text-xl">{country.flag}</span>
              <span className="font-semibold">{country.name}</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {items.map((item) => (
                <SidebarMenuItem key={item.to}>
                  <SidebarMenuButton
                    isActive={isNavItemActive(location.pathname, item)}
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
