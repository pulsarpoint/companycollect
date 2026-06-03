import {
  BarChart3,
  BookOpen,
  Building2,
  CheckSquare,
  CircleDollarSign,
  Clock3,
  Globe,
  ListTree,
  Server,
  Settings,
} from "lucide-react";
import { Link, useLocation } from "react-router";
import { Badge } from "~/components/ui/badge";
import {
  Sidebar,
  SidebarContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
} from "~/components/ui/sidebar";

const PRIMARY_NAV_ITEMS = [
  { title: "Dashboard", url: "/dashboard", icon: BarChart3 },
  { title: "Review", url: "/review", icon: CheckSquare },
  { title: "Companies", url: "/companies", icon: Building2 },
  { title: "Domains", url: "/domains", icon: Globe },
  { title: "Sources", url: "/sources", icon: Server },
] as const;

const SETTINGS_NAV_ITEMS = [
  { title: "LLM Providers", url: "/settings/llm-providers", icon: Settings },
  { title: "NACE Taxonomy", url: "/settings/nace-taxonomy", icon: BookOpen },
  {
    title: "NACE Browser",
    url: "/settings/nace-taxonomy/browser",
    icon: ListTree,
  },
  { title: "FX Rates", url: "/settings/fx-rates", icon: CircleDollarSign },
  { title: "Schedules", url: "/settings/workflow-schedules", icon: Clock3 },
] as const;

function isSettingsNavItemActive(pathname: string, url: string) {
  if (url === "/settings/nace-taxonomy") {
    return pathname === url;
  }
  return pathname.startsWith(url);
}

interface AppSidebarProps {
  pendingReview?: number;
}

export function AppSidebar({ pendingReview }: AppSidebarProps) {
  const location = useLocation();

  return (
    <Sidebar>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" asChild>
              <Link to="/dashboard">
                <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
                  <Globe className="size-4" />
                </div>
                <div className="flex flex-col gap-0.5 leading-none">
                  <span className="font-semibold">CorpScout</span>
                  <span className="text-xs text-muted-foreground">Company Discovery</span>
                </div>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarMenu>
          {PRIMARY_NAV_ITEMS.map((item) => (
            <SidebarMenuItem key={item.title}>
              <SidebarMenuButton asChild isActive={location.pathname.startsWith(item.url)}>
                <Link to={item.url}>
                  <item.icon />
                  <span>{item.title}</span>
                  {item.title === "Review" && pendingReview != null && pendingReview > 0 && (
                    <Badge
                      variant="destructive"
                      className="ml-auto h-5 min-w-5 justify-center px-1 text-xs"
                    >
                      {pendingReview}
                    </Badge>
                  )}
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
        <div className="flex flex-col gap-2 px-2 pt-2">
          <div className="px-2 text-xs font-medium text-muted-foreground">
            Settings
          </div>
          <SidebarMenu>
            {SETTINGS_NAV_ITEMS.map((item) => (
              <SidebarMenuItem key={item.title}>
                <SidebarMenuButton
                  asChild
                  isActive={isSettingsNavItemActive(location.pathname, item.url)}
                >
                  <Link to={item.url}>
                    <item.icon />
                    <span>{item.title}</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            ))}
          </SidebarMenu>
        </div>
      </SidebarContent>
      <SidebarRail />
    </Sidebar>
  );
}
