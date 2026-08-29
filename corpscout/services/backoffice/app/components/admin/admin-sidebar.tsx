import { Link, useLocation } from "react-router";
import {
  ArrowLeftIcon,
  BlocksIcon,
  Building2Icon,
  BotIcon,
  BrainCircuitIcon,
  ChevronRightIcon,
  DatabaseZapIcon,
  FlagIcon,
  MapPinIcon,
  PlayIcon,
  Settings2Icon,
  SlidersHorizontalIcon,
  TagsIcon,
  TriangleAlertIcon,
  UsersRoundIcon,
} from "lucide-react";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "~/components/ui/collapsible";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  SidebarRail,
} from "~/components/ui/sidebar";

const COUNTRY_NAVIGATION = [
  {
    title: "Sweden",
    code: "SE",
    icon: FlagIcon,
    items: [
      {
        title: "People",
        to: "/admin/se/people",
        icon: UsersRoundIcon,
        exact: true,
      },
      {
        title: "Stale corrections",
        to: "/admin/se/people/stale-corrections",
        icon: TriangleAlertIcon,
        exact: true,
      },
      {
        title: "People pipeline",
        to: "/admin/se/people/pipeline",
        icon: PlayIcon,
        exact: true,
      },
      {
        // One entry for the whole tabbed list area (Info · Geocoding ·
        // Financial · People). exact:false so it stays active on every tab.
        // Info corrections is reached as a secondary link from the Info tab.
        title: "Companies",
        to: "/admin/se/companies",
        icon: Building2Icon,
        exact: false,
      },
      {
        title: "Address corrections",
        to: "/admin/se/company-address/corrections",
        icon: MapPinIcon,
        exact: true,
      },
    ],
  },
] as const;

const GENERAL_NAVIGATION = [
  {
    title: "Roles",
    to: "/admin/general/roles",
    icon: TagsIcon,
  },
] as const;

const SETTINGS_NAVIGATION = [
  {
    title: "LLMs",
    to: "/admin/settings/llms",
    icon: BrainCircuitIcon,
  },
] as const;

export function AdminSidebar() {
  const { pathname } = useLocation();

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              size="lg"
              tooltip="CompanyCollect admin"
              render={<Link to="/admin/se/people" />}
            >
              <span className="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
                <BotIcon />
              </span>
              <span className="grid flex-1 text-left text-sm leading-tight">
                <span className="truncate font-semibold">CompanyCollect</span>
                <span className="truncate text-xs text-sidebar-foreground/70">
                  Curation workspace
                </span>
              </span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Workspace</SidebarGroupLabel>
          <SidebarMenu>
            <SidebarMenuItem>
              <SidebarMenuButton
                isActive={pathname === "/admin/esef"}
                tooltip="ESEF processing"
                render={<Link to="/admin/esef" />}
              >
                <DatabaseZapIcon />
                <span>ESEF</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
            <SidebarMenuItem>
              <SidebarMenuButton
                // Active on the whole subtree: the list AND every
                // /admin/technologies/:slug detail page.
                isActive={
                  pathname === "/admin/technologies" ||
                  pathname.startsWith("/admin/technologies/")
                }
                tooltip="Technology catalog"
                render={<Link to="/admin/technologies" />}
              >
                <BlocksIcon />
                <span>Technologies</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
            <Collapsible
              defaultOpen={pathname.startsWith("/admin/general/")}
              className="group/collapsible"
              render={<SidebarMenuItem />}
            >
              <CollapsibleTrigger
                render={
                  <SidebarMenuButton
                    isActive={pathname.startsWith("/admin/general/")}
                    tooltip="General"
                  />
                }
              >
                <Settings2Icon />
                <span>General</span>
                <ChevronRightIcon className="ml-auto transition-transform duration-200 group-data-open/collapsible:rotate-90" />
              </CollapsibleTrigger>
              <CollapsibleContent>
                <SidebarMenuSub>
                  {GENERAL_NAVIGATION.map((item) => (
                    <SidebarMenuSubItem key={item.to}>
                      <SidebarMenuSubButton
                        isActive={pathname === item.to}
                        render={<Link to={item.to} />}
                      >
                        <item.icon />
                        <span>{item.title}</span>
                      </SidebarMenuSubButton>
                    </SidebarMenuSubItem>
                  ))}
                </SidebarMenuSub>
              </CollapsibleContent>
            </Collapsible>
            <Collapsible
              defaultOpen={pathname.startsWith("/admin/settings/")}
              className="group/collapsible"
              render={<SidebarMenuItem />}
            >
              <CollapsibleTrigger
                render={
                  <SidebarMenuButton
                    isActive={pathname.startsWith("/admin/settings/")}
                    tooltip="Settings"
                  />
                }
              >
                <SlidersHorizontalIcon />
                <span>Settings</span>
                <ChevronRightIcon className="ml-auto transition-transform duration-200 group-data-open/collapsible:rotate-90" />
              </CollapsibleTrigger>
              <CollapsibleContent>
                <SidebarMenuSub>
                  {SETTINGS_NAVIGATION.map((item) => (
                    <SidebarMenuSubItem key={item.to}>
                      <SidebarMenuSubButton
                        isActive={pathname === item.to}
                        render={<Link to={item.to} />}
                      >
                        <item.icon />
                        <span>{item.title}</span>
                      </SidebarMenuSubButton>
                    </SidebarMenuSubItem>
                  ))}
                </SidebarMenuSub>
              </CollapsibleContent>
            </Collapsible>
          </SidebarMenu>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel>Countries</SidebarGroupLabel>
          <SidebarMenu>
            {COUNTRY_NAVIGATION.map((country) => {
              const countryIsActive = pathname.startsWith(
                `/admin/${country.code.toLowerCase()}/`,
              );

              return (
                <Collapsible
                  key={country.code}
                  defaultOpen={countryIsActive}
                  className="group/collapsible"
                  render={<SidebarMenuItem />}
                >
                  <CollapsibleTrigger
                    render={
                      <SidebarMenuButton
                        isActive={countryIsActive}
                        tooltip={country.title}
                      />
                    }
                  >
                    <country.icon />
                    <span>{country.title}</span>
                    <ChevronRightIcon className="ml-auto transition-transform duration-200 group-data-open/collapsible:rotate-90" />
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <SidebarMenuSub>
                      {country.items.map((item) => (
                        <SidebarMenuSubItem key={item.to}>
                          <SidebarMenuSubButton
                            isActive={
                              item.exact
                                ? pathname === item.to
                                : pathname === item.to ||
                                  pathname.startsWith(`${item.to}/`)
                            }
                            render={<Link to={item.to} />}
                          >
                            <item.icon />
                            <span>{item.title}</span>
                          </SidebarMenuSubButton>
                        </SidebarMenuSubItem>
                      ))}
                    </SidebarMenuSub>
                  </CollapsibleContent>
                </Collapsible>
              );
            })}
          </SidebarMenu>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              tooltip="Back to backoffice"
              render={<Link to="/countries" />}
            >
              <ArrowLeftIcon />
              <span>Back to backoffice</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  );
}
