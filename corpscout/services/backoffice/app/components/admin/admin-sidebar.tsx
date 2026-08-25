import { Link, useLocation } from "react-router";
import {
  ArrowLeftIcon,
  Building2Icon,
  BotIcon,
  BrainCircuitIcon,
  ChevronRightIcon,
  FlagIcon,
  ListChecksIcon,
  MapPinIcon,
  MapPinnedIcon,
  Rows3Icon,
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
        title: "Sources",
        to: "/admin/se/people/sources",
        icon: Rows3Icon,
        exact: false,
      },
      {
        title: "Company info",
        to: "/admin/se/company-info",
        icon: Building2Icon,
        exact: true,
      },
      {
        title: "Info corrections",
        to: "/admin/se/company-info/corrections",
        icon: ListChecksIcon,
        exact: true,
      },
      {
        title: "Geocoding",
        to: "/admin/se/company-info/geocoding",
        icon: MapPinnedIcon,
        exact: true,
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
