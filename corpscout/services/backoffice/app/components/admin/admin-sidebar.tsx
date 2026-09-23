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
  GlobeIcon,
  NetworkIcon,
  SearchIcon,
  Settings2Icon,
  SlidersHorizontalIcon,
  TagsIcon,
  UsersIcon,
  WorkflowIcon,
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
        // One entry for the whole tabbed list area (Info · Geocoding ·
        // Financial). exact:false so it stays active on every tab -- except
        // Domains, which has its own entry below and wins by longest match.
        title: "Companies",
        to: "/admin/se/companies",
        icon: Building2Icon,
        exact: false,
      },
      {
        // The Domains tab of the Companies area, surfaced on its own: the
        // domain entity (and its shared-domain review) is looked up directly
        // often enough to deserve a sidebar entry. Active on the list and on
        // every /domains/<domain> page.
        title: "Domains",
        to: "/admin/se/companies/domains",
        icon: GlobeIcon,
        exact: false,
      },
      {
        // A sibling of Companies, not one of its tabs: every published person
        // across every company (person spec section 7).
        title: "People",
        to: "/admin/se/people",
        icon: UsersIcon,
        exact: false,
      },
      { title: "Processing", to: "/admin/se/processing", icon: WorkflowIcon, exact: true },
    ],
  },
] as const;

type CountryNavigationItem = (typeof COUNTRY_NAVIGATION)[number]["items"][number];

function matchesCountryItem(item: CountryNavigationItem, pathname: string): boolean {
  return item.exact
    ? pathname === item.to
    : pathname === item.to || pathname.startsWith(`${item.to}/`);
}

/**
 * The one country entry a path lights up: of the entries whose `to` covers the
 * path, the most specific (longest `to`). Companies covers its whole tabbed
 * area, Domains only its own tab and detail pages -- on /companies/domains/x
 * both match, and Domains wins; on /companies/geocoding only Companies does.
 */
function activeCountryItem(
  items: readonly CountryNavigationItem[],
  pathname: string,
): CountryNavigationItem | undefined {
  return items
    .filter((item) => matchesCountryItem(item, pathname))
    .sort((a, b) => b.to.length - a.to.length)[0];
}

const GENERAL_NAVIGATION = [
  {
    title: "Roles",
    to: "/admin/general/roles",
    icon: TagsIcon,
  },
] as const;

const SETTINGS_NAVIGATION = [
  {
    title: "People prompts",
    to: "/admin/settings/people-prompts",
    icon: TagsIcon,
  },
  {
    title: "Domain prompts",
    to: "/admin/settings/domain-prompts",
    icon: TagsIcon,
  },
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
              render={<Link to="/admin" />}
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
              <SidebarMenuButton isActive={pathname === "/admin/domains" || pathname.startsWith("/admin/domains/")} tooltip="Domains" render={<Link to="/admin/domains" />}>
                <GlobeIcon /><span>Domains</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
            <SidebarMenuItem>
              <SidebarMenuButton
                isActive={pathname === "/admin/graph"}
                tooltip="Domain graph"
                render={<Link to="/admin/graph" />}
              >
                <NetworkIcon />
                <span>Graph</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
            <SidebarMenuItem>
              <SidebarMenuButton
                isActive={
                  pathname === "/admin/common-crawl" ||
                  pathname.startsWith("/admin/common-crawl/")
                }
                tooltip="Common Crawl evidence"
                render={<Link to="/admin/common-crawl" />}
              >
                <SearchIcon />
                <span>Common Crawl</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
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
                  pathname.startsWith("/admin/technologies/") ||
                  pathname === "/admin/technology-proposals" ||
                  pathname.startsWith("/admin/technology-proposals/")
                }
                tooltip="Technology catalog"
                render={<Link to="/admin/technologies" />}
              >
                <BlocksIcon />
                <span>Technologies</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
            <SidebarMenuItem>
              <SidebarMenuButton
                isActive={pathname.startsWith("/admin/crawls")}
                tooltip="Crawler attempts and human assistance"
                render={<Link to="/admin/crawls" />}
              >
                <BotIcon />
                <span>Crawler</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
            <SidebarMenuItem>
              <SidebarMenuButton isActive={pathname.startsWith("/admin/browsers")} tooltip="Browser servers and saved profiles" render={<Link to="/admin/browsers" />}>
                <BotIcon /><span>Browsers</span>
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
                            isActive={activeCountryItem(country.items, pathname) === item}
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
