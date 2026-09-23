import { NavLink } from "react-router";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import type { TechnologySection } from "~/lib/company-tabs";

export function TechnologySectionTabs({
  basePath,
  section,
  search = "",
  mailSecurity = false,
  websiteEvidenceOverview = false,
}: {
  basePath: string;
  section: TechnologySection | "technologies";
  search?: string;
  mailSecurity?: boolean;
  websiteEvidenceOverview?: boolean;
}) {
  return (
    <div className="max-w-full overflow-x-auto">
      <Tabs value={section}>
        <TabsList aria-label="Domain technology sections">
          <TabsTrigger
            value="overview"
            render={<NavLink to={`${basePath}${search}`} end />}
            nativeButton={false}
          >
            {websiteEvidenceOverview ? "Website evidence" : "Overview"}
          </TabsTrigger>
          {websiteEvidenceOverview ? (
            <TabsTrigger
              value="technologies"
              render={<NavLink to={`${basePath}/technologies${search}`} />}
              nativeButton={false}
            >
              Archived technologies
            </TabsTrigger>
          ) : null}
          <TabsTrigger
            value="web-technologies"
            render={<NavLink to={`${basePath}/web-technologies${search}`} />}
            nativeButton={false}
          >
            Web technologies
          </TabsTrigger>
          {!websiteEvidenceOverview ? (
            <TabsTrigger
              value="web-intelligence"
              render={<NavLink to={`${basePath}/web-intelligence${search}`} />}
              nativeButton={false}
            >
              Web intelligence
            </TabsTrigger>
          ) : null}
          <TabsTrigger
            value="infrastructure"
            render={<NavLink to={`${basePath}/infrastructure${search}`} />}
            nativeButton={false}
          >
            Infrastructure &amp; DNS
          </TabsTrigger>
          <TabsTrigger
            value="ip-addresses"
            render={<NavLink to={`${basePath}/ip-addresses${search}`} />}
            nativeButton={false}
          >
            IP addresses
          </TabsTrigger>
          {mailSecurity ? (
            <TabsTrigger
              value="mail-security"
              render={<NavLink to={`${basePath}/mail-security${search}`} />}
              nativeButton={false}
            >
              Mail security
            </TabsTrigger>
          ) : null}
        </TabsList>
      </Tabs>
    </div>
  );
}
