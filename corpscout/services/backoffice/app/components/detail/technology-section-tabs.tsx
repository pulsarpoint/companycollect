import { NavLink } from "react-router";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import type { TechnologySection } from "~/lib/company-tabs";

export function TechnologySectionTabs({
  basePath,
  section,
  search = "",
  mailSecurity = false,
  dnsRecords = false,
}: {
  basePath: string;
  section: TechnologySection | "dns";
  search?: string;
  mailSecurity?: boolean;
  dnsRecords?: boolean;
}) {
  return (
    <div className="max-w-full overflow-x-auto">
      <Tabs value={section}>
        <TabsList aria-label="Domain technology sections">
          {dnsRecords ? (
            <TabsTrigger value="dns" render={<NavLink to={`${basePath}/dns${search}`} />} nativeButton={false}>
              DNS records
            </TabsTrigger>
          ) : null}
          <TabsTrigger
            value="overview"
            render={<NavLink to={`${basePath}${dnsRecords ? "/overview" : ""}${search}`} end />}
            nativeButton={false}
          >
            Overview
          </TabsTrigger>
          <TabsTrigger
            value="web-technologies"
            render={<NavLink to={`${basePath}/web-technologies${search}`} />}
            nativeButton={false}
          >
            Web technologies
          </TabsTrigger>
          <TabsTrigger
            value="web-intelligence"
            render={<NavLink to={`${basePath}/web-intelligence${search}`} />}
            nativeButton={false}
          >
            Web intelligence
          </TabsTrigger>
          <TabsTrigger
            value="infrastructure"
            render={<NavLink to={`${basePath}/infrastructure${search}`} />}
            nativeButton={false}
          >
            {dnsRecords ? "Infrastructure" : "Infrastructure & DNS"}
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

export function CommonCrawlSectionTabs({
  basePath,
  section,
}: {
  basePath: string;
  section: "overview" | "web-technologies";
}) {
  return (
    <Tabs value={section}>
      <TabsList aria-label="Common Crawl sections">
        <TabsTrigger
          value="overview"
          render={<NavLink to={basePath} end />}
          nativeButton={false}
        >
          Website evidence
        </TabsTrigger>
        <TabsTrigger
          value="web-technologies"
          render={<NavLink to={`${basePath}/web-technologies`} />}
          nativeButton={false}
        >
          Web technologies
        </TabsTrigger>
      </TabsList>
    </Tabs>
  );
}
