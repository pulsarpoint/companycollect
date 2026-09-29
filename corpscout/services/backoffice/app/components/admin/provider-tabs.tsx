import { NavLink } from "react-router";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";

/** Tabs of a provider page: its provider-recon definition and the domains using it. */
export function ProviderTabs({ slug, active }: { slug: string; active: "definition" | "domains" }) {
  const base = `/admin/provider-feeds/providers/${encodeURIComponent(slug)}`;
  return (
    <Tabs value={active}>
      <TabsList aria-label="Provider sections">
        <TabsTrigger value="definition" render={<NavLink to={base} end />} nativeButton={false}>
          Definition &amp; feeds
        </TabsTrigger>
        <TabsTrigger value="domains" render={<NavLink to={`${base}/domains`} />} nativeButton={false}>
          Domains
        </TabsTrigger>
      </TabsList>
    </Tabs>
  );
}
