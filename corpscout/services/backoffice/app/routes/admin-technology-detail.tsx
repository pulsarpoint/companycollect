import type { Route } from "./+types/admin-technology-detail";
import {
  TechnologyDetailView,
  type TechnologyAdoptionTabData,
} from "~/components/admin/technology-detail";
import {
  parseTechnologyDetailCountry,
  parseTechnologyDetailTab,
  parseTechnologyListView,
} from "~/lib/technologies";
import {
  countTechnologyCompanies,
  countTechnologyDomains,
  loadTechnologyAdoption,
  loadTechnologyCompaniesComputedAt,
  loadTechnologyCompaniesPage,
  loadTechnologyCompanyCountries,
  loadTechnologyDetail,
  loadTechnologyDomainsComputedAt,
  loadTechnologyDomainsPage,
} from "~/lib/technologies.server";

// One catalog technology: the full catalog record, its global adoption count
// from the weekly rollup ("not computed yet" while the rollup is empty), and
// the adoption section's two tabs -- Domains (weekly technology_top_domains,
// ordered by harmonic centrality) and Companies (weekly technology_companies,
// country-filterable). Tab, country filter and paging are search params
// (mirrors /admin/se/people), and only the ACTIVE tab's rollup is queried per
// request. Every follow-up read needs the technology's exact detector NAME
// (rollups store the name, not the slug), so the catalog row resolves first
// and an unknown slug 404s before anything else runs.

export async function loader({ request, params }: Route.LoaderArgs) {
  const technology = await loadTechnologyDetail(params.slug);
  if (!technology) {
    throw new Response("Not found", { status: 404 });
  }
  const url = new URL(request.url);
  const activeTab = parseTechnologyDetailTab(url);
  const country = parseTechnologyDetailCountry(url);
  const view = parseTechnologyListView(url);
  const name = technology.technology;

  if (activeTab === "domains") {
    const [adoption, rows, total, computedAt] = await Promise.all([
      loadTechnologyAdoption(name),
      loadTechnologyDomainsPage(name, view.page, view.pageSize),
      countTechnologyDomains(name),
      loadTechnologyDomainsComputedAt(name),
    ]);
    const tab: TechnologyAdoptionTabData = {
      tab: "domains",
      rows,
      total,
      computedAt,
    };
    return { technology, adoption, tab, country, view };
  }

  const [adoption, rows, total, countries, computedAt] = await Promise.all([
    loadTechnologyAdoption(name),
    loadTechnologyCompaniesPage(name, country, view.page, view.pageSize),
    countTechnologyCompanies(name, country),
    loadTechnologyCompanyCountries(name),
    loadTechnologyCompaniesComputedAt(name),
  ]);
  const tab: TechnologyAdoptionTabData = {
    tab: "companies",
    rows,
    total,
    countries,
    computedAt,
  };
  return { technology, adoption, tab, country, view };
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.technology.technology ?? "Technology"} | CompanyCollect`,
    },
  ];
}

export default function AdminTechnologyDetail({
  loaderData,
}: Route.ComponentProps) {
  const { technology, adoption, tab, country, view } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <TechnologyDetailView
        technology={technology}
        adoption={adoption}
        tab={tab}
        country={country}
        view={view}
      />
    </div>
  );
}
