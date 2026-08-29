import type { Route } from "./+types/admin-technology-detail";
import { TechnologyDetailView } from "~/components/admin/technology-detail";
import {
  loadSeCompaniesUsingTechnology,
  loadTechnologyAdoption,
  loadTechnologyDetail,
} from "~/lib/technologies.server";

// One catalog technology: the full catalog record, its global adoption from
// the weekly rollup ("not computed yet" while the rollup is empty), and a
// LIVE key-pruned lookup of the Swedish companies whose domains carry a
// detection. The two follow-up reads need the technology's exact detector
// NAME (detections store the name, not the slug), so the catalog row resolves
// first and an unknown slug 404s before the heavy table is ever touched.

export async function loader({ params }: Route.LoaderArgs) {
  const technology = await loadTechnologyDetail(params.slug);
  if (!technology) {
    throw new Response("Not found", { status: 404 });
  }
  const [adoption, companies] = await Promise.all([
    loadTechnologyAdoption(technology.technology),
    loadSeCompaniesUsingTechnology(technology.technology),
  ]);
  return {
    technology,
    adoption,
    companies: companies.rows,
    companiesError: companies.error,
  };
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
  const { technology, adoption, companies, companiesError } = loaderData;
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <TechnologyDetailView
        technology={technology}
        adoption={adoption}
        companies={companies}
        companiesError={companiesError}
      />
    </div>
  );
}
