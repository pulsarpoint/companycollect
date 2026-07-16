import { useRouteLoaderData } from "react-router";
import type { Route } from "./+types/country-overview";
import type { loader as countryLoader } from "./country";
import { getCountry } from "~/lib/countries";
import { getCountryStats } from "~/lib/queries.server";
import {
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export async function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });
  return { stats: await getCountryStats(country) };
}

const nf = new Intl.NumberFormat("en-US");

export default function CountryOverview({ loaderData }: Route.ComponentProps) {
  const { stats } = loaderData;
  const parent = useRouteLoaderData<typeof countryLoader>("routes/country");
  const tiles = [
    { label: "Total companies", value: stats.total },
    { label: "Active", value: stats.active },
    { label: "Inactive", value: stats.total - stats.active },
  ];
  return (
    <>
      <h2 className="text-xl font-semibold">
        {parent?.country.name} overview
      </h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        {tiles.map((tile) => (
          <Card key={tile.label}>
            <CardHeader>
              <CardDescription>{tile.label}</CardDescription>
              <CardTitle className="text-3xl tabular-nums">
                {nf.format(tile.value)}
              </CardTitle>
            </CardHeader>
          </Card>
        ))}
      </div>
    </>
  );
}
