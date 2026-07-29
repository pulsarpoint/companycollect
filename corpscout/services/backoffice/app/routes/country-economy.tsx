import { useRouteLoaderData } from "react-router";
import type { Route } from "./+types/country-economy";
import type { loader as countryLayoutLoader } from "./country-layout";
import { EconomyTab } from "~/components/country/economy-tab";

export default function CountryEconomy(_: Route.ComponentProps) {
  const layoutData = useRouteLoaderData<typeof countryLayoutLoader>("routes/country-layout")!;

  return (
    <EconomyTab
      worldBank={layoutData.worldBank.series}
      worldBankUpdatedDate={layoutData.worldBank.sourceUpdatedDate}
      imf={layoutData.imf}
    />
  );
}
