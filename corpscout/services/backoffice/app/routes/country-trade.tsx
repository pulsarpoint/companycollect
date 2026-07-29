import { useRouteLoaderData } from "react-router";
import type { Route } from "./+types/country-trade";
import type { loader as countryLayoutLoader } from "./country-layout";
import { TradeTab } from "~/components/country/trade-tab";

export default function CountryTrade(_: Route.ComponentProps) {
  const layoutData = useRouteLoaderData<typeof countryLayoutLoader>("routes/country-layout")!;

  return <TradeTab worldBank={layoutData.worldBank.series} trade={layoutData.trade} />;
}
