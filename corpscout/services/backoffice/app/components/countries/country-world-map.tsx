import { useEffect, useState } from "react";
import WorldMap, { type ISOCode } from "react-svg-worldmap";
import { useNavigate } from "react-router";
import type { CountryDirectoryRow } from "~/lib/countries-overview.server";
import { Skeleton } from "~/components/ui/skeleton";

const nf = new Intl.NumberFormat("en-US");

export function CountryWorldMap({ countries }: { countries: CountryDirectoryRow[] }) {
  const navigate = useNavigate();
  const [mounted, setMounted] = useState(false);
  const byCode = new Map(countries.map((country) => [country.country_code, country]));

  useEffect(() => setMounted(true), []);

  // The package measures its container while rendering. Deferring that
  // measurement until mount keeps the server and first client render
  // identical, then lets the SVG adopt the real responsive width.
  if (!mounted) {
    return <Skeleton className="aspect-[4/3] w-full" aria-label="Loading world map" />;
  }

  return (
    <div
      className="w-full"
      onClick={(event) => {
        const href = (event.target as Element).closest("a")?.getAttribute("href");
        if (!href?.startsWith("/countries/")) return;
        event.preventDefault();
        navigate(href);
      }}
      onKeyDown={(event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        const href = (event.target as Element).closest("a")?.getAttribute("href");
        if (!href?.startsWith("/countries/")) return;
        event.preventDefault();
        navigate(href);
      }}
    >
      <WorldMap
      title="Countries with company registry data"
      size="responsive"
      frame={false}
      data={countries.map((country) => ({
        country: country.country_code as ISOCode,
        value: country.total_companies,
      }))}
      color="var(--primary)"
      backgroundColor="transparent"
      borderColor="var(--border)"
      tooltipBgColor="var(--popover)"
      tooltipTextColor="var(--popover-foreground)"
      regionClassName="country-map-region"
      styleFunction={({ countryValue }) => ({
        fill: countryValue == null ? "var(--muted)" : "var(--primary)",
        stroke: "var(--border)",
        strokeWidth: countryValue == null ? 0.35 : 0.6,
        opacity: countryValue == null ? 0.72 : 1,
        cursor: countryValue == null ? "default" : "pointer",
      })}
      tooltipTextFunction={({ countryCode, countryName }) => {
        const country = byCode.get(countryCode.toLowerCase());
        return country
          ? `${country.country_name}: ${nf.format(country.total_companies)} companies`
          : countryName;
      }}
      hrefFunction={({ countryCode }) => {
        const country = byCode.get(countryCode.toLowerCase());
        if (!country) return undefined;
        const href = `/countries/${country.country_code}`;
        return {
          href,
          "aria-label": `${country.country_name}, ${nf.format(country.total_companies)} companies`,
        };
      }}
      />
    </div>
  );
}
