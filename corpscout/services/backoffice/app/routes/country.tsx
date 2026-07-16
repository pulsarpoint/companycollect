import { data, Outlet } from "react-router";
import type { Route } from "./+types/country";
import { getCountry } from "~/lib/countries";
import { CountrySidebar } from "~/components/country-sidebar";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "~/components/ui/sidebar";
import { Separator } from "~/components/ui/separator";

export function loader({ params }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) {
    throw data(`Unknown country: ${params.country}`, { status: 404 });
  }
  return { country };
}

export default function CountryLayout({ loaderData }: Route.ComponentProps) {
  const { country } = loaderData;
  return (
    <SidebarProvider>
      <CountrySidebar country={country} />
      <SidebarInset>
        <header className="flex h-12 items-center gap-2 border-b px-4">
          <SidebarTrigger className="-ml-1" />
          <Separator orientation="vertical" className="mx-2 h-4" />
          <span className="text-sm font-medium">{country.name}</span>
        </header>
        <div className="flex flex-1 flex-col gap-4 p-4 md:p-6">
          <Outlet />
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}
