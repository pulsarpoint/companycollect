import { Link } from "react-router";
import type { Route } from "./+types/home";
import { COUNTRIES } from "~/lib/countries";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export function meta(_: Route.MetaArgs) {
  return [{ title: "CompanyCollect Backoffice" }];
}

export default function Home() {
  return (
    <main className="mx-auto max-w-5xl px-6 py-12">
      <h1 className="text-3xl font-semibold tracking-tight">
        CompanyCollect Backoffice
      </h1>
      <p className="text-muted-foreground mt-2">
        Select a country to explore its company data.
      </p>
      <div className="mt-8 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {COUNTRIES.map((country) => (
          <Link key={country.code} to={`/${country.code}`}>
            <Card className="hover:bg-accent/50 h-full transition-colors">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className="text-2xl">{country.flag}</span>
                  {country.name}
                </CardTitle>
                <CardDescription>
                  ~{country.approxCompanies} companies
                </CardDescription>
                <div className="mt-2 flex flex-wrap gap-1">
                  {country.features.map((f) => (
                    <Badge key={f} variant="secondary">
                      {f}
                    </Badge>
                  ))}
                </div>
              </CardHeader>
            </Card>
          </Link>
        ))}
      </div>
    </main>
  );
}
