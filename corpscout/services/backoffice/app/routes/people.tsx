import { Form, Link } from "react-router";
import { Search, Users } from "lucide-react";
import type { Route } from "./+types/people";
import { getCountry } from "~/lib/countries";
import { searchCountryPeople } from "~/lib/people.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Field, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";

const RESOLUTION_LABELS = {
  verified: "Verified identifier",
  reviewed: "Reviewed correction",
  provisional: "Provisional match",
  unresolved: "Single observation",
  merged: "Merged identity",
} as const;

export async function loader({ request }: Route.LoaderArgs) {
  const query = (new URL(request.url).searchParams.get("q") ?? "").trim();
  const result = await searchCountryPeople(query);
  return { query, ...result };
}

export function meta() {
  return [{ title: "People – CompanyCollect Backoffice" }];
}

function yearSpan(first: number, last: number): string {
  if (last <= 0) return "";
  if (first <= 0 || first === last) return String(last);
  return `${first}–${last}`;
}

export default function PeoplePage({ loaderData }: Route.ComponentProps) {
  const { query, rows, total } = loaderData;
  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <header className="flex flex-wrap items-center gap-3">
        <Users className="text-muted-foreground size-6" />
        <h1 className="text-2xl font-semibold">People</h1>
        <span className="text-muted-foreground text-sm">
          country-scoped identities resolved from company filings
        </span>
      </header>

      <Form method="get" action="/people">
        <FieldGroup className="max-w-lg">
          <Field orientation="horizontal">
            <FieldLabel htmlFor="people-search" className="sr-only">
              Person name
            </FieldLabel>
            <Input
              id="people-search"
              name="q"
              defaultValue={query}
              placeholder="Search by name — e.g. Erik Johan Westman"
              autoFocus
            />
            <Button type="submit" variant="secondary">
              <Search data-icon="inline-start" />
              Search
            </Button>
          </Field>
        </FieldGroup>
      </Form>

      {query.length >= 2 ? (
        <p className="text-muted-foreground text-xs">
          {total} matching {total === 1 ? "country person" : "country people"}
          {total > rows.length ? ` — showing the first ${rows.length}` : ""}. Each
          result has its own country-scoped identifier.
        </p>
      ) : (
        <p className="text-muted-foreground text-sm">
          Type at least two characters. A name can return multiple people in the
          same country.
        </p>
      )}

      {rows.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Matching people</CardTitle>
            <CardDescription>
              Provisional profiles expose the evidence used to combine observations.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ul className="divide-y">
              {rows.map((person) => {
                const country = getCountry(person.country_iso2.toLowerCase());
                const span = yearSpan(
                  person.first_observed_year,
                  person.last_observed_year,
                );
                return (
                  <li
                    key={`${person.country_iso2}-${person.person_id}`}
                    className="flex flex-wrap items-baseline gap-x-3 gap-y-1 py-2"
                  >
                    <span title={country?.name ?? person.country_iso2}>
                      {country?.flag ?? person.country_iso2}
                    </span>
                    <Link
                      to={`/country/${person.country_iso2.toLowerCase()}/person/${person.person_id}`}
                      className="font-medium hover:underline"
                    >
                      {person.preferred_name}
                    </Link>
                    <Badge
                      variant={
                        person.resolution_status === "verified"
                          ? "default"
                          : "outline"
                      }
                    >
                      {RESOLUTION_LABELS[person.resolution_status]}
                    </Badge>
                    <span className="text-muted-foreground text-xs">
                      {person.company_count}{" "}
                      {person.company_count === 1 ? "company" : "companies"} ·{" "}
                      {person.observation_count}{" "}
                      {person.observation_count === 1
                        ? "observation"
                        : "observations"}
                      {span ? ` · ${span}` : ""}
                    </span>
                  </li>
                );
              })}
            </ul>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}
