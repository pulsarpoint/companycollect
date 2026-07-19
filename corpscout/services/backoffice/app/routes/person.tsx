import { Link } from "react-router";
import { ArrowLeft, Users } from "lucide-react";
import type { Route } from "./+types/person";
import { getCountry } from "~/lib/countries";
import { chQuery } from "~/lib/clickhouse.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";

const ROLE_LABELS: Record<string, string> = {
  chairman: "Chairman",
  ceo: "CEO",
  board_member: "Board member",
  deputy_board_member: "Deputy board member",
  liquidator: "Liquidator",
  auditor: "Auditor",
  other: "Other",
  unknown: "Signatory",
};

const EMPHASIZED_ROLES = new Set(["chairman", "ceo"]);

interface PersonCompanyRow {
  country_iso2: string;
  company_id: string;
  company_name: string;
  first_name: string;
  last_name: string;
  role_kind: string;
  role_original: string;
  from_year: number;
  to_year: number;
  observations: number;
}

export async function loader({ params }: Route.LoaderArgs) {
  const name = decodeURIComponent(params.name).trim().toLowerCase();
  if (name === "" || name.length > 200) throw new Response("Not found", { status: 404 });
  // One row per company the name appears in; latest-year role wins the label.
  const rows = await chQuery<PersonCompanyRow>(
    `SELECT country_iso2 AS country_iso2,
       company_id AS company_id,
       any(company_name) AS company_name,
       any(first_name) AS first_name,
       any(last_name) AS last_name,
       argMax(role_kind, (fiscal_year, statement_order)) AS role_kind,
       argMax(role_original, (fiscal_year, statement_order)) AS role_original,
       toInt32(minIf(fiscal_year, fiscal_year > 0)) AS from_year,
       toInt32(max(fiscal_year)) AS to_year,
       toUInt32(count()) AS observations
     FROM (
       SELECT *, source_statement_key AS statement_order
       FROM company_people_all
       WHERE full_name_normalized = {name:String}
     )
     GROUP BY country_iso2, company_id
     ORDER BY to_year DESC, company_name
     LIMIT 500`,
    { name },
  );
  if (rows.length === 0) throw new Response("Person not found", { status: 404 });
  return { name, rows };
}

export function meta({ loaderData, params }: Route.MetaArgs) {
  const first = loaderData?.rows[0];
  const display = first ? `${first.first_name} ${first.last_name}`.trim() : decodeURIComponent(params.name);
  return [{ title: `${display} – CompanyCollect Backoffice` }];
}

function yearSpan(row: PersonCompanyRow): string {
  if (row.to_year <= 0) return "";
  if (row.from_year <= 0 || row.from_year === row.to_year) return String(row.to_year);
  return `${row.from_year}–${row.to_year}`;
}

export default function PersonPage({ loaderData }: Route.ComponentProps) {
  const { rows } = loaderData;
  const display = `${rows[0].first_name} ${rows[0].last_name}`.trim();

  // Role summary across companies, most frequent first.
  const roleCounts = new Map<string, number>();
  for (const r of rows) roleCounts.set(r.role_kind, (roleCounts.get(r.role_kind) ?? 0) + 1);
  const roleSummary = Array.from(roleCounts.entries()).sort((a, b) => b[1] - a[1]);

  const countries = new Set(rows.map((r) => r.country_iso2));
  const years = rows.filter((r) => r.to_year > 0);
  const minYear = years.length ? Math.min(...years.map((r) => (r.from_year > 0 ? r.from_year : r.to_year))) : 0;
  const maxYear = years.length ? Math.max(...years.map((r) => r.to_year)) : 0;

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div>
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2"
          nativeButton={false}
          render={<Link to="/companies" />}
        >
          <ArrowLeft className="size-4" />
          Companies
        </Button>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <Users className="text-muted-foreground size-6" />
        <h2 className="text-2xl font-semibold">{display}</h2>
        <span className="text-muted-foreground text-sm">
          {rows.length} {rows.length === 1 ? "company" : "companies"}
          {minYear > 0 ? ` · ${minYear === maxYear ? maxYear : `${minYear}–${maxYear}`}` : ""}
        </span>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {roleSummary.map(([kind, count]) => (
          <Badge key={kind} variant={EMPHASIZED_ROLES.has(kind) ? "default" : "outline"}>
            {ROLE_LABELS[kind] ?? kind}
            {count > 1 ? ` ×${count}` : ""}
          </Badge>
        ))}
        {Array.from(countries).map((c) => (
          <span key={c} title={getCountry(c.toLowerCase())?.name ?? c}>
            {getCountry(c.toLowerCase())?.flag ?? c}
          </span>
        ))}
      </div>

      <p className="text-muted-foreground -mt-1 text-xs">
        Grouped by name from filing signatures — rows may belong to different
        people who share this name. Registered person identifiers are not
        public in these sources.
      </p>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Registered roles</CardTitle>
        </CardHeader>
        <CardContent>
          <ul className="divide-y">
            {rows.map((r) => {
              const country = getCountry(r.country_iso2.toLowerCase());
              const span = yearSpan(r);
              return (
                <li
                  key={`${r.country_iso2}-${r.company_id}`}
                  className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5 py-1.5"
                >
                  <span title={country?.name ?? r.country_iso2}>{country?.flag ?? r.country_iso2}</span>
                  <Link
                    to={`/company/${r.country_iso2.toLowerCase()}/${r.company_id}`}
                    className="font-medium hover:underline"
                  >
                    {r.company_name || r.company_id}
                  </Link>
                  <Badge variant={EMPHASIZED_ROLES.has(r.role_kind) ? "default" : "outline"}>
                    {ROLE_LABELS[r.role_kind] ?? r.role_kind}
                  </Badge>
                  {span ? (
                    <span className="text-muted-foreground text-xs tabular-nums">{span}</span>
                  ) : null}
                  {r.role_original && r.role_original !== (ROLE_LABELS[r.role_kind] ?? "") ? (
                    <span className="text-muted-foreground text-xs">{r.role_original}</span>
                  ) : null}
                </li>
              );
            })}
          </ul>
        </CardContent>
      </Card>
    </div>
  );
}
