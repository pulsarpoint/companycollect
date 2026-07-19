import { Form, Link } from "react-router";
import { Search, Users } from "lucide-react";
import type { Route } from "./+types/people";
import { getCountry } from "~/lib/countries";
import { chQuery } from "~/lib/clickhouse.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent } from "~/components/ui/card";
import { Input } from "~/components/ui/input";

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

interface PeopleSearchRow {
  full_name_normalized: string;
  first_name: string;
  last_name: string;
  companies: number;
  countries: string[];
  top_role: string;
  latest_year: number;
}

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const q = (url.searchParams.get("q") ?? "").trim();
  if (q.length < 2) return { q, rows: [], total: null };

  const [rows, totals] = await Promise.all([
    chQuery<PeopleSearchRow>(
      `SELECT full_name_normalized AS full_name_normalized,
         any(first_name) AS first_name,
         any(last_name) AS last_name,
         toUInt32(uniqExact(country_iso2, company_id)) AS companies,
         groupUniqArray(country_iso2) AS countries,
         topK(1)(role_kind)[1] AS top_role,
         toInt32(max(fiscal_year)) AS latest_year
       FROM company_people_all
       WHERE full_name_normalized LIKE {pattern:String}
       GROUP BY full_name_normalized
       ORDER BY companies DESC, full_name_normalized
       LIMIT 50`,
      { pattern: `%${q.toLowerCase()}%` },
    ),
    chQuery<{ total: string }>(
      `SELECT uniqExact(full_name_normalized) AS total
       FROM company_people_all
       WHERE full_name_normalized LIKE {pattern:String}`,
      { pattern: `%${q.toLowerCase()}%` },
    ),
  ]);
  return { q, rows, total: Number(totals[0]?.total ?? 0) };
}

export function meta() {
  return [{ title: "People – CompanyCollect Backoffice" }];
}

export default function PeoplePage({ loaderData }: Route.ComponentProps) {
  const { q, rows, total } = loaderData;
  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <div className="flex flex-wrap items-center gap-3">
        <Users className="text-muted-foreground size-6" />
        <h2 className="text-2xl font-semibold">People</h2>
        <span className="text-muted-foreground text-sm">
          registered officers, board members and auditors from company filings
        </span>
      </div>

      <Form method="get" className="flex max-w-lg gap-2">
        <Input
          name="q"
          defaultValue={q}
          placeholder="Search by name — e.g. klas balkow"
          autoFocus
        />
        <Button type="submit" variant="secondary">
          <Search className="size-4" />
          Search
        </Button>
      </Form>

      {q.length >= 2 ? (
        <p className="text-muted-foreground text-xs">
          {total} matching {total === 1 ? "name" : "names"}
          {total !== null && total > rows.length ? ` — showing the ${rows.length} with most companies` : ""}
          . Grouped by name — a row may cover different people who share it.
        </p>
      ) : (
        <p className="text-muted-foreground text-sm">
          Type at least two characters to search 549k+ distinct names.
        </p>
      )}

      {rows.length > 0 ? (
        <Card>
          <CardContent>
            <ul className="divide-y">
              {rows.map((r) => (
                <li
                  key={r.full_name_normalized}
                  className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 py-1.5"
                >
                  <Link
                    to={`/person/${encodeURIComponent(r.full_name_normalized)}`}
                    className="font-medium hover:underline"
                  >
                    {r.first_name} {r.last_name}
                  </Link>
                  <span className="text-muted-foreground text-xs">
                    {r.companies} {r.companies === 1 ? "company" : "companies"}
                  </span>
                  <Badge variant="outline">{ROLE_LABELS[r.top_role] ?? r.top_role}</Badge>
                  {r.latest_year > 0 ? (
                    <span className="text-muted-foreground text-xs tabular-nums">
                      latest {r.latest_year}
                    </span>
                  ) : null}
                  <span>
                    {r.countries.map((c) => (
                      <span key={c} title={getCountry(c.toLowerCase())?.name ?? c}>
                        {getCountry(c.toLowerCase())?.flag ?? c}
                      </span>
                    ))}
                  </span>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}
