import { Link } from "react-router";
import type { Route } from "./+types/procurements";
import {
  countRows,
  listRegisters,
  sourceSlugToPath,
} from "~/lib/procurements.server";
import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

const nf = new Intl.NumberFormat("en-US");

export async function loader() {
  const registers = await listRegisters();
  const counts = await countRows(registers.flatMap((r) => r.source_tables));
  return { registers, counts };
}

export function meta() {
  return [{ title: "Procurement sources – CompanyCollect Backoffice" }];
}

export default function Procurements({ loaderData }: Route.ComponentProps) {
  const { registers, counts } = loaderData;

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold tracking-tight">
          Procurement sources
        </h1>
        <p className="text-muted-foreground max-w-3xl text-sm">
          Each register as itself, in its own shape. A country page answers what
          that country bought; these answer what a register contains and whether
          we read it correctly — which is why they are not filtered to one
          country, and why a record with no matched company still appears.
        </p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {registers.map((register) => {
          const rows = register.source_tables.reduce(
            (total, table) => total + (counts[table] ?? 0),
            0,
          );
          return (
            <Card key={register.source_slug} className="flex flex-col">
              <CardHeader>
                <CardTitle className="text-base">
                  <Link
                    to={`/procurements/${sourceSlugToPath(register.source_slug)}`}
                    className="underline-offset-2 hover:underline"
                  >
                    {register.register_name}
                  </Link>
                </CardTitle>
                <CardDescription>{register.operator}</CardDescription>
              </CardHeader>
              <CardContent className="flex flex-1 flex-col gap-3">
                <div className="flex flex-wrap gap-1">
                  {register.country_codes.map((code) => (
                    <Badge key={code} variant="secondary">
                      {code}
                    </Badge>
                  ))}
                </div>
                <p className="text-muted-foreground line-clamp-4 text-sm">
                  {register.coverage_description}
                </p>
                <dl className="mt-auto grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                  <dt className="text-muted-foreground">Rows</dt>
                  <dd className="tabular-nums">{nf.format(rows)}</dd>
                  <dt className="text-muted-foreground">Grain</dt>
                  <dd className="truncate" title={register.grain_description}>
                    {register.grain_description}
                  </dd>
                  <dt className="text-muted-foreground">Licence</dt>
                  <dd className="truncate" title={register.licence}>
                    {register.licence}
                  </dd>
                </dl>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
