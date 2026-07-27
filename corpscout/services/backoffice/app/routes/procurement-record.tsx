import { Link } from "react-router";
import type { Route } from "./+types/procurement-record";
import {
  getRegisterByPath,
  getSourceRecord,
  type SourceRow,
} from "~/lib/procurements.server";
import { sourceSlugToPath } from "~/lib/procurement-paths";
import { formatMoneyField } from "~/lib/money";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";

export async function loader({ params }: Route.LoaderArgs) {
  const register = await getRegisterByPath(params.source);
  if (!register) throw new Response("Source not found", { status: 404 });

  const record = await getSourceRecord(register, params.key);
  if (!record) throw new Response("Record not found", { status: 404 });

  return { register, record };
}

export function meta({ loaderData }: Route.MetaArgs) {
  return [
    {
      title: `${loaderData?.record.key ?? "Record"} – ${
        loaderData?.register.register_name ?? "Source"
      }`,
    },
  ];
}

function display(key: string, value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.length > 0 ? value.join(", ") : "—";
  if (typeof value === "object") return JSON.stringify(value, null, 1);
  const money = formatMoneyField(key, value);
  if (money !== null) return money;
  const text = String(value);
  return text === "" ? "—" : text;
}

/** Every field, including the empty ones.
 *
 * The contract detail page drops blanks, because there the question is "what is
 * this contract". Here the question is "what does this register publish", and a
 * field the register defines but left empty is part of that answer — it is the
 * difference between "Doffin does not publish a value" and "Doffin publishes
 * one and this notice omitted it". */
function Fields({ row }: { row: SourceRow }) {
  return (
    <dl className="grid grid-cols-1 gap-x-6 gap-y-2 sm:grid-cols-2 lg:grid-cols-3">
      {Object.entries(row).map(([key, value]) => {
        const text = display(key, value);
        return (
          <div key={key} className="flex flex-col gap-0.5 overflow-hidden">
            <dt className="text-muted-foreground text-xs">{key}</dt>
            <dd
              className={`truncate text-sm ${text === "—" ? "text-muted-foreground" : ""}`}
              title={text}
            >
              {text}
            </dd>
          </div>
        );
      })}
    </dl>
  );
}

export default function ProcurementRecord({ loaderData }: Route.ComponentProps) {
  const { register, record } = loaderData;
  const path = sourceSlugToPath(register.source_slug);

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-2">
        <Button
          variant="ghost"
          size="sm"
          className="self-start"
          nativeButton={false}
          render={<Link to={`/procurements/${path}`} />}
        >
          ← {register.register_name}
        </Button>
        <h1 className="font-mono text-2xl font-semibold tracking-tight">
          {record.key}
        </h1>
        <div className="text-muted-foreground flex flex-wrap items-center gap-2 text-sm">
          <Badge variant="secondary">{register.source_slug}</Badge>
          <span>{register.grain_description}</span>
        </div>
      </div>

      {record.primary ? (
        <Card>
          <CardHeader>
            <CardTitle>
              <code>{record.primary.table}</code>
            </CardTitle>
            <CardDescription>
              Exactly what this register publishes for this record, in its own
              vocabulary. Empty fields are shown rather than hidden: a column the
              register defines and left blank is itself a fact about the source.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Fields row={record.primary.row} />
          </CardContent>
        </Card>
      ) : null}

      {record.related.map((group) => (
        <Card key={group.table}>
          <CardHeader>
            <CardTitle>
              <code>{group.table}</code>
            </CardTitle>
            <CardDescription>
              {group.rows.length} row{group.rows.length === 1 ? "" : "s"} keyed
              on the same {register.notice_key_column}.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            {group.rows.map((row, index) => (
              <div
                key={index}
                className="border-border border-t pt-3 first:border-t-0 first:pt-0"
              >
                <Fields row={row} />
              </div>
            ))}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
