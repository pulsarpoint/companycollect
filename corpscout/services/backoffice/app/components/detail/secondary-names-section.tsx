import type { SecondaryNameRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";

/** Registered alternative names: scoped secondary trade names (särskilt
 * företagsnamn) and foreign-language name registrations. */
export function SecondaryNamesSection({ names }: { names: SecondaryNameRow[] }) {
  if (names.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">
          Also trades as{" "}
          <span className="text-muted-foreground text-sm font-normal">
            {names.length} registered {names.length === 1 ? "name" : "names"}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="divide-y">
          {names.map((n, i) => (
            <li key={i} className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 py-1.5">
              <span className="font-medium">{n.name}</span>
              {n.name_kind === "foreign" ? (
                <Badge variant="outline">foreign-language name</Badge>
              ) : null}
              {n.registered ? (
                <span className="text-muted-foreground text-xs tabular-nums">
                  since {n.registered}
                </span>
              ) : null}
              {n.scope ? (
                <span className="text-muted-foreground w-full text-xs">{n.scope}</span>
              ) : null}
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}
