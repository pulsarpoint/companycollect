import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import type { IndustryDetailRow } from "~/lib/queries.server";

export function IndustriesSection({ industries }: { industries: IndustryDetailRow[] }) {
  if (industries.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Industries</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="space-y-2">
          {industries.map((row, i) => (
            <li key={`${row.industry_code}-${i}`} className="flex flex-wrap items-baseline gap-2 text-sm">
              <span className="text-muted-foreground font-mono text-xs">{row.industry_code}</span>
              <span>{row.industry_label}</span>
              {row.is_primary ? <Badge>primary</Badge> : null}
              {row.description_original && row.description_original !== row.industry_label ? (
                <span className="text-muted-foreground text-xs">{row.description_original}</span>
              ) : null}
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}
