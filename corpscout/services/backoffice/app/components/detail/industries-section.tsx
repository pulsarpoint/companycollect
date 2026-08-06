import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import type { IndustryDetailRow, WikidataCompanyRow } from "~/lib/queries.server";
import { EvidencePanel } from "~/components/detail/evidence";

export function IndustriesSection({
  industries,
  wikidata,
}: {
  industries: IndustryDetailRow[];
  wikidata?: WikidataCompanyRow | null;
}) {
  if (industries.length === 0 && !wikidata?.industry_label) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Industries</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="flex flex-col gap-3">
          {industries.map((row, i) => (
            <li key={`${row.industry_code}-${i}`} className="flex flex-col gap-1 text-sm">
              <div className="flex flex-wrap items-baseline gap-2">
                <Badge variant="secondary">registry</Badge>
                <span className="text-muted-foreground font-mono text-xs">
                  {row.industry_code}
                </span>
                <span>{row.industry_label}</span>
                {row.is_primary ? <Badge>primary</Badge> : null}
                {row.description_original && row.description_original !== row.industry_label ? (
                  <span className="text-muted-foreground text-xs">
                    {row.description_original}
                  </span>
                ) : null}
              </div>
              <EvidencePanel evidence={row.evidence ?? []} />
            </li>
          ))}
          {wikidata?.industry_label ? (
            <li className="flex flex-col gap-1 text-sm">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="secondary">Wikidata</Badge>
                <span>{wikidata.industry_label}</span>
              </div>
              <EvidencePanel evidence={wikidata.evidence ?? []} />
            </li>
          ) : null}
        </ul>
      </CardContent>
    </Card>
  );
}
