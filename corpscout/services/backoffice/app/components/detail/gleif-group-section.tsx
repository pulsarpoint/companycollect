import { Link } from "react-router";
import type { GleifEntityRow, GleifRelationshipRow } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";

/** GLEIF no-parent exception reasons → human text. */
const OWNERSHIP_EXCEPTION_LABELS: Record<string, string> = {
  NATURAL_PERSONS: "held directly by natural persons",
  NO_KNOWN_PERSON: "no known controlling entity",
  NON_CONSOLIDATING: "no consolidating parent",
  NO_LEI: "parent exists but has no LEI",
  NON_PUBLIC: "parent not disclosed",
  CONSENT_NOT_OBTAINED: "parent not disclosed (consent not obtained)",
};

function ownershipSummary(entity: GleifEntityRow): string | null {
  const reasons = entity.ownership_exceptions
    .split(",")
    .map((r) => r.trim())
    .filter((r) => r !== "");
  if (reasons.length === 0) return null;
  const labels = Array.from(
    new Set(reasons.map((r) => OWNERSHIP_EXCEPTION_LABELS[r] ?? r.toLowerCase())),
  );
  return labels.join("; ");
}

/** GLEIF relationship_type → short human label. Unknown types fall back to
 * the raw vocabulary term so new GLEIF types are never hidden. */
const RELATIONSHIP_LABELS: Record<string, string> = {
  IS_DIRECTLY_CONSOLIDATED_BY: "direct parent",
  IS_ULTIMATELY_CONSOLIDATED_BY: "ultimate parent",
  IS_INTERNATIONAL_BRANCH_OF: "branch of",
  IS_FUND_MANAGED_BY: "fund manager",
  "IS_FUND-MANAGED_BY": "fund manager",
  IS_SUBFUND_OF: "subfund of",
  IS_FEEDER_TO: "feeder to",
};

const SUBSIDIARY_LABELS: Record<string, string> = {
  IS_DIRECTLY_CONSOLIDATED_BY: "direct subsidiary",
  IS_ULTIMATELY_CONSOLIDATED_BY: "in group (ultimate)",
  IS_INTERNATIONAL_BRANCH_OF: "international branch",
  IS_FUND_MANAGED_BY: "managed fund",
  "IS_FUND-MANAGED_BY": "managed fund",
  IS_SUBFUND_OF: "subfund",
  IS_FEEDER_TO: "feeder fund",
};

function relationLabel(row: GleifRelationshipRow): string {
  const table = row.direction === "parent" ? RELATIONSHIP_LABELS : SUBSIDIARY_LABELS;
  return table[row.relationship_type] ?? row.relationship_type;
}

function EntityLine({ row, countryCode }: { row: GleifRelationshipRow; countryCode: string }) {
  const name = row.name || row.other_lei;
  return (
    <li className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 py-1.5">
      {row.local_id ? (
        <Link to={`/company/${countryCode}/${row.local_id}`} className="font-medium hover:underline">
          {name}
        </Link>
      ) : (
        <span className="font-medium">{name}</span>
      )}
      <Badge variant="outline">{relationLabel(row)}</Badge>
      {row.jurisdiction ? (
        <span className="text-muted-foreground text-xs">{row.jurisdiction}</span>
      ) : null}
      <span className="text-muted-foreground font-mono text-[11px]">{row.other_lei}</span>
    </li>
  );
}

/** Corporate-group links from GLEIF consolidation data, split into parents
 * (who consolidates this company) and subsidiaries/branches (whom it
 * consolidates), plus the LEI entity facts: registration status, category,
 * headquarters location, and the GLEIF ownership declaration when no parent
 * is reported. Entities registered in this country link internally. */
export function GleifGroupSection({
  relationships,
  entity,
  countryCode,
}: {
  relationships: GleifRelationshipRow[];
  entity: GleifEntityRow | null;
  countryCode: string;
}) {
  if (relationships.length === 0 && entity === null) return null;
  // A subsidiary usually appears under BOTH direct and ultimate
  // consolidation; keep one line per entity, preferring the direct link.
  const seen = new Set<string>();
  const deduped: GleifRelationshipRow[] = [];
  for (const row of [...relationships].sort(
    (a, b) => Number(b.relationship_type === "IS_DIRECTLY_CONSOLIDATED_BY")
      - Number(a.relationship_type === "IS_DIRECTLY_CONSOLIDATED_BY"),
  )) {
    const key = `${row.direction}:${row.other_lei}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(row);
  }
  const parents = deduped.filter((r) => r.direction === "parent");
  const subsidiaries = deduped.filter((r) => r.direction === "subsidiary");
  const ownership = entity ? ownershipSummary(entity) : null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex flex-wrap items-baseline gap-x-2 text-base">
          Corporate group{" "}
          <span className="text-muted-foreground text-sm font-normal">
            GLEIF consolidation links
          </span>
          {entity && entity.lei_status !== "ISSUED" ? (
            <Badge variant="outline" className="text-amber-600">
              LEI {entity.lei_status.toLowerCase()}
            </Badge>
          ) : null}
          {entity && entity.category && entity.category !== "GENERAL" ? (
            <Badge variant="outline">{entity.category.toLowerCase().replaceAll("_", " ")}</Badge>
          ) : null}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {entity ? (
          <div className="text-muted-foreground space-y-0.5 text-xs">
            <div>
              Headquarters abroad:{" "}
              <span className="text-foreground font-medium">
                {entity.hq_abroad ? "true" : "false"}
              </span>
              {entity.hq_abroad && entity.hq_country ? (
                <span> ({entity.hq_country})</span>
              ) : null}
            </div>
            {ownership && parents.length === 0 ? (
              <div>
                Ownership (GLEIF declaration):{" "}
                <span className="text-foreground font-medium">{ownership}</span>
              </div>
            ) : null}
          </div>
        ) : null}
        {parents.length > 0 ? (
          <div>
            <div className="text-muted-foreground mb-1 text-xs font-medium uppercase">Parents</div>
            <ul className="divide-y">
              {parents.map((row) => (
                <EntityLine key={`p-${row.other_lei}`} row={row} countryCode={countryCode} />
              ))}
            </ul>
          </div>
        ) : null}
        {subsidiaries.length > 0 ? (
          <div>
            <div className="text-muted-foreground mb-1 text-xs font-medium uppercase">
              Subsidiaries &amp; branches ({subsidiaries.length})
            </div>
            <ul className="divide-y">
              {subsidiaries.map((row) => (
                <EntityLine key={`s-${row.other_lei}`} row={row} countryCode={countryCode} />
              ))}
            </ul>
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
