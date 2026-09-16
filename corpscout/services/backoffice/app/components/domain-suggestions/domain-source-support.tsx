import { companySourceLabel } from "~/components/admin/company-source-strip";
import { Badge } from "~/components/ui/badge";

export const DOMAIN_RANKING_EXPLANATION =
  "Human primary decisions come first. Among connected domains, more distinct supporting sources rank higher. Source priority breaks ties, with Brave first among automated sources. Source support alone does not verify a domain.";

export function DomainSourceSupport({ sources }: { sources: string[] }) {
  return (
    <div className="flex flex-wrap items-center gap-1" aria-label="Domain source support">
      <Badge variant={sources.length > 1 ? "secondary" : "outline"}>
        {sources.length} distinct source{sources.length === 1 ? "" : "s"}
      </Badge>
      {sources.map((source) => (
        <Badge key={source} variant="outline">{companySourceLabel(source)}</Badge>
      ))}
    </div>
  );
}
