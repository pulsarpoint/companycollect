import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import type { DomainRow } from "~/lib/queries.server";

export function TechnologyDomainsSection({
  domains,
  selectedDomain,
}: {
  domains: DomainRow[];
  selectedDomain: string;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Web presence</CardTitle>
        <CardDescription>
          Company/domain associations are listed here for context. Every
          technology section above is scoped to the domain selected in the
          Technology header.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {domains.map((domain) => (
          <div key={domain.domain} className="flex flex-col gap-2">
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-medium">{domain.domain}</span>
              {domain.domain === selectedDomain ? (
                <Badge>Selected</Badge>
              ) : null}
              {domain.review_status ? (
                <Badge
                  variant={
                    domain.review_status === "rejected"
                      ? "destructive"
                      : "outline"
                  }
                >
                  {domain.review_status.replaceAll("_", " ")}
                </Badge>
              ) : null}
              {(domain.source_names ?? [domain.domain_source]).map((source) => (
                <Badge key={source} variant="outline">
                  {source}
                </Badge>
              ))}
            </div>
            {domain.website_url ? (
              <a
                href={domain.website_url}
                target="_blank"
                rel="noreferrer"
                className="text-muted-foreground break-all text-sm underline underline-offset-2"
              >
                {domain.website_url}
              </a>
            ) : null}
          </div>
        ))}
      </CardContent>
    </Card>
  );
}
