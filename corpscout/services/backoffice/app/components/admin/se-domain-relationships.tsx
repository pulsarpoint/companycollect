import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "~/components/ui/collapsible";
import type { SeDomainRelationship } from "~/lib/se-company-domains.server";

function sourceHref(value: string): string | undefined {
  try {
    const url = new URL(value);
    return ["http:", "https:"].includes(url.protocol) ? url.href : undefined;
  } catch {
    return undefined;
  }
}

export function SeDomainRelationships({ relationships }: { relationships: SeDomainRelationship[] }) {
  if (relationships.length === 0) return null;
  return (
    <section className="flex flex-col gap-4" aria-label="Relationships and source mentions">
      <div className="flex flex-col gap-1">
        <h2 className="text-lg font-medium">Relationships and source mentions</h2>
        <p className="text-sm text-muted-foreground">
          Explanations generated from reports and captured website pages. Dates identify the report period or page capture;
          these statements have not been independently verified.
        </p>
      </div>
      {relationships.flatMap((row) => row.statements.map((statement, index) => (
        <Card key={`${row.source_kind}:${row.source_document_id}:${row.registrable_domain}:${index}`}>
          <CardHeader>
            <CardTitle>{statement.related_entity_name || row.registrable_domain}</CardTitle>
            <CardDescription className="flex flex-wrap items-center gap-2">
              <span>{row.registrable_domain}</span>
              <span>{row.source_kind === "website" ? `Website captured ${row.captured_at.slice(0, 10)}` : `Report period ending ${row.period_end}`}</span>
              {!statement.relationship_supported ? <Badge variant="outline">Connection unclear</Badge> : null}
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <p>{statement.description}</p>
            <Collapsible>
              <CollapsibleTrigger className="text-sm underline underline-offset-4">
                Show source evidence
              </CollapsibleTrigger>
              <CollapsibleContent className="flex flex-col gap-3 pt-3">
                {statement.evidence.map((quote, quoteIndex) => {
                  const evidence = row.evidence.find((item) => item.id === quote.evidence_id);
                  return (
                    <div className="flex flex-col gap-2" key={`${quote.evidence_id}:${quoteIndex}`}>
                      <blockquote className="border-l-2 pl-3 text-sm">{quote.quote}</blockquote>
                      {evidence ? <>
                        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
                          <span>Source location: {evidence.locations.map((location) => [location.page_id || location.xpath, location.link_id].filter(Boolean).join(" · ")).join(", ")}</span>
                          {evidence.page_title ? <span>{evidence.page_title}</span> : null}
                          {evidence.fetched_at ? <span>Captured {evidence.fetched_at}</span> : null}
                          {evidence.source_url && sourceHref(evidence.source_url) ? <a href={sourceHref(evidence.source_url)} target="_blank" rel="noreferrer" className="underline">Open source page</a> : null}
                          {sourceHref(evidence.url) ? <a href={sourceHref(evidence.url)} target="_blank" rel="noreferrer" className="underline">Referenced URL</a> : null}
                        </div>
                        <Collapsible>
                          <CollapsibleTrigger className="text-xs underline underline-offset-4">Read surrounding context</CollapsibleTrigger>
                          <CollapsibleContent className="flex flex-col gap-2 pt-2">
                            {evidence.source_context.headings.length ? <p className="text-sm text-muted-foreground">Nearby heading: {evidence.source_context.headings.join(" · ")}</p> : null}
                            <p className="whitespace-pre-wrap text-sm text-muted-foreground">{evidence.source_context.text}</p>
                            {evidence.source_context.attributes?.map((attribute) => <p className="break-all text-xs text-muted-foreground" key={attribute}>Recorded link destination: {attribute}</p>)}
                            {evidence.source_context.reading_order === "document_order_unverified" ? <p className="text-xs text-muted-foreground">Extracted text may read in a different order from the original page layout.</p> : null}
                            {evidence.source_context.truncated ? <p className="text-xs text-muted-foreground">This context excerpt is truncated.</p> : null}
                          </CollapsibleContent>
                        </Collapsible>
                      </> : null}
                    </div>
                  );
                })}
                {row.source_kind === "esef" && sourceHref(row.source_url) ? <a href={sourceHref(row.source_url)} target="_blank" rel="noreferrer" className="text-sm underline underline-offset-4">Open original report</a> : null}
              </CollapsibleContent>
            </Collapsible>
          </CardContent>
        </Card>
      )))}
    </section>
  );
}
