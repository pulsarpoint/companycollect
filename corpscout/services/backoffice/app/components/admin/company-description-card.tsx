import { Badge } from "~/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "~/components/ui/tabs";
import type { DescriptionProposal } from "~/lib/se-company-info-payload";

function DescriptionBlock({
  text,
  language,
}: {
  text: string;
  language: string;
}) {
  if (text === "") return null;
  return (
    <div className="flex flex-col gap-1.5">
      <Badge variant="outline" className="w-fit">
        {language === "" ? "original" : language}
      </Badge>
      <p className="max-w-[90ch] whitespace-pre-wrap text-sm leading-6">
        {text}
      </p>
    </div>
  );
}

/**
 * Country-agnostic "About the company" card: a menu of the source
 * description proposals it is given (whatever sources that country has),
 * english shown first, the original language underneath. Each country's
 * info page derives its own proposals; nothing here knows about specific
 * sources.
 */
export function CompanyDescriptionCard({
  proposals,
}: {
  proposals: DescriptionProposal[];
}) {
  if (proposals.length === 0) return null;
  // A source can propose several descriptions (e.g. one per fiscal year);
  // disambiguate its menu entries with the proposal's meta line.
  const sourceCounts = new Map<string, number>();
  for (const proposal of proposals) {
    sourceCounts.set(
      proposal.source,
      (sourceCounts.get(proposal.source) ?? 0) + 1,
    );
  }
  const menuLabel = (proposal: DescriptionProposal) =>
    (sourceCounts.get(proposal.source) ?? 0) > 1
      ? `${proposal.sourceLabel} · ${proposal.meta}`
      : proposal.sourceLabel;

  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-col gap-1">
        <h2 className="text-lg font-semibold tracking-tight">
          About the company
        </h2>
        <p className="text-sm text-muted-foreground">
          What each source proposes as the company description.
        </p>
      </div>
      <Card>
        <Tabs defaultValue={proposals[0].key}>
          <CardHeader>
            <TabsList>
              {proposals.map((proposal) => (
                <TabsTrigger key={proposal.key} value={proposal.key}>
                  {menuLabel(proposal)}
                </TabsTrigger>
              ))}
            </TabsList>
          </CardHeader>
          {proposals.map((proposal) => (
            <TabsContent key={proposal.key} value={proposal.key}>
              <CardContent className="flex flex-col gap-4">
                <CardTitle className="flex items-center gap-2 text-sm font-normal text-muted-foreground">
                  <Badge>{proposal.sourceLabel}</Badge>
                  {proposal.meta}
                </CardTitle>
                {proposal.english !== "" ? (
                  <DescriptionBlock text={proposal.english} language="en" />
                ) : null}
                <DescriptionBlock
                  text={proposal.original}
                  language={proposal.originalLanguage}
                />
              </CardContent>
            </TabsContent>
          ))}
        </Tabs>
      </Card>
    </section>
  );
}
