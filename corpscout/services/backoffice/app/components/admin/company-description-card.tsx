import { useState } from "react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
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

type DescriptionLanguage = "en" | "original";

/** The text a proposal shows for the chosen language, falling back to the
 * other side when the proposal only has one — the chip names what is
 * actually displayed. */
export function displayedBlock(
  proposal: DescriptionProposal,
  language: DescriptionLanguage,
): { text: string; chip: string } {
  const english = { text: proposal.english, chip: "en" };
  const original = {
    text: proposal.original,
    chip: proposal.originalLanguage === "" ? "original" : proposal.originalLanguage,
  };
  const preferred = language === "en" ? english : original;
  const fallback = language === "en" ? original : english;
  return preferred.text !== "" ? preferred : fallback;
}

/**
 * Country-agnostic "About the company" card: a menu of the source
 * description proposals it is given (whatever sources that country has),
 * one language at a time with an en/original toggle at the top. Each
 * country's info page derives its own proposals; nothing here knows about
 * specific sources.
 */
export function CompanyDescriptionCard({
  proposals,
}: {
  proposals: DescriptionProposal[];
}) {
  const [language, setLanguage] = useState<DescriptionLanguage>("en");
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
            <div className="flex flex-wrap items-center justify-between gap-3">
              <TabsList>
                {proposals.map((proposal) => (
                  <TabsTrigger key={proposal.key} value={proposal.key}>
                    {menuLabel(proposal)}
                  </TabsTrigger>
                ))}
              </TabsList>
              <div className="flex items-center gap-1">
                <Button
                  type="button"
                  size="sm"
                  variant={language === "en" ? "secondary" : "ghost"}
                  aria-label="Show english descriptions"
                  aria-pressed={language === "en"}
                  onClick={() => setLanguage("en")}
                >
                  en
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant={language === "original" ? "secondary" : "ghost"}
                  aria-label="Show original-language descriptions"
                  aria-pressed={language === "original"}
                  onClick={() => setLanguage("original")}
                >
                  original
                </Button>
              </div>
            </div>
          </CardHeader>
          {proposals.map((proposal) => {
            const block = displayedBlock(proposal, language);
            return (
              <TabsContent key={proposal.key} value={proposal.key}>
                <CardContent className="flex flex-col gap-3">
                  <CardTitle className="flex items-center gap-2 text-sm font-normal text-muted-foreground">
                    <Badge>{proposal.sourceLabel}</Badge>
                    {proposal.meta}
                    <Badge variant="outline">{block.chip}</Badge>
                  </CardTitle>
                  <p className="max-w-[90ch] whitespace-pre-wrap text-sm leading-6">
                    {block.text}
                  </p>
                </CardContent>
              </TabsContent>
            );
          })}
        </Tabs>
      </Card>
    </section>
  );
}
