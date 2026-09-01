import { useState, type ReactNode } from "react";
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
 * actually displayed, and `side` says which of the proposal's two blocks it
 * really is (the fallback means the chip alone cannot answer that). */
export function displayedBlock(
  proposal: DescriptionProposal,
  language: DescriptionLanguage,
): { text: string; chip: string; side: "english" | "original" } {
  const english = { text: proposal.english, chip: "en", side: "english" as const };
  const original = {
    text: proposal.original,
    chip: proposal.originalLanguage === "" ? "original" : proposal.originalLanguage,
    side: "original" as const,
  };
  const preferred = language === "en" ? english : original;
  const fallback = language === "en" ? original : english;
  return preferred.text !== "" ? preferred : fallback;
}

/** Which published column the block on screen belongs to. The original block
 * is the swedish column only when it really is swedish; an original in any
 * other language (or an unmarked one, as Wikidata's descriptions are) belongs
 * to the same column english does. */
function shownField(
  proposal: DescriptionProposal,
  side: "english" | "original",
): "description" | "description_sv" {
  return side === "original" && proposal.originalLanguage === "sv"
    ? "description_sv"
    : "description";
}

/** What an option's action slot is told: the proposal, and which field the
 * text currently under the reviewer's eyes would decide. */
export interface DescriptionShown {
  field: "description" | "description_sv";
  text: string;
}

/**
 * Country-agnostic "About the company" card: a menu of the source
 * description proposals it is given (whatever sources that country has),
 * one language at a time with an en/original toggle at the top. Each
 * country's info page derives its own proposals; nothing here knows about
 * specific sources.
 *
 * `renderAction` is the slot a page hangs its own decision UI in (a "Use
 * this" form, an inline editor): it is handed the proposal and the block
 * actually on screen, so the page never has to re-derive which language — and
 * therefore which field — the reviewer is looking at.
 */
export function CompanyDescriptionCard({
  proposals,
  renderAction,
}: {
  proposals: DescriptionProposal[];
  renderAction?: (
    proposal: DescriptionProposal,
    shown: DescriptionShown,
  ) => ReactNode;
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
              // keepMounted: an option's action slot can hold a form the
              // reviewer half-filled (an inline editor); unmounting it on every
              // tab switch would throw that away, and it keeps every option's
              // form in the document rather than only the open one's.
              <TabsContent keepMounted key={proposal.key} value={proposal.key}>
                <CardContent className="flex flex-col gap-3">
                  <CardTitle className="flex items-center gap-2 text-sm font-normal text-muted-foreground">
                    <Badge>{proposal.sourceLabel}</Badge>
                    {proposal.meta}
                    <Badge variant="outline">{block.chip}</Badge>
                  </CardTitle>
                  <p className="max-w-[90ch] whitespace-pre-wrap text-sm leading-6">
                    {block.text}
                  </p>
                  {renderAction?.(proposal, {
                    field: shownField(proposal, block.side),
                    text: block.text,
                  })}
                </CardContent>
              </TabsContent>
            );
          })}
        </Tabs>
      </Card>
    </section>
  );
}
