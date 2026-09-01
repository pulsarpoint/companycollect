import { useState } from "react";
import { ChevronLeftIcon, ChevronRightIcon } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
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
 * Carousel over every source's proposed company description: english first
 * when the source has it, the original language underneath. Only the current
 * slide renders; prev/next step through the proposals.
 */
export function CompanyDescriptionCarousel({
  proposals,
}: {
  proposals: DescriptionProposal[];
}) {
  const [index, setIndex] = useState(0);
  if (proposals.length === 0) return null;
  const slide = proposals[Math.min(index, proposals.length - 1)];

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
        <CardHeader>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <CardTitle className="flex items-center gap-2">
              <Badge>{slide.sourceLabel}</Badge>
              <span className="text-sm font-normal text-muted-foreground">
                {slide.meta}
              </span>
            </CardTitle>
            <div className="flex items-center gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                aria-label="Previous description"
                disabled={index === 0}
                onClick={() => setIndex((current) => Math.max(0, current - 1))}
              >
                <ChevronLeftIcon />
              </Button>
              <span className="text-sm tabular-nums text-muted-foreground">
                {Math.min(index, proposals.length - 1) + 1} /{" "}
                {proposals.length}
              </span>
              <Button
                type="button"
                variant="outline"
                size="sm"
                aria-label="Next description"
                disabled={index >= proposals.length - 1}
                onClick={() =>
                  setIndex((current) =>
                    Math.min(proposals.length - 1, current + 1),
                  )
                }
              >
                <ChevronRightIcon />
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {slide.english !== "" ? (
            <DescriptionBlock text={slide.english} language="en" />
          ) : null}
          <DescriptionBlock
            text={slide.original}
            language={slide.originalLanguage}
          />
        </CardContent>
      </Card>
    </section>
  );
}
