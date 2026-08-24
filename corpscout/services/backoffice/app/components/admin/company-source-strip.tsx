import { Badge } from "~/components/ui/badge";
import { EMPTY_VALUE } from "~/components/admin/definition-list";
import {
  PROFILE_SOURCES,
  profileSourceLabel,
} from "~/lib/se-company-info-filters";

/**
 * Source tokens that only ever appear INSIDE one datatype, so they earn no
 * letter in the profile alphabet and are not in `PROFILE_SOURCES`.
 *
 * They are still registers a reader has to be able to name: the Financial tab
 * labels its per-source cards with `source_id`, and the Domains tab with
 * `company_domains.source_names`. Two of them are the same register under
 * another spelling ('esef_filing' is ESEF suggesting a website;
 * 'bolagsverket-annual-accounts' is Bolagsverket's own filings view), so they
 * are mapped onto the catalog's names rather than shown raw -- one register
 * must not read as two on the same page.
 */
const DATATYPE_SOURCE_LABELS: Record<string, string> = {
  "bolagsverket-annual-accounts": "Bolagsverket",
  esef_filing: "ESEF",
  common_crawl_identity: "Common Crawl",
};

/** What a reader calls one source token, wherever it came from. The profile
 * catalog answers first, so a register keeps ONE name across all five tabs. */
export function companySourceLabel(source: string): string {
  return DATATYPE_SOURCE_LABELS[source] ?? profileSourceLabel(source);
}

/** The catalog's canonical order, by LABEL -- what the strip sorts on, since
 * two tokens can share one label ('esef' and 'esef_filing' are both ESEF). */
const CATALOG_ORDER = new Map<string, number>(
  PROFILE_SOURCES.map((source, index) => [source.label, index]),
);

/**
 * The distinct registers behind a tab, named and ordered for reading.
 *
 * Deduplicated by LABEL, not by token, so a Domains tab carrying both 'esef'
 * and 'esef_filing' says "ESEF" once. Ordered by the profile catalog first
 * (B, S, E, W -- the same order the list page's letters are in), then anything
 * it does not name alphabetically after them, so a strip never reshuffles
 * itself because a source arrived in a different order from ClickHouse.
 */
export function companySourceLabels(sources: readonly string[]): string[] {
  const labels = [...new Set(sources.map(companySourceLabel))];
  return labels.sort((left, right) => {
    const leftRank = CATALOG_ORDER.get(left) ?? PROFILE_SOURCES.length;
    const rightRank = CATALOG_ORDER.get(right) ?? PROFILE_SOURCES.length;
    return leftRank - rightRank || left.localeCompare(right);
  });
}

/**
 * The one-line "which registers is this tab built from" strip that sits at the
 * top of every `/admin/se/company/:id/*` tab.
 *
 * Full names, not the list page's letters: a detail page has the room, and the
 * five strips together are what makes the list's `profile_sources` letters
 * checkable. Every tab derives its own from data it already loaded, so this
 * costs no query -- see each tab component for which field it reads.
 */
export function CompanySourceStrip({
  sources,
}: {
  sources: readonly string[];
}) {
  const labels = companySourceLabels(sources);
  return (
    <div
      className="flex flex-wrap items-center gap-2 text-xs"
      data-source-strip={labels.join(",")}
    >
      <span className="text-muted-foreground uppercase tracking-wide">
        Sources
      </span>
      {labels.length === 0
        ? EMPTY_VALUE
        : labels.map((label) => (
            <Badge key={label} variant="outline">
              {label}
            </Badge>
          ))}
    </div>
  );
}
