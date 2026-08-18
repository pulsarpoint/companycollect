import type { LucideIcon } from "lucide-react";
import { Check, CircleHelp, Minus, X } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import {
  FINANCIAL_FILING_STATUS_DEFINITIONS,
  financialFilingSourceLabel,
  type CompanyFinancialFilingStatus,
  type FinancialFilingStatusDefinition,
  type FinancialFilingStatus,
} from "~/lib/financial-filing-status";

type FilingStatusPresentation = FinancialFilingStatusDefinition & {
  status: FinancialFilingStatus;
  icon: LucideIcon;
  badgeVariant: "default" | "secondary" | "destructive" | "outline";
};

const ICON_BY_STATUS: Record<FinancialFilingStatus, LucideIcon> = {
  data_available: Check,
  filed_unstructured: Minus,
  not_submitted: X,
  unknown: CircleHelp,
};

const BADGE_VARIANT_BY_STATUS: Record<
  FinancialFilingStatus,
  FilingStatusPresentation["badgeVariant"]
> = {
  data_available: "default",
  filed_unstructured: "secondary",
  not_submitted: "destructive",
  unknown: "outline",
};

export const FINANCIAL_FILING_STATUS_PRESENTATION: readonly FilingStatusPresentation[] =
  FINANCIAL_FILING_STATUS_DEFINITIONS.map((definition) => ({
    ...definition,
    status: definition.value,
    icon: ICON_BY_STATUS[definition.value],
    badgeVariant: BADGE_VARIANT_BY_STATUS[definition.value],
  }));

function presentation(status: FinancialFilingStatus): FilingStatusPresentation {
  return FINANCIAL_FILING_STATUS_PRESENTATION.find(
    (candidate) => candidate.status === status,
  )!;
}

export function FinancialFilingStatusBadge({
  status,
}: {
  status: FinancialFilingStatus;
}) {
  const definition = presentation(status);
  const { icon: Icon, badgeVariant } = definition;
  return (
    <Badge variant={badgeVariant} title={definition.meaning}>
      <Icon data-icon="inline-start" />
      {definition.label}
    </Badge>
  );
}

export function FinancialFilingStatusGlyph({
  status,
}: {
  status: FinancialFilingStatus;
}) {
  const definition = presentation(status);
  const { icon: Icon, badgeVariant } = definition;
  return (
    <Badge
      variant={badgeVariant}
      className="size-5 p-0"
      aria-label={definition.label}
      title={`${definition.label}: ${definition.meaning}`}
    >
      <Icon />
    </Badge>
  );
}

export function FinancialFilingStatusSummary({
  filing,
}: {
  filing: CompanyFinancialFilingStatus;
}) {
  const metadata = [
    filing.reportPeriodEnd
      ? `Report period ended ${filing.reportPeriodEnd}`
      : null,
    filing.filingRegisteredOn
      ? `registered ${filing.filingRegisteredOn}`
      : null,
    filing.sourceSlug
      ? `Source: ${financialFilingSourceLabel(filing.sourceSlug)}`
      : null,
    filing.sourceFileFormat ? `Format: ${filing.sourceFileFormat}` : null,
    filing.bolagsverketDocumentId
      ? `Document: ${filing.bolagsverketDocumentId}`
      : null,
    filing.observedAt ? `observed ${filing.observedAt.slice(0, 10)}` : null,
  ].filter((value): value is string => value !== null);

  return (
    <div className="flex flex-wrap items-center gap-2">
      <FinancialFilingStatusBadge status={filing.status} />
      {metadata.length > 0 ? (
        <span className="text-muted-foreground text-xs">
          {metadata.join(" · ")}
        </span>
      ) : null}
    </div>
  );
}
