import type { XbrlFact } from "~/lib/xbrl-facts";

export interface EsefFinancialReportSummary {
  fxoId: string;
  entityName: string;
  fiscalYear: number;
  periodEnd: string;
  currency: string;
  factCount: number;
  mappedFactCount: number;
  sourceFactCount: number;
  filingVersion: number;
  viewerUrl: string;
  sourceUrl: string;
  packageUrl: string;
  errorCount: number;
  warningCount: number;
  dateAdded: string;
}

/** Compatibility aliases for ESEF-specific callers. New country adapters and
 * shared components should use the source-neutral XBRL names directly. */
export type EsefFinancialFact = XbrlFact;
export type {
  XbrlConceptText as EsefConceptLabel,
  XbrlConceptTextSource as EsefConceptTextSource,
  XbrlFactConceptLabels as EsefFactConceptLabels,
} from "~/lib/xbrl-facts";
export {
  xbrlConceptLabel as esefConceptLabel,
  xbrlDecimalsLabel as esefDecimalsLabel,
  xbrlDimensionSummary as esefDimensionSummary,
  xbrlFactConceptLabels as esefFactConceptLabels,
  xbrlFactPeriod as esefFactPeriod,
  xbrlTextValue as esefTextValue,
} from "~/lib/xbrl-facts";

export interface EsefFinancialReport {
  summary: EsefFinancialReportSummary;
  facts: XbrlFact[];
}
