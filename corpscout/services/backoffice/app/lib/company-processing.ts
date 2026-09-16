import { ACTIVE_COMPANY_RUN_STATUSES, type CompanyActionArea, type CompanyActionOperation } from "~/lib/company-actions";

export interface ProcessingRun {
  runId: string;
  area: CompanyActionArea;
  operation: CompanyActionOperation;
  status: string;
  createdAt: number;
  startTime: number | null;
  endTime: number | null;
  operator: string;
  activeSteps: string[];
  runUrl: string | null;
}

export interface ProcessingSnapshot {
  runs: ProcessingRun[];
  lastSuccess: Partial<Record<string, ProcessingRun>>;
  errors: Partial<Record<CompanyActionArea, string>>;
  checkedAt: number;
}

// Keep the last known rows during an outage; the page marks the affected
// workflow as unavailable and disables its actions until a fresh read succeeds.
export function mergeProcessingSnapshot(previous: ProcessingSnapshot, next: ProcessingSnapshot): ProcessingSnapshot {
  return {
    ...next,
    runs: [...new Map([
      ...previous.runs.filter((run) => next.errors[run.area]), ...next.runs,
    ].map((run) => [run.runId, run])).values()],
    lastSuccess: {
      ...Object.fromEntries(Object.entries(previous.lastSuccess).filter(([, run]) => run && next.errors[run.area])),
      ...next.lastSuccess,
    },
  };
}

export function isProcessingActive(status: string): boolean {
  return (ACTIVE_COMPANY_RUN_STATUSES as readonly string[]).includes(status);
}

export function processingStatusLabel(status: string): string {
  return ({ QUEUED: "Queued", NOT_STARTED: "Not started", STARTING: "Starting", STARTED: "Running", CANCELING: "Stopping", CANCELED: "Canceled", SUCCESS: "Succeeded", FAILURE: "Failed", MANAGED: "Managed" } as Record<string, string>)[status] ?? status;
}

export function processingTime(timestamp: number): string {
  return new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Stockholm", day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" }).format(new Date(timestamp * 1000));
}

export function processingDuration(start: number, end: number): string {
  const seconds = Math.max(0, Math.floor(end - start));
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

export function processingStepLabel(step: string): string {
  if (step.includes("freshness") || step.includes("snapshot_fresh")) return "Check data freshness";
  if (step.includes("tables_non_empty")) return "Check published data";
  if (step === "se_address_geocodes_warm") return "Geocode addresses";
  if (step === "se_ratsit_financial_periods_usd") return "Convert Ratsit figures to USD";
  if (step.endsWith("_suggestions_llm")) return "Synthesize descriptions";
  if (step.endsWith("_match_input")) return "Update matching inputs and hashes";
  if (step.endsWith("_match")) return "Match people with the LLM";
  if (step.endsWith("_normalize")) return "Normalize inputs";
  if (step === "se_company_domain_verification") return "Score domains with the LLM";
  if (step === "se_company_domain_publish") return "Fold and publish domains";
  if (step === "se_company_domain_precedence_clickhouse") return "Update source precedence";
  if (step.endsWith("_publish")) return "Fold and publish";
  const source = step.split("_suggestions_")[1];
  if (source) return `Sync ${({ scb: "SCB", bolagsverket: "Bolagsverket", bolagsverket_comparative: "comparative filings", ratsit: "Ratsit", esef: "ESEF", esef_filing: "ESEF filings", common_crawl_identity: "Common Crawl matches", brave: "Brave answers", wikidata: "Wikidata" } as Record<string, string>)[source] ?? source}`;
  return step.replaceAll("_", " ");
}
