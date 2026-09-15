export const COMPANY_ACTION_AREAS = [
  {
    value: "info", label: "Company info",
    description: "Company identity and descriptions",
    jobs: { sync: "se_company_basic_info_sync_job", process: "se_company_basic_info_refresh_job" },
    sync: "Sync company information from ingested SCB, Bolagsverket, ESEF, Wikidata and Ratsit data. No LLM calls or publishing.",
    process: "Sync company information, combine source descriptions with the selected LLM, then fold and publish company information.",
  },
  {
    value: "finance", label: "Finance",
    description: "Financial periods and reported figures",
    jobs: { sync: "se_company_financial_sync_job", process: "se_company_financial_refresh_job" },
    sync: "Prepare Ratsit USD figures and sync ingested Bolagsverket, comparative filings, ESEF and Ratsit financial data. No publishing.",
    process: "Sync financial inputs, then fold and publish financial periods using source precedence. No LLM calls.",
  },
  {
    value: "addresses", label: "Addresses",
    description: "Normalized addresses and coordinates",
    jobs: { sync: "se_company_address_sync_job", process: "se_company_address_refresh_job" },
    sync: "Sync addresses from ingested SCB, Bolagsverket, Ratsit and ESEF data, then normalize the inputs. No geocoding or publishing.",
    process: "Sync and normalize address inputs, warm the geocoding cache using existing OSM data, then fold and publish addresses. No LLM calls.",
  },
  {
    value: "people", label: "People",
    description: "People, roles and matches across sources",
    jobs: { sync: "se_company_person_sync_job", process: "se_company_person_refresh_job" },
    sync: "Sync people from ingested Bolagsverket, ESEF, Wikidata and Ratsit data into the normalized input table and update its hashes. No LLM calls or publishing.",
    process: "Sync people and input hashes, match with the selected LLM and People prompt, then fold and publish.",
  },
  {
    value: "domains", label: "Domains",
    description: "Company websites and domain associations",
    jobs: { sync: "se_company_domain_sync_job", process: "se_company_domain_refresh_job" },
    sync: "Sync domain evidence from ingested Wikidata, ESEF filings and Common Crawl matches, plus source precedence. No crawling, LLM calls or publishing.",
    process: "Sync domain evidence, resolve associations using source precedence, then publish with change history. Optionally verify uncertain or conflicting associations with the selected LLM and Domain prompt. Reviewer decisions take priority.",
  },
] as const;

export type CompanyActionArea = typeof COMPANY_ACTION_AREAS[number]["value"];
export type CompanyActionOperation = "sync" | "process";
export type CompanyActionSelection = { area: CompanyActionArea; operation: CompanyActionOperation };
export const ACTIVE_COMPANY_RUN_STATUSES = ["QUEUED", "NOT_STARTED", "STARTING", "STARTED", "CANCELING", "MANAGED"] as const;

export interface CompanyActionResult {
  ok: boolean;
  error: string;
  runId?: string;
  runUrl?: string | null;
  status?: string;
}
