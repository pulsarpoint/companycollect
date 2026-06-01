export interface StatsResponse {
  total_companies: number;
  total_domains: number;
  active_domains: number;
  pending_review: number;
  pending_raw_inputs: number;
  enabled_sources: number;
}

export interface RawInput {
  id: string;
  source: string;
  name: string;
  native_id: string;
  status: string;
  state: string;
  has_suggestion: boolean;
  translation_status?: "pending" | "translating" | "translated" | "failed";
  created_at: string;
}

export interface RawInputListResponse {
  items: RawInput[];
  total: number;
  page: number;
  limit: number;
}

export interface BrregRawRecordListItem {
  id: string;
  organization_number: string;
  organization_name: string;
  website: string;
  registration_status: string;
  payload_hash: string;
  is_current: boolean;
  first_seen_at: string;
  last_seen_at: string;
  lifecycle_state: string;
  translation_status: string;
  domain_status: string;
  financial_status: string;
  enhanced_status: string;
  best_domain: string;
  task_statuses: Record<string, string>;
  task_errors: Record<string, unknown>;
}

export interface BrregRawRecordListResponse {
  items: BrregRawRecordListItem[];
  total: number;
  page: number;
  limit: number;
}

export interface BrregRawRecordDetail extends BrregRawRecordListItem {
  country_iso2: string;
  raw_payload: Record<string, unknown>;
  raw_metadata: Record<string, unknown>;
  translation_result: Record<string, unknown>;
  domain_result: Record<string, unknown>;
  financial_result: Record<string, unknown>;
  enhanced_result: Record<string, unknown>;
  tasks: Array<Record<string, unknown>>;
}

interface BrregTaskStateAssetSummary {
  asset: string;
  raw_records_current: number;
  task_no_state: number;
  task_pending: number;
  task_running_active: number;
  task_running_stale: number;
  task_failed_retryable: number;
  task_failed_terminal: number;
  task_succeeded: number;
  task_skipped: number;
  task_eligible_now: number;
  artifact_succeeded: number;
  artifact_skipped: number;
  artifact_failed: number;
  artifact_missing: number;
}

export interface BrregTaskStateAction {
  key: string;
  label: string;
  description: string;
  task_type?: string;
  asset?: string;
  state: BrregTaskStateAssetSummary;
}

interface BrregTaskStateResultTable {
  name: string;
  label: string;
  count: number;
  href?: string;
}

export interface BrregTaskStateResponse {
  source: "brreg";
  updated_at: string;
  actions: BrregTaskStateAction[];
  result_tables: BrregTaskStateResultTable[];
}

export interface RawInputDetail {
  id: string;
  source: string;
  name: string;
  native_id: string;
  status: string;
  state: string;
  company_type?: string;
  registration_status?: string;
  website?: string;
  country_iso2?: string;
  run_id?: string;
  processing_attempts: number;
  processing_error?: string;
  payload_hash: string;
  raw_payload: Record<string, unknown>;
  translation_status?: "pending" | "translating" | "translated" | "failed";
  translation_attempts?: number;
  translation_error?: string;
  translation_model?: string;
  translation_prompt_version?: string;
  translation_fx_source?: string;
  translation_fx_rate_date?: string;
  translated_at?: string;
  first_seen_at: string;
  last_seen_at: string;
  processed_at?: string;
  created_at: string;
  updated_at: string;
}

type Signal = "registry_website" | "wikidata" | "certsh" | "whois" | "search" | "manual_upload";

export interface ReviewCandidate {
  id: string;
  company_id: string;
  domain_id: string;
  relationship_type: string;
  status: string;
  signal: Signal;
  confidence: number;
  evidence: Record<string, unknown> | null;
  first_seen_at: string;
  last_seen_at: string;
  company_name: string;
  domain: string;
}

export interface ReviewListResponse {
  items: ReviewCandidate[];
  page: number;
  limit: number;
  total: number;
}

export interface CompanySuggestion {
  id: string;
  target_company_id: string | null;
  created_company_id: string | null;
  source_id: string;
  source_type: string;
  source_input_table: string;
  source_input_id: string;
  source_native_id: string | null;
  source_payload_hash: string | null;
  confidence: number | null;
  status: string;
  created_at: string;
  company_name: string;
  proposed_name: string | null;
  registration_number: string | null;
  lei: string | null;
  country_id: string | null;
  total_count: number;
  pending_count: number;
  applied_count: number;
  rejected_count: number;
}

export interface CompanySuggestionListResponse {
  items: CompanySuggestion[];
  page: number;
  limit: number;
  total: number;
}

type SourceConfig = Record<string, unknown>;

interface SyncCheckpoint {
  cursor: string;
  last_completed_at?: string;
  updated_at: string;
  mode: "bulk" | "incremental" | "none";
  bulk_date?: string;
}

export interface DataSource {
  id: string;
  name: string;
  display_name: string | null;
  sync_checkpoint?: SyncCheckpoint;
  description: string | null;
  source_group: string;
  input_table_name: string;
  enabled: boolean;
  schedule_enabled: boolean;
  schedule_kind: "manual" | "interval" | "cron" | "event";
  schedule_expression: string | null;
  config: SourceConfig;
  last_started_at: string | null;
  last_success_at: string | null;
  last_failed_at: string | null;
  next_scheduled_at: string | null;
  download_workflow_registered: boolean;
  manual_trigger_available: boolean;
  last_source_marker_type: string | null;
  last_source_marker: string | null;
  last_source_modified_at: string | null;
  last_error: string | null;
  consecutive_failures: number;
  country_id: string | null;
  capabilities: string[];
  requires_translation: boolean;
  created_at: string;
  updated_at: string;
}

export interface Country {
  id: string;
  name: string;
  iso_alpha2: string;
}

interface Estimate {
  value?: number;
  currency?: string;
  year?: number;
  source?: string;
  label?: string;
  min?: number | null;
  max?: number | null;
}

// PostgREST view types

export interface VCompany {
  id: string;
  name: string;
  short_name: string | null;
  registration_number: string | null;
  lei: string | null;
  status: string;
  website: string | null;
  short_description: string | null;
  description: string | null;
  founded_year: number | null;
  employee_estimate: Estimate;
  revenue_estimate: Estimate;
  employee_count: number | null;
  revenue_usd: number | null;
  ownership: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  country_id: string;
  country_name: string;
  country_iso2: string;
  primary_source: string | null;
  primary_source_display_name: string | null;
  domain_count: number;
  headquarters_location: string | null;
}

export interface VCompanyLocation {
  id: string;
  company_id: string;
  location_type: "headquarters" | "registered_address" | "office";
  label: string | null;
  address_line1: string | null;
  address_line2: string | null;
  city: string | null;
  region: string | null;
  postal_code: string | null;
  country: string | null;
  country_code: string | null;
  latitude: number | null;
  longitude: number | null;
  source: string;
  confidence: number | null;
  evidence: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface VCompanyPhone {
  id: string;
  company_id: string;
  phone: string;
  description: string | null;
  purpose: string;
  source: string;
  confidence: number | null;
  evidence: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface VCompanyEmail {
  id: string;
  company_id: string;
  email: string;
  description: string | null;
  purpose: string;
  name: string | null;
  source: string;
  confidence: number | null;
  evidence: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface VCompanyIndustry {
  id: string;
  company_id: string;
  industry: string;
  source: string;
  confidence: number | null;
  created_at: string;
}

export interface VCompanyMarket {
  id: string;
  company_id: string;
  market: string;
  source: string;
  confidence: number | null;
  created_at: string;
}

export interface VCompanyService {
  id: string;
  company_id: string;
  service: string;
  description: string | null;
  source: string;
  confidence: number | null;
  created_at: string;
}

export interface VCompanySource {
  company_id: string;
  external_id: string | null;
  fetched_at: string | null;
  source_id: string;
  source_name: string;
  source_display_name: string;
  source_type: string;
}

export interface VDomain {
  id: string;
  domain: string;
  import_source: string;
  first_seen_at: string | null;
  last_verified_at: string | null;
  company_count: number;
  max_confidence: number | null;
  primary_company_name: string | null;
  primary_company_id: string | null;
  primary_signal: string | null;
}

export interface DomainImportBatch {
  id: string;
  filename: string;
  csv_s3_key: string;
  status: "pending" | "processing" | "completed" | "failed";
  rows_total: number;
  rows_imported: number;
  rows_skipped: number;
  rows_failed: number;
  error_message: string | null;
  river_job_id: number | null;
  created_at: string;
  completed_at: string | null;
}

export interface DomainDetail {
  id: string;
  domain: string;
  first_seen_at: string;
  last_verified_at: string | null;
}

interface EnrichmentSource {
  name: string;
  display_name: string | null;
  can_provide: string[];
}

export interface EnrichmentSourcesResponse {
  missing_fields: string[];
  sources: EnrichmentSource[];
}

export interface CompanyFinancial {
  id: string;
  company_id: string;
  year: number;
  source_name: string;
  employee_count: number | null;
  revenue_amount: number | null;
  revenue_currency: string | null;
  revenue_usd: number | null;
  profit_amount: number | null;
  profit_usd: number | null;
  status: "suggested" | "approved" | "rejected";
  reviewed_by: string | null;
  reviewed_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface CompanyFinancialPending extends CompanyFinancial {
  company_name: string;
}
