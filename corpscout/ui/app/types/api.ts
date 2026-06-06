export interface StatsResponse {
  total_companies: number;
  total_domains: number;
  active_domains: number;
  pending_review: number;
  pending_raw_inputs: number;
  enabled_sources: number;
}

export interface LLMProvider {
  id: string;
  slug: string;
  display_name: string;
  provider_type: "openai_compatible";
  base_url: string;
  model: string;
  has_api_key: boolean;
  is_default: boolean;
  enabled: boolean;
  capabilities: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface LLMProviderListResponse {
  providers: LLMProvider[];
}

export interface LLMProviderInput {
  slug: string;
  display_name: string;
  provider_type: "openai_compatible";
  base_url: string;
  model: string;
  api_key?: string;
  enabled: boolean;
  is_default: boolean;
  capabilities: Record<string, unknown>;
  metadata: Record<string, unknown>;
}

export interface LLMProviderTestRequest {
  prompt: string;
  max_tokens?: number;
  timeout_seconds?: number;
}

export interface LLMProviderTestResponse {
  status: "succeeded" | "failed";
  provider_id: string;
  provider_slug: string;
  model: string;
  response_text: string;
  duration_ms: number;
  error?: {
    code: string;
    message: string;
  };
}

export interface WorkflowScheduleSpec {
  timezone: string;
  cron_expression: string;
  overlap_policy:
    | "skip"
    | "buffer_one"
    | "allow_all"
    | "cancel_other"
    | "terminate_other";
  catchup_window_seconds: number;
}

export interface WorkflowScheduleTemporalState {
  exists: boolean;
  paused: boolean;
  note: string;
  next_run_at?: string;
}

export interface WorkflowSchedule {
  id: string;
  temporal_schedule_id: string;
  workflow_key: WorkflowScheduleKey;
  workflow_name: string;
  task_queue: string;
  domain: string;
  purpose: string;
  display_name: string;
  description?: string;
  enabled: boolean;
  tags: string[];
  metadata: Record<string, unknown>;
  spec: WorkflowScheduleSpec;
  action_input: Record<string, unknown>;
  temporal: WorkflowScheduleTemporalState;
  created_at: string;
  updated_at: string;
}

export interface WorkflowScheduleListResponse {
  items: WorkflowSchedule[];
}

export interface WorkflowScheduleInput {
  temporal_schedule_id: string;
  workflow_key: WorkflowScheduleKey;
  display_name: string;
  description?: string;
  enabled: boolean;
  tags: string[];
  metadata: Record<string, unknown>;
  spec: WorkflowScheduleSpec;
  action_input: Record<string, unknown>;
}

export type WorkflowScheduleKey =
  | "nace_taxonomy_sync"
  | "fx_rate_sync"
  | "brreg_source_explorer_refresh";

export interface NACETaxonomySyncRequest {
  revision?: string;
  source_url?: string;
  trigger?: "manual" | "schedule";
  force_reprocess?: boolean;
}

export interface NACETaxonomyWorkflowRun {
  workflow_id: string;
  run_id: string;
  workflow_type: string;
  status: string;
  start_time?: string;
  close_time?: string;
  execution_time?: string;
}

export interface NACETaxonomyWorkflowRunListResponse {
  items: NACETaxonomyWorkflowRun[];
}

export interface ExchangeRateSyncRequest {
  provider?: "ecb";
  source_url?: string;
  trigger?: "manual" | "schedule";
  force_reprocess?: boolean;
}

export interface ExchangeRateWorkflowRun {
  workflow_id: string;
  run_id: string;
  workflow_type: string;
  status: string;
  start_time?: string;
  close_time?: string;
  execution_time?: string;
}

export interface ExchangeRateWorkflowRunListResponse {
  items: ExchangeRateWorkflowRun[];
}

export interface BrregWorkflowRunPrefix {
  prefix: string;
  label: string;
  workflow_type: string;
}

export interface BrregWorkflowRun {
  workflow_id: string;
  run_id: string;
  workflow_type: string;
  prefix: string;
  action: string;
  status: string;
  start_time?: string;
  close_time?: string;
  execution_time?: string;
}

export interface BrregWorkflowRunListResponse {
  prefixes: BrregWorkflowRunPrefix[];
  items: BrregWorkflowRun[];
}

export interface NACERevision {
  classification_id: string;
  code_system: "NACE";
  revision: string;
  name: string;
  valid_from?: string;
  valid_to?: string;
  active_codes: number;
  inactive_codes: number;
  sections: number;
  divisions: number;
  groups: number;
  classes: number;
  codes_updated_at?: string;
  classification_updated_at: string;
}

export interface NACERevisionListResponse {
  items: NACERevision[];
}

export interface NACECode {
  id: string;
  classification_id: string;
  code: string;
  normalized_code: string;
  level: number;
  level_name: "section" | "division" | "group" | "class";
  parent_code?: string | null;
  parent_id?: string | null;
  title: string;
  description?: string | null;
  includes?: string | null;
  excludes?: string | null;
  active: boolean;
  has_children: boolean;
}

export interface NACECodeListResponse {
  items: NACECode[];
}

export interface StartWorkflowResponse {
  status: string;
  workflow: string;
  workflow_id: string;
  workflow_run_id: string;
}

export interface BrregCompanyTranslationRequest {
  all_records?: boolean;
  ids?: string[];
  filters?: Record<string, string>;
  limit?: number;
  batch_size?: number;
  claim_mode?: "auto" | "fixed";
  max_request_chars?: number;
  max_terms?: number;
  max_parallel_tasks?: number;
  lease_seconds?: number;
  max_attempts?: number;
  batch_delay_seconds?: number;
  provider?: string;
  model?: string;
  prompt_version?: string;
  trigger?: string;
}

export type AriregisterCompanyTranslationRequest =
  BrregCompanyTranslationRequest;

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
  source_company_id?: string | null;
  source_payload_hash?: string | null;
  source_synced_at?: string | null;
  sync_status: "not_synced" | "synced" | "needs_update";
  synced: boolean;
  updated_at: string;
}

export interface BrregRawRecordListResponse {
  items: BrregRawRecordListItem[];
  total: number;
  page: number;
  limit: number;
}

export interface BrregSourceEntryListItem {
  company_id: string;
  organization_number: string;
  organization_name: string;
  description_en?: string;
  lifecycle_status: string;
  registration_status?: string;
  organization_form_code?: string;
  organization_form_label?: string;
  primary_industry_code?: string;
  primary_industry_label?: string;
  primary_nace_code?: string;
  primary_nace_title?: string;
  city?: string;
  municipality?: string;
  municipality_number?: string;
  county?: string;
  postal_code?: string;
  formatted_address?: string;
  employee_count?: number;
  employee_band?: string;
  website_url: string;
  website_host?: string;
  website_count: number;
  domain_count: number;
  contact_count: number;
  latest_financial_year?: number;
  latest_revenue_usd_cents?: number;
  latest_total_assets_usd_cents?: number;
  latest_net_income_usd_cents?: number;
  financial_status: "success" | "unknown" | "skipped";
  translation_missing_count: number;
  translation_pending_count: number;
  translation_running_count: number;
  translation_succeeded_count: number;
  translation_failed_count: number;
  domain_pending_count: number;
  domain_running_count: number;
  domain_succeeded_count: number;
  updated_at: string;
}

export interface BrregSourceEntryListResponse {
  items: BrregSourceEntryListItem[];
  total: number;
  page: number;
  limit: number;
}

export interface AriregisterSourceEntryListItem {
  company_id: string;
  registry_code: string;
  legal_name: string;
  legal_form_label?: string;
  lifecycle_status: string;
  registration_status?: string;
  registration_status_label?: string;
  primary_industry_code?: string;
  primary_industry_label?: string;
  primary_nace_code?: string;
  primary_nace_title?: string;
  city_or_area?: string;
  postal_code?: string;
  normalized_full_address?: string;
  employee_count?: number;
  latest_financial_year?: number;
  website_count: number;
  domain_count: number;
  contact_count: number;
  translation_missing_count: number;
  updated_at: string;
}

export interface AriregisterSourceEntryListResponse {
  items: AriregisterSourceEntryListItem[];
  total: number;
  page: number;
  limit: number;
}

export interface AriregisterSourceCompanyDetail {
  id: string;
  raw_record_id: string;
  registry_code: string;
  source_native_id: string;
  country_iso2: string;
  legal_name: string;
  legal_name_normalized: string;
  legal_name_en?: string | null;
  registration_status?: string | null;
  registration_status_label?: string | null;
  registration_status_label_en?: string | null;
  lifecycle_status: string;
  legal_form_code?: string | null;
  legal_form_number?: number | null;
  legal_form_label?: string | null;
  legal_form_label_en?: string | null;
  legal_form_subtype?: string | null;
  legal_form_subtype_label?: string | null;
  legal_form_subtype_label_en?: string | null;
  region_code?: number | null;
  region_label?: string | null;
  region_label_en?: string | null;
  region_label_long?: string | null;
  region_label_long_en?: string | null;
  active_label?: string | null;
  active_label_en?: string | null;
  first_registered_on?: string | null;
  deleted_on?: string | null;
  evks_registered_at?: string | null;
  has_missing_beneficial_owner_discrepancy_notice?: boolean | null;
  founded_without_contribution?: boolean | null;
  waived_form_requirements?: boolean | null;
  is_accounting_required?: boolean | null;
  reports_beneficial_owners?: boolean | null;
  is_active?: boolean | null;
  last_annual_report_year?: number | null;
  employee_count?: number | null;
  employee_count_source?: string | null;
  employee_band?: string | null;
  source_updated_at?: string | null;
  payload_hash: string;
  profile_version: string;
  row_status: string;
  normalized_payload: Record<string, unknown>;
  raw_company_payload: Record<string, unknown>;
  evidence: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  superseded_at?: string | null;
  names: Array<Record<string, unknown>>;
  statuses: Array<Record<string, unknown>>;
  legal_forms: Array<Record<string, unknown>>;
  addresses: Array<Record<string, unknown>>;
  contacts: Array<Record<string, unknown>>;
  websites: Array<Record<string, unknown>>;
  domains: Array<Record<string, unknown>>;
  industries: Array<Record<string, unknown>>;
  capital: Array<Record<string, unknown>>;
  annual_reports: Array<Record<string, unknown>>;
  articles: Array<Record<string, unknown>>;
  registry_notes: Array<Record<string, unknown>>;
}

export interface BrregSourceCompanyDetail {
  id: string;
  raw_record_id: string;
  enhanced_record_id?: string;
  translation_result_id?: string;
  financial_result_id?: string;
  organization_number: string;
  source_native_id: string;
  country_iso2: string;
  organization_name: string;
  organization_name_normalized: string;
  organization_name_en?: string;
  short_description?: string;
  short_description_en?: string;
  description?: string;
  description_en?: string;
  registration_status?: string;
  registration_status_label?: string;
  registration_status_label_en?: string;
  lifecycle_status: string;
  organization_form_code?: string;
  organization_form_label?: string;
  organization_form_label_en?: string;
  language_code?: string;
  response_class?: string;
  response_class_en?: string;
  founded_date?: string;
  unit_registry_registered_at?: string;
  enterprise_registry_registered_at?: string;
  vat_registry_registered_at?: string;
  vat_registry_unit_registered_at?: string;
  articles_date?: string;
  last_annual_report_year?: number;
  activity_description?: string;
  activity_description_en?: string;
  statutory_purpose?: string;
  statutory_purpose_en?: string;
  is_bankrupt?: boolean;
  is_in_group?: boolean;
  is_under_liquidation?: boolean;
  is_forced_dissolution?: boolean;
  has_registered_employees?: boolean;
  in_vat_register?: boolean;
  in_business_register?: boolean;
  in_voluntary_register?: boolean;
  in_foundation_register?: boolean;
  in_party_register?: boolean;
  employee_count?: number;
  employee_count_source?: string;
  employee_band?: string;
  source_updated_at?: string;
  payload_hash: string;
  profile_version: string;
  row_status: string;
  normalized_payload: Record<string, unknown>;
  raw_company_payload: Record<string, unknown>;
  evidence: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  superseded_at?: string;
  addresses: Array<Record<string, unknown>>;
  industries: Array<Record<string, unknown>>;
  websites: Array<Record<string, unknown>>;
  domains: Array<Record<string, unknown>>;
  contacts: Array<Record<string, unknown>>;
  financial_years: Array<Record<string, unknown>>;
  roles: Array<Record<string, unknown>>;
  shareholdings: Array<Record<string, unknown>>;
  translation_status: Record<string, number>;
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
  domain_search_evidence: BrregDomainSearchEvidence[];
}

export interface BrregDomainSearchEvidence {
  action_attempt_id: string;
  workflow_run_id?: string;
  task_attempt_id?: string;
  raw_record_id: string;
  search_engine?: string;
  model?: string;
  attempt: number;
  action_status: string;
  started_at: string;
  finished_at?: string;
  action_error?: string;
  error_category?: string;
  error_code?: string;
  retry_strategy?: string;
  search_term: string;
  artifact_id: string;
  artifact_created_at: string;
  search_url: string;
  final_url: string;
  crawl_status: string;
  markdown: string;
  markdown_hash: string;
  links: string[];
  crawl_metadata: Record<string, unknown>;
  crawl_error: Record<string, unknown>;
  artifact_payload: Record<string, unknown>;
  artifact_metadata: Record<string, unknown>;
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

type Signal =
  | "registry_website"
  | "wikidata"
  | "certsh"
  | "whois"
  | "search"
  | "manual_upload";

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
  source_entries_available: boolean;
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
