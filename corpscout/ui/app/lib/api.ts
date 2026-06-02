import type {
  StatsResponse,
  ReviewListResponse,
  CompanySuggestionListResponse,
  DataSource,
  BrregTaskStateResponse,
  Country,
  DomainDetail,
  DomainImportBatch,
  EnrichmentSourcesResponse,
  CompanyFinancial,
  CompanyFinancialPending,
  VCompany,
  RawInputListResponse,
  RawInputDetail,
  BrregRawRecordListResponse,
  BrregRawRecordDetail,
  BrregSourceEntryListResponse,
  BrregSourceCompanyDetail,
  LLMProvider,
  LLMProviderInput,
  LLMProviderListResponse,
  LLMProviderTestRequest,
  LLMProviderTestResponse,
  WorkflowSchedule,
  WorkflowScheduleInput,
  WorkflowScheduleListResponse,
  NACETaxonomySyncRequest,
  NACETaxonomyWorkflowRunListResponse,
  NACECodeListResponse,
  NACERevisionListResponse,
  StartWorkflowResponse,
} from "~/types/api";

const BASE = "/api/v1";

class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback;
}

async function responseError(res: Response): Promise<ApiError> {
  const fallback = `${res.status} ${res.statusText}`;
  const contentType = res.headers.get("Content-Type") ?? "";

  if (contentType.includes("application/json")) {
    try {
      const body = (await res.json()) as { error?: unknown; message?: unknown };
      const message =
        typeof body.error === "string"
          ? body.error
          : typeof body.message === "string"
            ? body.message
            : fallback;
      return new ApiError(res.status, message);
    } catch {
      return new ApiError(res.status, fallback);
    }
  }

  const text = await res.text();
  return new ApiError(
    res.status,
    safeNonJSONErrorMessage(res, contentType, text, fallback),
  );
}

function safeNonJSONErrorMessage(
  res: Response,
  contentType: string,
  text: string,
  fallback: string,
): string {
  const trimmed = text.trim();
  const looksLikeHTML =
    contentType.includes("text/html") ||
    trimmed.toLowerCase().startsWith("<!doctype") ||
    trimmed.toLowerCase().startsWith("<html");
  if (looksLikeHTML) {
    if (res.status === 502) return "API backend unavailable";
    if (res.status === 503) return "API backend temporarily unavailable";
    if (res.status === 504) return "API backend request timed out";
    return fallback;
  }
  return trimmed || fallback;
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path);
  if (!res.ok) throw await responseError(res);
  return res.json() as Promise<T>;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await responseError(res);
  return res.json() as Promise<T>;
}

async function patch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await responseError(res);
  return res.json() as Promise<T>;
}

async function del<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path, { method: "DELETE" });
  if (!res.ok) throw await responseError(res);
  return res.json() as Promise<T>;
}

export const api = {
  getStats: () => get<StatsResponse>("/stats"),

  getReview: (
    page = 1,
    limit = 50,
    filters?: { signal?: string; min_confidence?: number; q?: string },
  ) => {
    const qs = new URLSearchParams({
      page: String(page),
      limit: String(limit),
    });
    if (filters?.signal) qs.set("signal", filters.signal);
    if (filters?.min_confidence != null)
      qs.set("min_confidence", String(filters.min_confidence));
    if (filters?.q) qs.set("q", filters.q);
    return get<ReviewListResponse>(`/review?${qs.toString()}`);
  },

  createReview: (id: string, action: "approved" | "rejected" | "superseded") =>
    post<unknown>(`/review/${id}/reviews`, { action, reviewed_by: "ops" }),

  bulkReview: (ids: string[], action: "approved" | "rejected" | "superseded") =>
    post<{ updated: number; skipped: number }>("/review/bulk", { ids, action }),

  getReviewIDs: (filters?: {
    signal?: string;
    min_confidence?: number;
    q?: string;
  }) => {
    const qs = new URLSearchParams();
    if (filters?.signal) qs.set("signal", filters.signal);
    if (filters?.min_confidence != null)
      qs.set("min_confidence", String(filters.min_confidence));
    if (filters?.q) qs.set("q", filters.q);
    return get<{ ids: string[] }>(
      `/review/ids${qs.toString() ? `?${qs.toString()}` : ""}`,
    );
  },

  getRawInputs: (
    params: {
      page?: number;
      limit?: number;
      source?: string;
      status?: string;
      processing_status?: string;
      translation_status?: string;
      has_suggestion?: string;
      q?: string;
      sort?: string;
      dir?: "asc" | "desc";
    } = {},
  ) => {
    const qs = new URLSearchParams();
    if (params.page) qs.set("page", String(params.page));
    if (params.limit) qs.set("limit", String(params.limit));
    if (params.source) qs.set("source", params.source);
    if (params.processing_status)
      qs.set("processing_status", params.processing_status);
    else if (params.status) qs.set("status", params.status);
    if (params.translation_status)
      qs.set("translation_status", params.translation_status);
    if (params.has_suggestion) qs.set("has_suggestion", params.has_suggestion);
    if (params.q) qs.set("q", params.q);
    if (params.sort) qs.set("sort", params.sort);
    if (params.dir) qs.set("dir", params.dir);
    return get<RawInputListResponse>(`/raw-inputs?${qs.toString()}`);
  },

  getRawInput: (source: string, id: string) =>
    get<RawInputDetail>(`/raw-inputs/${source}/${id}`),

  getBrregRawRecords: (
    params: {
      page?: number;
      limit?: number;
      q?: string;
      state?: string;
      lifecycle_state?: string;
      translation_status?: string;
      domain_status?: string;
      domain_search?: string;
      financial_status?: string;
      enhanced_status?: string;
      sort?: string;
      dir?: "asc" | "desc";
    } = {},
  ) => {
    const qs = new URLSearchParams();
    if (params.page) qs.set("page", String(params.page));
    if (params.limit) qs.set("limit", String(params.limit));
    if (params.q) qs.set("q", params.q);
    if (params.state) qs.set("state", params.state);
    if (params.lifecycle_state)
      qs.set("lifecycle_state", params.lifecycle_state);
    if (params.translation_status)
      qs.set("translation_status", params.translation_status);
    if (params.domain_status) qs.set("domain_status", params.domain_status);
    if (params.domain_search) qs.set("domain_search", params.domain_search);
    if (params.financial_status)
      qs.set("financial_status", params.financial_status);
    if (params.enhanced_status)
      qs.set("enhanced_status", params.enhanced_status);
    if (params.sort) qs.set("sort", params.sort);
    if (params.dir) qs.set("dir", params.dir);
    const q = qs.toString();
    return get<BrregRawRecordListResponse>(
      `/brreg/raw-records${q ? `?${q}` : ""}`,
    );
  },

  getBrregRawRecord: (id: string) =>
    get<BrregRawRecordDetail>(`/brreg/raw-records/${id}`),

  getBrregSourceEntries: (
    params: {
      page?: number;
      limit?: number;
      q?: string;
      state?: string;
      lifecycle_status?: string;
      registration_status?: string;
      translation_status?: string;
      sort?: string;
      dir?: "asc" | "desc";
    } = {},
  ) => {
    const qs = new URLSearchParams();
    if (params.page) qs.set("page", String(params.page));
    if (params.limit) qs.set("limit", String(params.limit));
    if (params.q) qs.set("q", params.q);
    if (params.state) qs.set("state", params.state);
    if (params.lifecycle_status)
      qs.set("lifecycle_status", params.lifecycle_status);
    if (params.registration_status)
      qs.set("registration_status", params.registration_status);
    if (params.translation_status)
      qs.set("translation_status", params.translation_status);
    if (params.sort) qs.set("sort", params.sort);
    if (params.dir) qs.set("dir", params.dir);
    const q = qs.toString();
    return get<BrregSourceEntryListResponse>(
      `/brreg/source-entries${q ? `?${q}` : ""}`,
    );
  },

  getBrregSourceCompanyDetail: (id: string) =>
    get<BrregSourceCompanyDetail>(`/sources/brreg/companies/${id}`),

  getLLMProviders: () => get<LLMProviderListResponse>("/llm-providers"),

  createLLMProvider: (body: LLMProviderInput) =>
    post<LLMProvider>("/llm-providers", body),

  updateLLMProvider: (id: string, body: LLMProviderInput) =>
    patch<LLMProvider>(`/llm-providers/${id}`, body),

  setDefaultLLMProvider: (id: string) =>
    post<LLMProvider>(`/llm-providers/${id}/default`, {}),

  testLLMProvider: (id: string, body: LLMProviderTestRequest) =>
    post<LLMProviderTestResponse>(`/llm-providers/${id}/test`, body),

  getWorkflowSchedules: () =>
    get<WorkflowScheduleListResponse>("/workflow-schedules"),

  createWorkflowSchedule: (body: WorkflowScheduleInput) =>
    post<WorkflowSchedule>("/workflow-schedules", body),

  updateWorkflowSchedule: (scheduleId: string, body: WorkflowScheduleInput) =>
    patch<WorkflowSchedule>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}`,
      body,
    ),

  triggerWorkflowSchedule: (scheduleId: string) =>
    post<{ status: string; temporal_schedule_id: string }>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}/trigger`,
      {},
    ),

  pauseWorkflowSchedule: (scheduleId: string, note = "") =>
    post<{ status: string; temporal_schedule_id: string }>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}/pause`,
      { note },
    ),

  resumeWorkflowSchedule: (scheduleId: string, note = "") =>
    post<{ status: string; temporal_schedule_id: string }>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}/resume`,
      { note },
    ),

  deleteWorkflowSchedule: (scheduleId: string) =>
    del<{ status: string; temporal_schedule_id: string }>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}`,
    ),

  startNACETaxonomySync: (body: NACETaxonomySyncRequest = {}) =>
    post<StartWorkflowResponse>("/workflows/nace/taxonomy-sync", body),

  getNACETaxonomySyncRuns: (limit = 10) =>
    get<NACETaxonomyWorkflowRunListResponse>(
      `/workflows/nace/taxonomy-sync/runs?limit=${limit}`,
    ),

  getNACERevisions: () => get<NACERevisionListResponse>("/nace/revisions"),

  getNACECodeChildren: (params: { revision?: string; parent_id?: string } = {}) => {
    const qs = new URLSearchParams();
    if (params.revision) qs.set("revision", params.revision);
    if (params.parent_id) qs.set("parent_id", params.parent_id);
    const q = qs.toString();
    return get<NACECodeListResponse>(`/nace/codes${q ? `?${q}` : ""}`);
  },

  getCompanySuggestions: (page = 1, limit = 50) =>
    get<CompanySuggestionListResponse>(
      `/suggestions/companies?page=${page}&limit=${limit}`,
    ),

  getCompanySuggestionIDs: () =>
    get<{ ids: string[] }>("/suggestions/companies/ids"),

  bulkCompanySuggestions: (ids: string[], action: "approve" | "reject") =>
    post<{ updated: number; skipped: number }>("/suggestions/companies/bulk", {
      ids,
      action,
    }),

  getSources: () => get<DataSource[]>("/sources"),

  getSource: (name: string) => get<DataSource>(`/sources/${name}`),

  getBrregTaskState: () =>
    get<BrregTaskStateResponse>("/sources/brreg/task-state"),

  patchSource: (
    name: string,
    body: {
      enabled?: boolean;
      schedule_enabled?: boolean;
      schedule_kind?: DataSource["schedule_kind"];
      schedule_expression?: string | null;
      config?: Record<string, unknown>;
    },
  ) => patch<{ status: string }>(`/sources/${name}`, body),

  translateBrreg: (
    body: {
      ids?: string[];
      filters?: Record<string, string>;
      limit?: number;
      batch_size?: number;
      max_attempts?: number;
      max_parallel_tasks?: number;
      lease_seconds?: number;
      provider?: string;
      model?: string;
      prompt_version?: string;
      source_lang?: string;
      target_lang?: string;
      max_service_retries?: number;
      trigger?: string;
    } = {},
  ) =>
    post<{ status: string; workflow_id: string; workflow_run_id?: string }>(
      "/workflows/brreg/translation",
      body,
    ),

  searchBrregDomains: (
    body: {
      ids?: string[];
      filters?: Record<string, string>;
      limit?: number;
      batch_size?: number;
      max_attempts?: number;
      max_parallel_tasks?: number;
      lease_seconds?: number;
      search_engine?: string;
      provider?: string;
      model?: string;
      candidate_threshold?: number;
      domain_threshold?: number;
      max_candidates?: number;
      max_site_checks?: number;
      timeout_seconds?: number;
      trigger?: string;
    } = {},
  ) =>
    post<{ status: string; workflow_id: string; workflow_run_id?: string }>(
      "/workflows/brreg/domain-search",
      body,
    ),

  syncBrregSourceProfiles: (
    body: {
      ids?: string[];
      filters?: Record<string, string>;
      limit?: number;
      trigger?: string;
    } = {},
  ) =>
    post<{ status: string; workflow_id: string; workflow_run_id?: string }>(
      "/workflows/brreg/source-profile-normalization",
      body,
    ),

  loadBrregBulkRawRecords: (
    body: {
      limit?: number;
      source_url?: string;
      trigger?: string;
    } = {},
  ) => post<StartWorkflowResponse>("/workflows/brreg/bulk-raw-ingest", body),

  cancelJob: (id: number) =>
    post<{ status: string; id: number }>(`/jobs/${id}/cancel`, {}),

  cancelBulkByIds: (ids: number[]) =>
    post<{ cancelled: number }>("/jobs/cancel-bulk", { ids }),

  getCountries: () => get<Country[]>("/countries"),

  getCompanyEnrichmentSources: (id: string) =>
    get<EnrichmentSourcesResponse>(`/companies/${id}/enrichment-sources`),

  enrichCompanyFromSource: (id: string, source: string) =>
    post<{ job_id: number }>(`/companies/${id}/enrich-from-source`, { source }),

  getFinancialSuggestions: (page = 1, limit = 50) =>
    get<{
      items: CompanyFinancialPending[];
      total: number;
      page: number;
      limit: number;
    }>(`/financials/review?page=${page}&limit=${limit}`),

  getFinancialSuggestionIDs: () =>
    get<{ ids: string[] }>("/financials/review/ids"),

  bulkFinancialSuggestions: (ids: string[], action: "approve" | "reject") =>
    post<void>("/financials/review/bulk", { ids, action }),

  getCompanyFinancials: (companyId: string) =>
    get<{ items: CompanyFinancial[] }>(`/companies/${companyId}/financials`),

  reviewFinancial: (id: string, action: "approve" | "reject") =>
    post<void>(`/financials/${id}/review`, { action }),

  patchCompany: (
    id: string,
    body: {
      name?: string;
      short_name?: string;
      short_description?: string;
      description?: string;
      website?: string;
      founded_year?: number;
    },
  ) => patch<VCompany>(`/companies/${id}`, body),
};

export function getDomain(domainId: string): Promise<DomainDetail> {
  return get<DomainDetail>(`/domains/${domainId}`);
}

export async function uploadDomainsCSV(file: File): Promise<DomainImportBatch> {
  const formData = new FormData();
  formData.append("file", file);
  const res = await fetch(`${BASE}/domains/import`, {
    method: "POST",
    body: formData,
  });
  if (!res.ok) throw await responseError(res);
  return res.json() as Promise<DomainImportBatch>;
}
