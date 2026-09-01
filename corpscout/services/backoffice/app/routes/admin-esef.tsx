import type { Route } from "./+types/admin-esef";
import {
  EsefOperationsWorkspace,
  type EsefLaunchActionResult,
  type EsefProfileOption,
} from "~/components/admin/esef-operations-workspace";
import {
  DagsterError,
  DagsterRequestError,
  DagsterRunConfigValidationError,
  dagsterRunUrl,
} from "~/lib/dagster.server";
import {
  ESEF_ENRICHMENT_FORM_DEFAULTS,
  ESEF_ENRICHMENT_RUNTIME_DEFAULTS,
  launchEsefDocumentCompanyInformation,
  type EsefRefreshBehavior,
} from "~/lib/esef-enrichment-launch.server";
import { loadEsefCountryCodes } from "~/lib/esef-countries.server";
import {
  EsefLaunchBlockedError,
  loadEsefOverview,
} from "~/lib/esef-operations.server";
import {
  isLocalCodexEnabled,
  listLlmProfiles,
  type LlmProfile,
} from "~/lib/llm-settings.server";

// Synthetic picker entry for the locally running codex agent. It is not a
// stored profile: availability comes from the settings toggle plus the
// LOCAL_CODEX_BASE_URL environment variable, and no API key applies (the
// worker still needs an env-var NAME, so a placeholder is sent).
const LOCAL_CODEX_PROFILE_ID = "local_codex";
const LOCAL_CODEX_MODEL = "codex";
const LOCAL_CODEX_KEY_ENVIRONMENT_VARIABLE = "LOCAL_CODEX_API_KEY";

function localCodexBaseUrl(): string {
  return process.env.LOCAL_CODEX_BASE_URL?.trim() ?? "";
}

function localCodexOption(): EsefProfileOption | null {
  if (!isLocalCodexEnabled()) return null;
  const baseUrl = localCodexBaseUrl();
  return {
    profileId: LOCAL_CODEX_PROFILE_ID,
    name: "Local codex",
    provider: LOCAL_CODEX_PROFILE_ID,
    model: LOCAL_CODEX_MODEL,
    baseUrl,
    isActive: false,
    disabled: baseUrl === "",
    disabledReason:
      baseUrl === ""
        ? "Set LOCAL_CODEX_BASE_URL on the backoffice to launch against the local codex agent."
        : "",
  };
}

const REFRESH_BEHAVIORS = new Set<EsefRefreshBehavior>([
  "reuse_existing",
  "refresh_existing",
  "reprocess_existing_without_model",
]);

class EsefActionValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EsefActionValidationError";
  }
}

function formValue(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value.trim() : "";
}

function boundedInteger(
  form: FormData,
  name: string,
  label: string,
  fallback: number,
  minimum: number,
  maximum: number,
): number {
  const raw = formValue(form, name);
  if (raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new EsefActionValidationError(
      `${label} must be a whole number from ${minimum.toLocaleString()} to ${maximum.toLocaleString()}.`,
    );
  }
  return value;
}

function boundedNumber(
  form: FormData,
  name: string,
  label: string,
  fallback: number,
  minimum: number,
  maximum: number,
): number {
  const raw = formValue(form, name);
  if (raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < minimum || value > maximum) {
    throw new EsefActionValidationError(
      `${label} must be from ${minimum.toLocaleString()} to ${maximum.toLocaleString()}.`,
    );
  }
  return value;
}

function identifiers(value: string): string[] {
  return [
    ...new Set(
      value
        .split(/[\s,]+/)
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  ];
}

function selectedProfile(
  profiles: LlmProfile[],
  profileId: string,
): LlmProfile {
  if (profileId === LOCAL_CODEX_PROFILE_ID) {
    if (!isLocalCodexEnabled()) {
      throw new EsefActionValidationError(
        "Local codex is not enabled in LLM settings.",
      );
    }
    const baseUrl = localCodexBaseUrl();
    if (baseUrl === "") {
      throw new EsefActionValidationError(
        "Set LOCAL_CODEX_BASE_URL on the backoffice before launching against the local codex agent.",
      );
    }
    const now = new Date().toISOString();
    return {
      profileId: LOCAL_CODEX_PROFILE_ID,
      name: "Local codex",
      provider: LOCAL_CODEX_PROFILE_ID,
      baseUrl,
      model: LOCAL_CODEX_MODEL,
      apiKeyEnvironmentVariable: LOCAL_CODEX_KEY_ENVIRONMENT_VARIABLE,
      isActive: false,
      apiKeyAvailable: true,
      createdAt: now,
      updatedAt: now,
    };
  }
  const profile = profiles.find(
    (candidate) => candidate.profileId === profileId,
  );
  if (!profile) {
    throw new EsefActionValidationError(
      "Choose an LLM profile from central LLM settings.",
    );
  }
  return profile;
}

async function selectedCountries(form: FormData): Promise<string[]> {
  const countries = [
    ...new Set(
      form
        .getAll("country_iso2s")
        .filter((value): value is string => typeof value === "string")
        .map((value) => value.trim().toUpperCase())
        .filter(Boolean),
    ),
  ];
  if (countries.some((country) => !/^[A-Z]{2}$/.test(country))) {
    throw new EsefActionValidationError(
      "Every country must be a two-letter ISO country code.",
    );
  }
  if (countries.length === 0) return [];

  let availableCountries: string[];
  try {
    availableCountries = await loadEsefCountryCodes();
  } catch {
    throw new EsefActionValidationError(
      "Country options could not be verified. Try again when ClickHouse is available.",
    );
  }
  const available = new Set(availableCountries);
  if (countries.some((country) => !available.has(country))) {
    throw new EsefActionValidationError(
      "Choose countries from the ESEF document country list.",
    );
  }
  return countries.sort();
}

function refreshBehavior(form: FormData): EsefRefreshBehavior {
  const value = formValue(form, "refresh_behavior") as EsefRefreshBehavior;
  if (!REFRESH_BEHAVIORS.has(value)) {
    throw new EsefActionValidationError(
      "Choose how the run should handle existing results.",
    );
  }
  return value;
}

function documentLimitEnabled(form: FormData): boolean {
  const value = formValue(form, "limit_documents");
  if (value !== "" && value !== "1") {
    throw new EsefActionValidationError(
      "The document-limit option is invalid.",
    );
  }
  return value === "1";
}

function describeSelection(input: {
  sourceDocumentIds: string[];
  companyIds: string[];
  countryIso2s: string[];
  maxDocuments: number | null;
}): string {
  if (input.sourceDocumentIds.length === 1) return input.sourceDocumentIds[0];
  if (input.sourceDocumentIds.length > 1) {
    return `${input.sourceDocumentIds.length} ESEF filings`;
  }
  if (input.companyIds.length === 1) return "1 company";
  if (input.companyIds.length > 1)
    return `${input.companyIds.length} companies`;
  if (input.countryIso2s.length === 1) {
    return input.maxDocuments === null
      ? `All eligible documents in ${input.countryIso2s[0]}`
      : `Up to ${input.maxDocuments} documents in ${input.countryIso2s[0]}`;
  }
  if (input.countryIso2s.length > 1) {
    return input.maxDocuments === null
      ? `All eligible documents in ${input.countryIso2s.length} countries`
      : `Up to ${input.maxDocuments} documents in ${input.countryIso2s.length} countries`;
  }
  return input.maxDocuments === null
    ? "All eligible documents"
    : `Up to ${input.maxDocuments} eligible documents`;
}

function profileOptions(profiles: LlmProfile[]): EsefProfileOption[] {
  const remote = profiles.map((profile) => ({
    profileId: profile.profileId,
    name: profile.name,
    provider: profile.provider,
    model: profile.model,
    baseUrl: profile.baseUrl,
    isActive: profile.isActive,
  }));
  const local = localCodexOption();
  return local ? [...remote, local] : remote;
}

function refused(error: string): EsefLaunchActionResult {
  return { ok: false, error, launched: null };
}

export async function loader(_: Route.LoaderArgs) {
  const profiles = listLlmProfiles();
  const countryDataPromise = loadEsefCountryCodes().then(
    (countries) => ({ countries, countryError: "" }),
    () => ({
      countries: [],
      countryError: "Countries could not be loaded from ESEF filing documents.",
    }),
  );
  try {
    const overview = await loadEsefOverview();
    const countryData = await countryDataPromise;
    const runIds = new Set([
      ...overview.inventory.assets.flatMap((asset) =>
        asset.materialization ? [asset.materialization.runId] : [],
      ),
      ...overview.inventory.activeRuns.map((run) => run.runId),
      ...overview.enrichment.recentEnrichmentRuns.map((run) => run.runId),
    ]);
    return {
      overview,
      error: "",
      profiles: profileOptions(profiles),
      ...countryData,
      runtimeDefaults: ESEF_ENRICHMENT_FORM_DEFAULTS,
      runUrls: Object.fromEntries(
        [...runIds].flatMap((runId) => {
          const url = dagsterRunUrl(runId);
          return url ? [[runId, url]] : [];
        }),
      ),
    };
  } catch (error) {
    const countryData = await countryDataPromise;
    return {
      overview: null,
      error:
        error instanceof DagsterRequestError
          ? "Dagster did not answer. Asset status and actions are temporarily unavailable."
          : error instanceof DagsterError
            ? error.message
            : "Dagster status could not be loaded.",
      profiles: profileOptions(profiles),
      ...countryData,
      runtimeDefaults: ESEF_ENRICHMENT_FORM_DEFAULTS,
      runUrls: {},
    };
  }
}

export async function action({
  request,
}: Route.ActionArgs): Promise<EsefLaunchActionResult> {
  try {
    const form = await request.formData();
    if (formValue(form, "intent") !== "launch-company-information") {
      return refused("Unknown ESEF action.");
    }

    const profile = selectedProfile(
      listLlmProfiles(),
      formValue(form, "profile_id"),
    );
    const sourceDocumentIds = identifiers(
      formValue(form, "source_document_ids"),
    );
    const companyIds = identifiers(formValue(form, "company_ids"));
    const countryIso2s = await selectedCountries(form);
    if (companyIds.length > 0 && countryIso2s.length !== 1) {
      throw new EsefActionValidationError(
        "Company IDs require exactly one selected country because company identity is country-scoped.",
      );
    }
    const maxDocuments = documentLimitEnabled(form)
      ? boundedInteger(
          form,
          "max_documents",
          "Document limit",
          ESEF_ENRICHMENT_RUNTIME_DEFAULTS.maxDocuments,
          1,
          100_000,
        )
      : null;
    const behavior = refreshBehavior(form);
    const run = await launchEsefDocumentCompanyInformation({
      requestedBy: process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
      countryIso2s,
      companyIds,
      sourceDocumentIds,
      maxDocuments,
      refreshBehavior: behavior,
      maxEvidenceChars: boundedInteger(
        form,
        "max_evidence_chars",
        "Evidence limit",
        ESEF_ENRICHMENT_RUNTIME_DEFAULTS.maxEvidenceChars,
        500,
        250_000,
      ),
      timeoutSeconds: boundedInteger(
        form,
        "timeout_seconds",
        "Timeout",
        ESEF_ENRICHMENT_RUNTIME_DEFAULTS.timeoutSeconds,
        1,
        600,
      ),
      llm: {
        provider: profile.provider,
        model: profile.model,
        baseUrl: profile.baseUrl,
        apiKeyEnvironmentVariable: profile.apiKeyEnvironmentVariable,
        temperature: boundedNumber(
          form,
          "temperature",
          "Temperature",
          ESEF_ENRICHMENT_RUNTIME_DEFAULTS.temperature,
          0,
          2,
        ),
        promptVersion: ESEF_ENRICHMENT_RUNTIME_DEFAULTS.promptVersion,
        concurrency: boundedInteger(
          form,
          "concurrency",
          "Concurrency",
          ESEF_ENRICHMENT_RUNTIME_DEFAULTS.concurrency,
          1,
          8,
        ),
      },
    });

    return {
      ok: true,
      error: "",
      launched: {
        ...run,
        runUrl: dagsterRunUrl(run.runId),
        model: profile.model,
        selection: describeSelection({
          sourceDocumentIds,
          companyIds,
          countryIso2s,
          maxDocuments,
        }),
      },
    };
  } catch (error) {
    if (error instanceof EsefActionValidationError)
      return refused(error.message);
    if (error instanceof EsefLaunchBlockedError) {
      return refused(error.reasons.join(" "));
    }
    if (error instanceof DagsterRunConfigValidationError) {
      const details = error.errors.map((entry) => entry.message).join(" ");
      return refused(
        `Dagster rejected the launch configuration. ${details}`.trim(),
      );
    }
    if (error instanceof DagsterRequestError) {
      return refused("Dagster did not answer, so no ESEF run was launched.");
    }
    if (error instanceof DagsterError) {
      return refused("Dagster refused the ESEF launch. No run was started.");
    }
    console.error("Unexpected ESEF enrichment launch failure", error);
    return refused(
      "The ESEF action could not be launched. No run was started.",
    );
  }
}

export function meta() {
  return [{ title: "ESEF processing | CompanyCollect" }];
}

export default function AdminEsef({ loaderData }: Route.ComponentProps) {
  return <EsefOperationsWorkspace {...loaderData} />;
}
