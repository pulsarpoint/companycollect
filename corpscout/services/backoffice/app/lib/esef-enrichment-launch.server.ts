import { randomUUID } from "node:crypto";

import { launchRun, type DagsterOptions } from "~/lib/dagster.server";
import { assertEsefLaunchAllowed } from "~/lib/esef-operations.server";

export const ESEF_DOCUMENT_COMPANY_INFORMATION_JOB =
  "esef_document_company_information_job";

export const ESEF_ENRICHMENT_FORM_DEFAULTS = {
  temperature: 0,
  concurrency: 1,
  maxDocuments: 50,
  maxEvidenceChars: 64_000,
  timeoutSeconds: 180,
} as const;

export const ESEF_ENRICHMENT_RUNTIME_DEFAULTS = {
  ...ESEF_ENRICHMENT_FORM_DEFAULTS,
  promptVersion: "esef-company-enrichment-v2",
} as const;

const ESEF_DOCUMENT_COMPANY_INFORMATION_ASSET =
  "esef_document_company_information_clickhouse";

export type EsefRefreshBehavior =
  "reuse_existing" | "refresh_existing" | "reprocess_existing_without_model";

export interface EsefLlmRuntimeProfile {
  provider: string;
  model: string;
  baseUrl: string;
  apiKeyEnvironmentVariable: string;
  temperature: number;
  promptVersion: string;
  concurrency: number;
}

export interface LaunchEsefDocumentCompanyInformationInput {
  /** Authenticated server-side operator identity, never a browser form value. */
  requestedBy: string;
  countryIso2s: readonly string[];
  companyIds: readonly string[];
  sourceDocumentIds: readonly string[];
  maxDocuments: number | null;
  refreshBehavior: EsefRefreshBehavior;
  maxEvidenceChars: number;
  timeoutSeconds: number;
  llm: EsefLlmRuntimeProfile;
}

export interface EsefDocumentCompanyInformationLaunch {
  runId: string;
  status: string;
  requestId: string;
}

function normalizedIds(values: readonly string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

function normalizedCountryCodes(values: readonly string[]): string[] {
  const countries = [
    ...new Set(
      values.map((value) => value.trim().toUpperCase()).filter(Boolean),
    ),
  ].sort();
  if (countries.some((country) => !/^[A-Z]{2}$/.test(country))) {
    throw new Error("ESEF countries must be two-letter ISO country codes.");
  }
  return countries;
}

/**
 * Launch the complete ESEF company-information job from Backoffice.
 *
 * The job, selection and correlation tags are policy owned by this server
 * adapter. Callers can only provide the ESEF scope and non-secret runtime
 * profile, so browser input cannot redirect a launch to another Dagster job or
 * replace its asset selection and audit tags.
 */
export async function launchEsefDocumentCompanyInformation(
  input: LaunchEsefDocumentCompanyInformationInput,
  options: DagsterOptions = {},
): Promise<EsefDocumentCompanyInformationLaunch> {
  const requestedBy = input.requestedBy.trim();
  if (requestedBy === "") {
    throw new Error(
      "An authenticated operator is required to launch ESEF enrichment.",
    );
  }

  await assertEsefLaunchAllowed(options);
  const requestId = randomUUID();
  const companyIds = normalizedIds(input.companyIds);
  const sourceDocumentIds = normalizedIds(input.sourceDocumentIds);
  const countryIso2s = normalizedCountryCodes(input.countryIso2s);
  if (companyIds.length > 0 && countryIso2s.length !== 1) {
    throw new Error(
      "ESEF company IDs require exactly one country because company identity is country-scoped.",
    );
  }
  const run = await launchRun(
    {
      job: ESEF_DOCUMENT_COMPANY_INFORMATION_JOB,
      runConfig: {
        ops: {
          [ESEF_DOCUMENT_COMPANY_INFORMATION_ASSET]: {
            config: {
              provider: input.llm.provider.trim(),
              model: input.llm.model.trim(),
              base_url: input.llm.baseUrl.trim(),
              api_key_environment_variable:
                input.llm.apiKeyEnvironmentVariable.trim(),
              temperature: input.llm.temperature,
              prompt_version: input.llm.promptVersion.trim(),
              concurrency: input.llm.concurrency,
              country_iso2s: countryIso2s,
              company_ids: companyIds,
              source_document_ids: sourceDocumentIds,
              max_documents: input.maxDocuments,
              refresh_existing: input.refreshBehavior === "refresh_existing",
              reprocess_existing_without_model:
                input.refreshBehavior === "reprocess_existing_without_model",
              max_evidence_chars: input.maxEvidenceChars,
              timeout_seconds: input.timeoutSeconds,
            },
          },
        },
      },
      tags: {
        "corpscout/trigger_source": "backoffice",
        "corpscout/request_id": requestId,
        "corpscout/requested_by": requestedBy,
        "corpscout/llm_provider": input.llm.provider.trim(),
        "corpscout/llm_model": input.llm.model.trim(),
        "corpscout/country_count": String(countryIso2s.length),
        "corpscout/company_count": String(companyIds.length),
        "corpscout/source_document_count": String(sourceDocumentIds.length),
        "corpscout/refresh_behavior": input.refreshBehavior,
        ...(sourceDocumentIds.length === 1
          ? { "corpscout/source_document_id": sourceDocumentIds[0] }
          : {}),
        ...(countryIso2s.length === 1
          ? { "corpscout/country_iso2": countryIso2s[0] }
          : {}),
      },
    },
    options,
  );

  return { ...run, requestId };
}
