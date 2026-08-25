import { randomUUID } from "node:crypto";

import {
  launchRun,
  type DagsterOptions,
} from "~/lib/dagster.server";
import { assertEsefLaunchAllowed } from "~/lib/esef-operations.server";

export const ESEF_DOCUMENT_COMPANY_INFORMATION_JOB =
  "esef_document_company_information_job";

const ESEF_DOCUMENT_COMPANY_INFORMATION_ASSET =
  "esef_document_company_information_clickhouse";

export type EsefRefreshBehavior =
  | "reuse_existing"
  | "refresh_existing"
  | "reprocess_existing_without_model";

export interface EsefLlmRuntimeProfile {
  provider: string;
  model: string;
  baseUrl: string;
  temperature: number;
  maxTokens: number;
  promptVersion: string;
  concurrency: number;
}

export interface LaunchEsefDocumentCompanyInformationInput {
  /** Authenticated server-side operator identity, never a browser form value. */
  requestedBy: string;
  countryIso2: string;
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
    throw new Error("An authenticated operator is required to launch ESEF enrichment.");
  }

  await assertEsefLaunchAllowed(options);
  const requestId = randomUUID();
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
              temperature: input.llm.temperature,
              max_tokens: input.llm.maxTokens,
              prompt_version: input.llm.promptVersion.trim(),
              concurrency: input.llm.concurrency,
              country_iso2: input.countryIso2.trim().toUpperCase(),
              company_ids: normalizedIds(input.companyIds),
              source_document_ids: normalizedIds(input.sourceDocumentIds),
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
      },
    },
    options,
  );

  return { ...run, requestId };
}
