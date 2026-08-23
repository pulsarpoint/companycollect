import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import type { DatabaseSync } from "node:sqlite";
import {
  DuckDBAppender,
  DuckDBConnection,
  DuckDBPendingResultState,
} from "@duckdb/node-api";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import {
  connectSwedenPeopleDraftDatabase,
  PEOPLE_DRAFT_STEP_1_TABLE,
  SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
} from "~/lib/sweden-people-draft.server";
import {
  connectCurationDatabase,
  SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
} from "~/lib/sweden-person-profile-responses.server";
import {
  ensureSwedenPeopleProcessingJobTable,
  importLegacySwedenPeopleProcessingJob,
  insertSwedenPeopleProcessingJob,
  persistSwedenPeopleProcessingJob,
  readActiveSwedenPeopleProcessingJob,
  readLatestSwedenPeopleProcessingJob,
  readSwedenPeopleProcessingJob,
} from "~/lib/sweden-people-processing-jobs.server";
import {
  swedenPeopleTemporalClient,
  swedenPeopleTemporalTaskQueue,
} from "~/lib/sweden-people-temporal-client.server";
import {
  readSwedenRoleMappings,
  type StoredSourceRoleMapping,
} from "~/lib/sweden-role-mappings.server";

export const PEOPLE_DRAFT_STEP_2_TABLE = "people_draft_step_2";
const PEOPLE_DRAFT_STEP_2_BUILD_TABLE = "people_draft_step_2_build";
const PEOPLE_DRAFT_STEP_2_JOB_TABLE =
  "people_draft_step_2_initialization_job";
const PEOPLE_DRAFT_STEP_2_JOB_TYPE = "draft_2_rebuild" as const;
const ROLE_MAPPING_TABLE = "draft_2_role_mapping";
const PROGRESS_UPDATE_INTERVAL_MS = 250;

const migrationGlobal = globalThis as typeof globalThis & {
  __swedenPeopleDraftTwoJobMigrations?: Map<string, Promise<void>>;
};
const jobMigrations =
  migrationGlobal.__swedenPeopleDraftTwoJobMigrations ?? new Map();
migrationGlobal.__swedenPeopleDraftTwoJobMigrations = jobMigrations;

export interface SwedenPeopleDraftTwoStatus {
  tableExists: boolean;
  rowCount: number;
}

export interface SwedenPeopleDraftOneRow {
  observation_id: string;
  company_id: string;
  name: string;
  source: string;
  role_original: string | null;
  fiscal_year: number | null;
  description: string | null;
  source_entity_id: string;
  source_record_uid: string;
  source_profile_hash: string;
  source_role_hash: string;
  source_payload_json: string | null;
  source_observed_at: string | null;
}

export type SwedenPeopleDraftSourceObservation = SwedenPeopleDraftOneRow;

export type SwedenPeopleDraftSource = "bolagsverket" | "esef" | "wikidata";

export interface SwedenPeopleDraftRowsPage<Row> {
  rows: Row[];
  page: number;
  pageSize: number;
  totalRows: number;
  totalPages: number;
}

export interface SwedenPeopleDraftTwoRow {
  draft_2_id: string;
  company_id: string;
  name: string;
  position: string;
  start_year: number | null;
  end_year: number | null;
  source_count: number;
  observation_count: number;
  bolagsverket_source_ids: string[];
  bolagsverket_descriptions: string[];
  esef_source_ids: string[];
  esef_descriptions: string[];
  wikidata_source_ids: string[];
  wikidata_descriptions: string[];
  has_llm_suggestion?: boolean;
  /** Deterministic ClickHouse `se_company_person` id, for the review page link. */
  se_person_id?: string;
  source_observations?: SwedenPeopleDraftSourceObservation[];
}

export type SwedenPeopleDraftTwoJobStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed";

export type SwedenPeopleDraftTwoJobPhase =
  | "queued"
  | "validating"
  | "matching"
  | "publishing"
  | "completed"
  | "failed";

export interface SwedenPeopleDraftTwoJob {
  jobId: string;
  workflowId: string;
  status: SwedenPeopleDraftTwoJobStatus;
  phase: SwedenPeopleDraftTwoJobPhase;
  processedRows: number;
  totalRows: number;
  outputRows: number;
  skippedRolelessRows: number;
  skippedUnmappedRows: number;
  unmappedRoleExamples: string[];
  progressPercent: number;
  message: string;
  errorMessage: string;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
  updatedAt: string;
}

interface StoredDraftTwoJob {
  job_id: string;
  status: SwedenPeopleDraftTwoJobStatus;
  phase: SwedenPeopleDraftTwoJobPhase;
  processed_rows: number | string | bigint;
  total_rows: number | string | bigint;
  output_rows: number | string | bigint;
  skipped_roleless_rows: number | string | bigint;
  skipped_unmapped_rows: number | string | bigint;
  unmapped_role_examples_json: string;
  progress_percent: number | string | bigint;
  message: string;
  error_message: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
}

interface CountRow {
  row_count: number | string | bigint;
}

function emptyRowsPage<Row>(pageSize: number): SwedenPeopleDraftRowsPage<Row> {
  return {
    rows: [],
    page: 1,
    pageSize,
    totalRows: 0,
    totalPages: 0,
  };
}

function paginationBounds({
  requestedPage,
  requestedPageSize,
  totalRows,
}: {
  requestedPage: number;
  requestedPageSize: number;
  totalRows: number;
}) {
  const pageSize = Math.max(1, Math.min(100, Math.floor(requestedPageSize)));
  const totalPages = Math.ceil(totalRows / pageSize);
  const page =
    totalPages === 0
      ? 1
      : Math.min(
          totalPages,
          Math.max(1, Math.floor(requestedPage) || 1),
        );
  return {
    page,
    pageSize,
    totalPages,
    offset: (page - 1) * pageSize,
  };
}

interface DraftTwoInputStatistics {
  eligible_row_count: number | string | bigint;
  roleless_row_count: number | string | bigint;
  unmapped_row_count: number | string | bigint;
  unmapped_roles: string[] | null;
}

export class SwedenPeopleDraftTwoReplacementConfirmationError extends Error {
  readonly rowCount: number;

  constructor(rowCount: number) {
    super(
      `${PEOPLE_DRAFT_STEP_2_TABLE} contains ${rowCount} rows; explicit confirmation is required to replace them.`,
    );
    this.name = "SwedenPeopleDraftTwoReplacementConfirmationError";
    this.rowCount = rowCount;
  }
}

async function tableExists(
  connection: DuckDBConnection,
  tableName: string,
): Promise<boolean> {
  const reader = await connection.runAndReadAll(
    `SELECT count(*) AS row_count
     FROM information_schema.tables
     WHERE table_schema = 'main' AND table_name = $table_name`,
    { table_name: tableName },
  );
  const [row] = reader.getRowObjectsJson() as unknown as CountRow[];
  return Number(row?.row_count ?? 0) > 0;
}

function mapStoredJob(row: StoredDraftTwoJob): SwedenPeopleDraftTwoJob {
  return {
    jobId: row.job_id,
    workflowId: `backoffice-se-people-draft-2-${row.job_id}`,
    status: row.status,
    phase: row.phase,
    processedRows: Number(row.processed_rows),
    totalRows: Number(row.total_rows),
    outputRows: Number(row.output_rows),
    skippedRolelessRows: Number(row.skipped_roleless_rows),
    skippedUnmappedRows: Number(row.skipped_unmapped_rows ?? 0),
    unmappedRoleExamples: JSON.parse(
      row.unmapped_role_examples_json || "[]",
    ) as string[],
    progressPercent: Number(row.progress_percent),
    message: row.message,
    errorMessage: row.error_message,
    createdAt: row.created_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    updatedAt: row.updated_at,
  };
}

async function migrateLegacyDraftTwoJobs(
  connection: DuckDBConnection,
  jobDatabase: DatabaseSync,
): Promise<void> {
  if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_2_JOB_TABLE))) {
    return;
  }
  const reader = await connection.runAndReadAll(
    `SELECT *
     FROM ${PEOPLE_DRAFT_STEP_2_JOB_TABLE}
     ORDER BY created_at`,
  );
  const rows = reader.getRowObjectsJson() as unknown as StoredDraftTwoJob[];
  const interruptedAt = new Date().toISOString();
  let interruptedBuild = false;

  jobDatabase.exec("BEGIN IMMEDIATE");
  try {
    for (const row of rows) {
      let job = mapStoredJob(row);
      if (job.status === "queued" || job.status === "running") {
        interruptedBuild = true;
        job = {
          ...job,
          status: "failed",
          phase: "failed",
          message: "Draft 2 build was interrupted",
          errorMessage:
            "The previous in-process background worker stopped before Draft 2 completed.",
          completedAt: interruptedAt,
          updatedAt: interruptedAt,
        };
      }
      importLegacySwedenPeopleProcessingJob(
        jobDatabase,
        PEOPLE_DRAFT_STEP_2_JOB_TYPE,
        job,
      );
    }
    jobDatabase.exec("COMMIT");
  } catch (error) {
    jobDatabase.exec("ROLLBACK");
    throw error;
  }

  if (interruptedBuild) {
    await connection.run(
      `DROP TABLE IF EXISTS ${PEOPLE_DRAFT_STEP_2_BUILD_TABLE}`,
    );
  }
  await connection.run(`DROP TABLE ${PEOPLE_DRAFT_STEP_2_JOB_TABLE}`);
}

async function ensureLegacyDraftTwoJobsMigrated(
  databasePath: string,
  curationDatabasePath: string,
): Promise<void> {
  if (!existsSync(databasePath)) return;
  const migrationKey = `${resolve(databasePath)}\n${resolve(curationDatabasePath)}`;
  let migration = jobMigrations.get(migrationKey);
  if (!migration) {
    migration = (async () => {
      const connection = await connectSwedenPeopleDraftDatabase(databasePath);
      const jobDatabase = connectCurationDatabase(curationDatabasePath);
      try {
        ensureSwedenPeopleProcessingJobTable(jobDatabase);
        await migrateLegacyDraftTwoJobs(connection, jobDatabase);
      } finally {
        connection.closeSync();
        jobDatabase.close();
      }
    })();
    jobMigrations.set(migrationKey, migration);
  }
  try {
    await migration;
  } catch (error) {
    jobMigrations.delete(migrationKey);
    throw error;
  }
}

async function createRoleMappingTable(
  connection: DuckDBConnection,
  mappings: StoredSourceRoleMapping[],
): Promise<void> {
  await connection.run(`DROP TABLE IF EXISTS ${ROLE_MAPPING_TABLE}`);
  await connection.run(
    `CREATE TEMP TABLE ${ROLE_MAPPING_TABLE} (
      source VARCHAR NOT NULL,
      source_role_code VARCHAR NOT NULL,
      source_role_name VARCHAR NOT NULL,
      canonical_role_code VARCHAR,
      mapping_status VARCHAR NOT NULL
    )`,
  );
  const appender = await connection.createAppender(ROLE_MAPPING_TABLE);
  try {
    for (const mapping of mappings) {
      appender.appendVarchar(mapping.source);
      appender.appendVarchar(mapping.source_role_code);
      appender.appendVarchar(mapping.source_role_name);
      if (mapping.canonical_role_code === null) {
        appender.appendNull();
      } else {
        appender.appendVarchar(mapping.canonical_role_code);
      }
      appender.appendVarchar(mapping.mapping_status);
      appender.endRow();
    }
  } finally {
    appender.closeSync();
  }
}

function mappedSourceRowsCtes(): string {
  return `parsed_source_rows AS (
    SELECT
      observations.*,
      lower(regexp_replace(trim(observations.name), '\\s+', ' ', 'g'))
        AS name_normalized,
      CASE observations.source
        WHEN 'bolagsverket' THEN json_extract_string(
          observations.source_payload_json,
          '$.role_kind'
        )
        WHEN 'esef' THEN json_extract_string(
          observations.source_payload_json,
          '$.role_category'
        )
        WHEN 'wikidata' THEN json_extract_string(
          observations.source_payload_json,
          '$.role_property'
        )
        ELSE ''
      END AS source_role_code,
      CASE observations.source
        WHEN 'esef' THEN coalesce(
          year(try_cast(json_extract_string(
            observations.source_payload_json,
            '$.effective_from'
          ) AS DATE)),
          observations.fiscal_year
        )
        WHEN 'wikidata' THEN year(try_cast(json_extract_string(
          observations.source_payload_json,
          '$.start_date'
        ) AS DATE))
        ELSE observations.fiscal_year
      END AS source_start_year,
      CASE observations.source
        WHEN 'esef' THEN coalesce(
          year(try_cast(json_extract_string(
            observations.source_payload_json,
            '$.effective_to'
          ) AS DATE)),
          observations.fiscal_year
        )
        WHEN 'wikidata' THEN year(try_cast(json_extract_string(
          observations.source_payload_json,
          '$.end_date'
        ) AS DATE))
        ELSE observations.fiscal_year
      END AS source_end_year,
      CASE
        WHEN observations.source = 'esef'
          AND json_extract_string(
            observations.source_payload_json,
            '$.status'
          ) = 'current'
          AND json_extract_string(
            observations.source_payload_json,
            '$.effective_to'
          ) IS NULL
          THEN true
        WHEN observations.source = 'wikidata'
          AND try_cast(json_extract_string(
            observations.source_payload_json,
            '$.is_current'
          ) AS INTEGER) = 1
          AND json_extract_string(
            observations.source_payload_json,
            '$.end_date'
          ) IS NULL
          THEN true
        ELSE false
      END AS is_open_ended
    FROM ${PEOPLE_DRAFT_STEP_1_TABLE} AS observations
  ),
  mapping_candidates AS (
    SELECT
      parsed.*,
      mappings.canonical_role_code,
      mappings.mapping_status,
      row_number() OVER (
        PARTITION BY parsed.observation_id
        ORDER BY
          CASE
            WHEN mappings.source_role_name = coalesce(parsed.role_original, '')
              AND mappings.source_role_name != ''
              THEN 0
            ELSE 1
          END,
          mappings.source_role_name
      ) AS mapping_rank
    FROM parsed_source_rows AS parsed
    LEFT JOIN ${ROLE_MAPPING_TABLE} AS mappings
      ON mappings.source = parsed.source
     AND mappings.source_role_code = parsed.source_role_code
     AND (
       mappings.source_role_name = ''
       OR mappings.source_role_name = coalesce(parsed.role_original, '')
     )
  ),
  mapped_source_rows AS (
    SELECT * EXCLUDE (mapping_rank)
    FROM mapping_candidates
    WHERE mapping_rank = 1
  )`;
}

function inputStatisticsSql(): string {
  return `WITH ${mappedSourceRowsCtes()}
  SELECT
    count(*) FILTER (
      WHERE mapping_status = 'mapped' AND canonical_role_code IS NOT NULL
    ) AS eligible_row_count,
    count(*) FILTER (WHERE mapping_status = 'roleless') AS roleless_row_count,
    count(*) FILTER (
      WHERE mapping_status IS NULL
        AND (
          coalesce(source_role_code, '') != ''
          OR coalesce(trim(role_original), '') != ''
        )
    ) AS unmapped_row_count,
    list_slice(
      list_sort(list_distinct(list(
        source || ':' || coalesce(source_role_code, '') || ':' ||
        coalesce(role_original, '')
      ) FILTER (
        WHERE mapping_status IS NULL
          AND (
            coalesce(source_role_code, '') != ''
            OR coalesce(trim(role_original), '') != ''
          )
      ))),
      1,
      10
    ) AS unmapped_roles
  FROM mapped_source_rows`;
}

export function buildDraftTwoTableSql(): string {
  return `CREATE TABLE ${PEOPLE_DRAFT_STEP_2_BUILD_TABLE} AS
  WITH ${mappedSourceRowsCtes()},
  eligible_source_rows AS (
    SELECT
      *,
      split_part(name_normalized, ' ', 1) || '|' ||
        list_extract(str_split(name_normalized, ' '), -1) AS name_match_key
    FROM mapped_source_rows
    WHERE mapping_status = 'mapped'
      AND canonical_role_code IS NOT NULL
      AND name_normalized != ''
  ),
  merged_people AS (
    SELECT
      company_id,
      name_match_key,
      canonical_role_code AS position,
      first(
        name
        ORDER BY
          CASE source
            WHEN 'esef' THEN 3
            WHEN 'bolagsverket' THEN 2
            ELSE 1
          END DESC,
          source_observed_at DESC,
          observation_id
      ) AS name,
      min(source_start_year) AS start_year,
      CASE
        WHEN bool_or(is_open_ended) THEN NULL
        ELSE max(source_end_year)
      END AS end_year,
      count(DISTINCT source) AS source_count,
      count(*) AS observation_count,
      coalesce(
        list_sort(list_distinct(list(observation_id) FILTER (
          WHERE source = 'bolagsverket'
        ))),
        []::VARCHAR[]
      ) AS bolagsverket_source_ids,
      coalesce(
        list_sort(list_distinct(list(description) FILTER (
          WHERE source = 'bolagsverket'
            AND description IS NOT NULL
            AND trim(description) != ''
        ))),
        []::VARCHAR[]
      ) AS bolagsverket_descriptions,
      coalesce(
        list_sort(list_distinct(list(observation_id) FILTER (
          WHERE source = 'esef'
        ))),
        []::VARCHAR[]
      ) AS esef_source_ids,
      coalesce(
        list_sort(list_distinct(list(description) FILTER (
          WHERE source = 'esef'
            AND description IS NOT NULL
            AND trim(description) != ''
        ))),
        []::VARCHAR[]
      ) AS esef_descriptions,
      coalesce(
        list_sort(list_distinct(list(observation_id) FILTER (
          WHERE source = 'wikidata'
        ))),
        []::VARCHAR[]
      ) AS wikidata_source_ids,
      coalesce(
        list_sort(list_distinct(list(description) FILTER (
          WHERE source = 'wikidata'
            AND description IS NOT NULL
            AND trim(description) != ''
        ))),
        []::VARCHAR[]
      ) AS wikidata_descriptions
    FROM eligible_source_rows
    GROUP BY company_id, name_match_key, canonical_role_code
  )
  SELECT
    sha256(
      'se-company-person-draft-2-v1\\n' || company_id || '\\n' ||
      name_match_key || '\\n' || position
    ) AS draft_2_id,
    company_id,
    name,
    position,
    start_year::INTEGER AS start_year,
    end_year::INTEGER AS end_year,
    source_count::INTEGER AS source_count,
    observation_count::INTEGER AS observation_count,
    bolagsverket_source_ids,
    bolagsverket_descriptions,
    esef_source_ids,
    esef_descriptions,
    wikidata_source_ids,
    wikidata_descriptions,
    $created_at::VARCHAR AS created_at
  FROM merged_people`;
}

async function runPendingBuild(
  transformConnection: DuckDBConnection,
  jobDatabase: DatabaseSync,
  job: SwedenPeopleDraftTwoJob,
  createdAt: string,
  reportProgress: (job: SwedenPeopleDraftTwoJob) => void,
): Promise<SwedenPeopleDraftTwoJob> {
  const pending = await transformConnection.start(buildDraftTwoTableSql(), {
    created_at: createdAt,
  });
  let currentJob = job;
  let lastProgressUpdate = 0;

  while (true) {
    const state = pending.runTask();
    const now = Date.now();
    if (now - lastProgressUpdate >= PROGRESS_UPDATE_INTERVAL_MS) {
      const rawPercentage = transformConnection.progress.percentage;
      const queryPercentage = Math.max(
        0,
        Math.min(100, rawPercentage <= 1 ? rawPercentage * 100 : rawPercentage),
      );
      const processedRows = Math.min(
        currentJob.totalRows,
        Math.floor((currentJob.totalRows * queryPercentage) / 100),
      );
      currentJob = {
        ...currentJob,
        processedRows,
        progressPercent: Math.min(
          90,
          15 + Math.floor(queryPercentage * 0.75),
        ),
        updatedAt: new Date().toISOString(),
      };
      persistSwedenPeopleProcessingJob(jobDatabase, currentJob);
      reportProgress(currentJob);
      lastProgressUpdate = now;
    }
    if (state === DuckDBPendingResultState.RESULT_READY) break;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 1));
  }
  await pending.getResult();
  return {
    ...currentJob,
    processedRows: currentJob.totalRows,
    progressPercent: 90,
    updatedAt: new Date().toISOString(),
  };
}

export async function executeSwedenPeopleDraftTwoBuild(
  jobId: string,
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
  curationDatabasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
  reportProgress: (job: SwedenPeopleDraftTwoJob) => void = () => {},
): Promise<SwedenPeopleDraftTwoJob | null> {
  const jobDatabase = connectCurationDatabase(curationDatabasePath);
  ensureSwedenPeopleProcessingJobTable(jobDatabase);
  const transformConnection =
    await connectSwedenPeopleDraftDatabase(databasePath);
  let job = readSwedenPeopleProcessingJob<SwedenPeopleDraftTwoJob>(
    jobDatabase,
    jobId,
  );
  if (!job) {
    jobDatabase.close();
    transformConnection.closeSync();
    return null;
  }

  const startedAt = new Date().toISOString();
  job = {
    ...job,
    status: "running",
    phase: "validating",
    progressPercent: 5,
    message: "Validating source role mappings",
    errorMessage: "",
    startedAt,
    completedAt: null,
    updatedAt: startedAt,
  };
  persistSwedenPeopleProcessingJob(jobDatabase, job);
  reportProgress(job);

  try {
    await createRoleMappingTable(
      transformConnection,
      readSwedenRoleMappings(),
    );
    const statisticsReader = await transformConnection.runAndReadAll(
      inputStatisticsSql(),
    );
    const [statistics] =
      statisticsReader.getRowObjectsJson() as unknown as DraftTwoInputStatistics[];
    job = {
      ...job,
      phase: "matching",
      totalRows: Number(statistics?.eligible_row_count ?? 0),
      skippedRolelessRows: Number(statistics?.roleless_row_count ?? 0),
      skippedUnmappedRows: Number(statistics?.unmapped_row_count ?? 0),
      unmappedRoleExamples: statistics?.unmapped_roles ?? [],
      progressPercent: 15,
      message: "Matching people and positions across sources",
      updatedAt: new Date().toISOString(),
    };
    persistSwedenPeopleProcessingJob(jobDatabase, job);
    reportProgress(job);

    await transformConnection.run(
      `DROP TABLE IF EXISTS ${PEOPLE_DRAFT_STEP_2_BUILD_TABLE}`,
    );
    job = await runPendingBuild(
      transformConnection,
      jobDatabase,
      job,
      startedAt,
      reportProgress,
    );
    const outputReader = await transformConnection.runAndReadAll(
      `SELECT count(*) AS row_count FROM ${PEOPLE_DRAFT_STEP_2_BUILD_TABLE}`,
    );
    const [output] = outputReader.getRowObjectsJson() as unknown as CountRow[];
    job = {
      ...job,
      phase: "publishing",
      outputRows: Number(output?.row_count ?? 0),
      progressPercent: 95,
      message: "Publishing Draft 2 atomically",
      updatedAt: new Date().toISOString(),
    };
    persistSwedenPeopleProcessingJob(jobDatabase, job);
    reportProgress(job);

    const completedAt = new Date().toISOString();
    const completedJob: SwedenPeopleDraftTwoJob = {
      ...job,
      status: "completed",
      phase: "completed",
      progressPercent: 100,
      message: "Draft 2 build completed",
      completedAt,
      updatedAt: completedAt,
    };
    await transformConnection.run("BEGIN TRANSACTION");
    try {
      await transformConnection.run(
        `DROP TABLE IF EXISTS ${PEOPLE_DRAFT_STEP_2_TABLE}`,
      );
      await transformConnection.run(
        `ALTER TABLE ${PEOPLE_DRAFT_STEP_2_BUILD_TABLE}
         RENAME TO ${PEOPLE_DRAFT_STEP_2_TABLE}`,
      );
      await transformConnection.run("COMMIT");
      job = completedJob;
    } catch (error) {
      await transformConnection.run("ROLLBACK");
      throw error;
    }
    persistSwedenPeopleProcessingJob(jobDatabase, completedJob);
    reportProgress(completedJob);

    try {
      await transformConnection.run("CHECKPOINT");
    } catch (error) {
      console.warn("Sweden Draft 2 checkpoint failed", { jobId, error });
    }
  } catch (error) {
    await transformConnection.run(
      `DROP TABLE IF EXISTS ${PEOPLE_DRAFT_STEP_2_BUILD_TABLE}`,
    );
    const failedAt = new Date().toISOString();
    const detail = error instanceof Error ? error.message : "Unknown error";
    job = {
      ...job,
      status: "failed",
      phase: "failed",
      message: "Draft 2 build failed",
      errorMessage: detail,
      completedAt: failedAt,
      updatedAt: failedAt,
    };
    persistSwedenPeopleProcessingJob(jobDatabase, job);
    reportProgress(job);
    console.error("Sweden Draft 2 build failed", { jobId, error });
  } finally {
    jobDatabase.close();
    transformConnection.closeSync();
  }
  return job;
}

export async function getSwedenPeopleDraftTwoStatus(
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
): Promise<SwedenPeopleDraftTwoStatus> {
  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  try {
    if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_2_TABLE))) {
      return { tableExists: false, rowCount: 0 };
    }
    const reader = await connection.runAndReadAll(
      `SELECT count(*) AS row_count FROM ${PEOPLE_DRAFT_STEP_2_TABLE}`,
    );
    const [row] = reader.getRowObjectsJson() as unknown as CountRow[];
    return { tableExists: true, rowCount: Number(row?.row_count ?? 0) };
  } finally {
    connection.closeSync();
  }
}

export async function getLatestSwedenPeopleDraftTwoJob(
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
  curationDatabasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): Promise<SwedenPeopleDraftTwoJob | null> {
  await ensureLegacyDraftTwoJobsMigrated(
    databasePath,
    curationDatabasePath,
  );
  const jobDatabase = connectCurationDatabase(curationDatabasePath);
  try {
    ensureSwedenPeopleProcessingJobTable(jobDatabase);
    return readLatestSwedenPeopleProcessingJob<SwedenPeopleDraftTwoJob>(
      jobDatabase,
      PEOPLE_DRAFT_STEP_2_JOB_TYPE,
    );
  } finally {
    jobDatabase.close();
  }
}

export async function startSwedenPeopleDraftTwoBuild({
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
  curationDatabasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
  confirmReplacement = false,
}: {
  databasePath?: string;
  curationDatabasePath?: string;
  confirmReplacement?: boolean;
} = {}): Promise<SwedenPeopleDraftTwoJob> {
  await ensureLegacyDraftTwoJobsMigrated(
    databasePath,
    curationDatabasePath,
  );
  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  const jobDatabase = connectCurationDatabase(curationDatabasePath);
  let sqliteTransactionStarted = false;
  let job: SwedenPeopleDraftTwoJob;
  try {
    ensureSwedenPeopleProcessingJobTable(jobDatabase);
    if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_1_TABLE))) {
      throw new Error("Draft 1 must be initialized before building Draft 2.");
    }

    jobDatabase.exec("BEGIN IMMEDIATE");
    sqliteTransactionStarted = true;
    const activeJob =
      readActiveSwedenPeopleProcessingJob<SwedenPeopleDraftTwoJob>(
        jobDatabase,
        PEOPLE_DRAFT_STEP_2_JOB_TYPE,
      );
    if (activeJob) {
      jobDatabase.exec("COMMIT");
      sqliteTransactionStarted = false;
      return activeJob;
    }

    let currentRowCount = 0;
    if (await tableExists(connection, PEOPLE_DRAFT_STEP_2_TABLE)) {
      const countReader = await connection.runAndReadAll(
        `SELECT count(*) AS row_count FROM ${PEOPLE_DRAFT_STEP_2_TABLE}`,
      );
      const [row] = countReader.getRowObjectsJson() as unknown as CountRow[];
      currentRowCount = Number(row?.row_count ?? 0);
    }
    if (currentRowCount > 0 && !confirmReplacement) {
      throw new SwedenPeopleDraftTwoReplacementConfirmationError(
        currentRowCount,
      );
    }

    const now = new Date().toISOString();
    const jobId = randomUUID();
    job = {
      jobId,
      workflowId: `backoffice-se-people-draft-2-${jobId}`,
      status: "queued",
      phase: "queued",
      processedRows: 0,
      totalRows: 0,
      outputRows: 0,
      skippedRolelessRows: 0,
      skippedUnmappedRows: 0,
      unmappedRoleExamples: [],
      progressPercent: 0,
      message: "Draft 2 build queued",
      errorMessage: "",
      createdAt: now,
      startedAt: null,
      completedAt: null,
      updatedAt: now,
    };
    insertSwedenPeopleProcessingJob(
      jobDatabase,
      PEOPLE_DRAFT_STEP_2_JOB_TYPE,
      job,
    );
    jobDatabase.exec("COMMIT");
    sqliteTransactionStarted = false;
  } catch (error) {
    if (sqliteTransactionStarted) jobDatabase.exec("ROLLBACK");
    throw error;
  } finally {
    connection.closeSync();
    jobDatabase.close();
  }

  try {
    const client = await swedenPeopleTemporalClient();
    await client.workflow.start("swedenPeopleDraftTwoWorkflow", {
      workflowId: job.workflowId,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      taskQueue: swedenPeopleTemporalTaskQueue(),
      args: [{ jobId: job.jobId }],
      memo: {
        country: "SE",
        operation: "draft-2-rebuild",
      },
    });
  } catch (error) {
    const failedAt = new Date().toISOString();
    const failedJob: SwedenPeopleDraftTwoJob = {
      ...job,
      status: "failed",
      phase: "failed",
      message: "Draft 2 build could not start",
      errorMessage: "Temporal could not start the Draft 2 rebuild workflow.",
      completedAt: failedAt,
      updatedAt: failedAt,
    };
    const failedJobDatabase = connectCurationDatabase(curationDatabasePath);
    try {
      ensureSwedenPeopleProcessingJobTable(failedJobDatabase);
      persistSwedenPeopleProcessingJob(failedJobDatabase, failedJob);
    } finally {
      failedJobDatabase.close();
    }
    throw error;
  }
  return job;
}

export async function getSwedenPeopleDraftOneRowsPage({
  companyId = "",
  onlyUnmapped = false,
  page = 1,
  pageSize = 25,
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
}: {
  companyId?: string;
  onlyUnmapped?: boolean;
  page?: number;
  pageSize?: number;
  databasePath?: string;
} = {}): Promise<SwedenPeopleDraftRowsPage<SwedenPeopleDraftOneRow>> {
  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  try {
    if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_1_TABLE))) {
      return emptyRowsPage(pageSize);
    }
    if (onlyUnmapped) {
      await createRoleMappingTable(connection, readSwedenRoleMappings());
    }
    const sourceRowsSql = onlyUnmapped
      ? `WITH ${mappedSourceRowsCtes()}
         SELECT *
         FROM mapped_source_rows
         WHERE mapping_status IS NULL
           AND (
             coalesce(source_role_code, '') != ''
             OR coalesce(trim(role_original), '') != ''
           )`
      : `SELECT * FROM ${PEOPLE_DRAFT_STEP_1_TABLE}`;

    const parameters = {
      all_companies: companyId === "",
      company_id: companyId,
    };
    const countReader = await connection.runAndReadAll(
      `SELECT count(*) AS row_count
       FROM (${sourceRowsSql}) AS observations
       WHERE ($all_companies OR company_id = $company_id)`,
      parameters,
    );
    const [countRow] =
      countReader.getRowObjectsJson() as unknown as CountRow[];
    const totalRows = Number(countRow?.row_count ?? 0);
    const bounds = paginationBounds({
      requestedPage: page,
      requestedPageSize: pageSize,
      totalRows,
    });
    const reader = await connection.runAndReadAll(
      `SELECT
        observation_id,
        company_id,
        name,
        source,
        role_original,
        fiscal_year::INTEGER AS fiscal_year,
        description,
        source_entity_id,
        source_record_uid,
        source_profile_hash,
        source_role_hash,
        source_payload_json,
        source_observed_at
       FROM (${sourceRowsSql}) AS observations
       WHERE ($all_companies OR company_id = $company_id)
       ORDER BY company_id, name, fiscal_year NULLS LAST, source, observation_id
       LIMIT $row_limit OFFSET $row_offset`,
      {
        ...parameters,
        row_limit: bounds.pageSize,
        row_offset: bounds.offset,
      },
    );
    return {
      rows:
        reader.getRowObjectsJson() as unknown as SwedenPeopleDraftOneRow[],
      page: bounds.page,
      pageSize: bounds.pageSize,
      totalRows,
      totalPages: bounds.totalPages,
    };
  } finally {
    connection.closeSync();
  }
}

export async function getSwedenPeopleDraftOneRows({
  companyId = "",
  onlyUnmapped = false,
  rowLimit = 25,
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
}: {
  companyId?: string;
  onlyUnmapped?: boolean;
  rowLimit?: number;
  databasePath?: string;
} = {}): Promise<SwedenPeopleDraftOneRow[]> {
  const result = await getSwedenPeopleDraftOneRowsPage({
    companyId,
    onlyUnmapped,
    pageSize: rowLimit,
    databasePath,
  });
  return result.rows;
}

async function loadDraftTwoSourceObservations(
  connection: DuckDBConnection,
  rows: SwedenPeopleDraftTwoRow[],
): Promise<void> {
  const observationIds = [
    ...new Set(
      rows.flatMap((row) => [
        ...row.bolagsverket_source_ids,
        ...row.esef_source_ids,
        ...row.wikidata_source_ids,
      ]),
    ),
  ];
  if (observationIds.length === 0) return;

  await connection.run(
    `CREATE TEMP TABLE selected_draft_2_observation (
      observation_id VARCHAR NOT NULL
    )`,
  );
  const appender = await connection.createAppender(
    "selected_draft_2_observation",
  );
  try {
    for (const observationId of observationIds) {
      appender.appendVarchar(observationId);
      appender.endRow();
    }
  } finally {
    appender.closeSync();
  }

  const reader = await connection.runAndReadAll(
    `SELECT
      observations.observation_id,
      observations.company_id,
      observations.name,
      observations.source,
      observations.role_original,
      observations.fiscal_year::INTEGER AS fiscal_year,
      observations.description,
      observations.source_entity_id,
      observations.source_record_uid,
      observations.source_profile_hash,
      observations.source_role_hash,
      observations.source_payload_json,
      observations.source_observed_at
     FROM ${PEOPLE_DRAFT_STEP_1_TABLE} AS observations
     INNER JOIN selected_draft_2_observation AS selected
       USING (observation_id)
     ORDER BY
       observations.source,
       observations.fiscal_year NULLS LAST,
       observations.observation_id`,
  );
  const observations =
    reader.getRowObjectsJson() as unknown as SwedenPeopleDraftSourceObservation[];
  const observationsById = new Map(
    observations.map((observation) => [observation.observation_id, observation]),
  );

  for (const row of rows) {
    row.source_observations = [
      ...row.bolagsverket_source_ids,
      ...row.esef_source_ids,
      ...row.wikidata_source_ids,
    ]
      .map((observationId) => observationsById.get(observationId))
      .filter(
        (observation): observation is SwedenPeopleDraftSourceObservation =>
          observation !== undefined,
      );
  }
}

export async function getSwedenPeopleDraftTwoRowsPage({
  companyId = "",
  onlyMultipleSources = false,
  selectedSources = [],
  draftTwoIds,
  page = 1,
  pageSize = 25,
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
}: {
  companyId?: string;
  onlyMultipleSources?: boolean;
  selectedSources?: SwedenPeopleDraftSource[];
  draftTwoIds?: string[];
  page?: number;
  pageSize?: number;
  databasePath?: string;
} = {}): Promise<SwedenPeopleDraftRowsPage<SwedenPeopleDraftTwoRow>> {
  if (draftTwoIds !== undefined && draftTwoIds.length === 0) {
    return emptyRowsPage(pageSize);
  }
  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  try {
    if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_2_TABLE))) {
      return emptyRowsPage(pageSize);
    }
    const selectedSourceSet = new Set(selectedSources);
    const parameters = {
      all_companies: companyId === "",
      company_id: companyId,
      all_source_counts: !onlyMultipleSources,
      all_bolagsverket_sources: !selectedSourceSet.has("bolagsverket"),
      all_esef_sources: !selectedSourceSet.has("esef"),
      all_wikidata_sources: !selectedSourceSet.has("wikidata"),
    };
    let draftTwoIdCondition = "";
    if (draftTwoIds !== undefined) {
      await connection.run(
        `CREATE TEMP TABLE selected_draft_2_id (
          draft_2_id VARCHAR NOT NULL
        )`,
      );
      const appender = await connection.createAppender("selected_draft_2_id");
      try {
        for (const draftTwoId of new Set(draftTwoIds)) {
          appender.appendVarchar(draftTwoId);
          appender.endRow();
        }
      } finally {
        appender.closeSync();
      }
      draftTwoIdCondition = `
        AND EXISTS (
          SELECT 1
          FROM selected_draft_2_id AS selected
          WHERE selected.draft_2_id = people.draft_2_id
        )`;
    }
    const filteredRowsSql = `
      FROM ${PEOPLE_DRAFT_STEP_2_TABLE} AS people
      WHERE ($all_companies OR company_id = $company_id)
        AND ($all_source_counts OR source_count > 1)
        AND ($all_bolagsverket_sources OR len(bolagsverket_source_ids) > 0)
        AND ($all_esef_sources OR len(esef_source_ids) > 0)
        AND ($all_wikidata_sources OR len(wikidata_source_ids) > 0)
        ${draftTwoIdCondition}`;
    const countReader = await connection.runAndReadAll(
      `SELECT count(*) AS row_count
       ${filteredRowsSql}`,
      parameters,
    );
    const [countRow] =
      countReader.getRowObjectsJson() as unknown as CountRow[];
    const totalRows = Number(countRow?.row_count ?? 0);
    const bounds = paginationBounds({
      requestedPage: page,
      requestedPageSize: pageSize,
      totalRows,
    });
    const reader = await connection.runAndReadAll(
      `SELECT people.* EXCLUDE (created_at)
       ${filteredRowsSql}
       ORDER BY company_id, name, position, start_year NULLS LAST
       LIMIT $row_limit OFFSET $row_offset`,
      {
        ...parameters,
        row_limit: bounds.pageSize,
        row_offset: bounds.offset,
      },
    );
    const rows =
      reader.getRowObjectsJson() as unknown as SwedenPeopleDraftTwoRow[];
    await loadDraftTwoSourceObservations(connection, rows);
    return {
      rows,
      page: bounds.page,
      pageSize: bounds.pageSize,
      totalRows,
      totalPages: bounds.totalPages,
    };
  } finally {
    connection.closeSync();
  }
}

export async function getSwedenPeopleDraftTwoRows({
  companyId = "",
  onlyMultipleSources = false,
  selectedSources = [],
  draftTwoIds,
  rowLimit = 25,
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
}: {
  companyId?: string;
  onlyMultipleSources?: boolean;
  selectedSources?: SwedenPeopleDraftSource[];
  draftTwoIds?: string[];
  rowLimit?: number;
  databasePath?: string;
} = {}): Promise<SwedenPeopleDraftTwoRow[]> {
  const result = await getSwedenPeopleDraftTwoRowsPage({
    companyId,
    onlyMultipleSources,
    selectedSources,
    draftTwoIds,
    pageSize: rowLimit,
    databasePath,
  });
  return result.rows;
}

export async function listSwedenPeopleDraftTwoLlmCandidateIds({
  companyId = "",
  selectedSources = [],
  draftTwoIds,
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
}: {
  companyId?: string;
  selectedSources?: SwedenPeopleDraftSource[];
  draftTwoIds?: string[];
  databasePath?: string;
} = {}): Promise<string[]> {
  if (draftTwoIds !== undefined && draftTwoIds.length === 0) return [];

  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  try {
    if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_2_TABLE))) return [];

    let selectedIdCondition = "";
    if (draftTwoIds !== undefined) {
      await connection.run(
        `CREATE TEMP TABLE selected_llm_draft_2_id (
          draft_2_id VARCHAR NOT NULL
        )`,
      );
      const appender = await connection.createAppender(
        "selected_llm_draft_2_id",
      );
      try {
        for (const draftTwoId of new Set(draftTwoIds)) {
          appender.appendVarchar(draftTwoId);
          appender.endRow();
        }
      } finally {
        appender.closeSync();
      }
      selectedIdCondition = `
        AND EXISTS (
          SELECT 1
          FROM selected_llm_draft_2_id AS selected
          WHERE selected.draft_2_id = people.draft_2_id
        )`;
    }

    const selectedSourceSet = new Set(selectedSources);
    const reader = await connection.runAndReadAll(
      `SELECT draft_2_id
       FROM ${PEOPLE_DRAFT_STEP_2_TABLE} AS people
       WHERE source_count > 1
         AND ($all_companies OR company_id = $company_id)
         AND ($all_bolagsverket_sources OR len(bolagsverket_source_ids) > 0)
         AND ($all_esef_sources OR len(esef_source_ids) > 0)
         AND ($all_wikidata_sources OR len(wikidata_source_ids) > 0)
         ${selectedIdCondition}
       ORDER BY draft_2_id`,
      {
        all_companies: companyId === "",
        company_id: companyId,
        all_bolagsverket_sources:
          !selectedSourceSet.has("bolagsverket"),
        all_esef_sources: !selectedSourceSet.has("esef"),
        all_wikidata_sources: !selectedSourceSet.has("wikidata"),
      },
    );
    const rows = reader.getRowObjectsJson() as unknown as Array<{
      draft_2_id: string;
    }>;
    return rows.map((row) => row.draft_2_id);
  } finally {
    connection.closeSync();
  }
}

export async function getSwedenPeopleDraftTwoRow(
  draftTwoId: string,
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
): Promise<SwedenPeopleDraftTwoRow | null> {
  const cleanDraftTwoId = draftTwoId.trim();
  if (cleanDraftTwoId === "") return null;

  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  try {
    if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_2_TABLE))) {
      return null;
    }
    const reader = await connection.runAndReadAll(
      `SELECT * EXCLUDE (created_at)
       FROM ${PEOPLE_DRAFT_STEP_2_TABLE}
       WHERE draft_2_id = $draft_two_id
       LIMIT 1`,
      { draft_two_id: cleanDraftTwoId },
    );
    const [row] =
      reader.getRowObjectsJson() as unknown as SwedenPeopleDraftTwoRow[];
    if (!row) return null;
    await loadDraftTwoSourceObservations(connection, [row]);
    return row;
  } finally {
    connection.closeSync();
  }
}
