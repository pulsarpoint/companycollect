import { createHash, randomUUID } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import type { DatabaseSync } from "node:sqlite";
import {
  DuckDBAppender,
  DuckDBConnection,
  DuckDBInstance,
} from "@duckdb/node-api";
import { WorkflowIdReusePolicy } from "@temporalio/common";
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
  getSwedenPeopleDraftSourceCount,
  streamSwedenPeopleDraftSource,
  SWEDEN_PEOPLE_DRAFT_SOURCES,
  type SwedenPeopleDraftSource,
  type SwedenPeopleDraftSourceObservation,
} from "~/lib/sweden-people-draft-sources.server";

export const PEOPLE_DRAFT_STEP_1_TABLE = "people_draft_step_1";
const PEOPLE_DRAFT_STEP_1_BUILD_TABLE = "people_draft_step_1_build";
const PEOPLE_DRAFT_STEP_1_JOB_TABLE =
  "people_draft_step_1_initialization_job";
const PEOPLE_DRAFT_STEP_1_JOB_TYPE = "draft_1_rebuild" as const;
const PROGRESS_BATCH_SIZE = 50_000;

const migrationGlobal = globalThis as typeof globalThis & {
  __swedenPeopleDraftOneJobMigrations?: Map<string, Promise<void>>;
};
const jobMigrations =
  migrationGlobal.__swedenPeopleDraftOneJobMigrations ?? new Map();
migrationGlobal.__swedenPeopleDraftOneJobMigrations = jobMigrations;

export const SWEDEN_PEOPLE_DRAFT_DATABASE_PATH =
  process.env.SWEDEN_PEOPLE_DRAFT_DATABASE_PATH?.trim() ||
  join(process.cwd(), "data", "sweden", "people-draft.duckdb");

export interface SwedenPeopleDraftStatus {
  tableExists: boolean;
  rowCount: number;
}

export type SwedenPeopleDraftInitializationStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed";

export type SwedenPeopleDraftInitializationPhase =
  | "queued"
  | "counting"
  | "importing"
  | "publishing"
  | "completed"
  | "failed";

export interface SwedenPeopleDraftInitializationJob {
  jobId: string;
  workflowId: string;
  status: SwedenPeopleDraftInitializationStatus;
  phase: SwedenPeopleDraftInitializationPhase;
  currentSource: SwedenPeopleDraftSource | null;
  processedRows: number;
  totalRows: number;
  insertedRows: number;
  progressPercent: number;
  message: string;
  errorMessage: string;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
  updatedAt: string;
}

interface StoredInitializationJob {
  job_id: string;
  status: SwedenPeopleDraftInitializationStatus;
  phase: SwedenPeopleDraftInitializationPhase;
  current_source: SwedenPeopleDraftSource | null;
  processed_rows: number | string | bigint;
  total_rows: number | string | bigint;
  inserted_rows: number | string | bigint;
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

export class SwedenPeopleDraftTableExistsError extends Error {
  constructor() {
    super(
      `${PEOPLE_DRAFT_STEP_1_TABLE} already exists; explicit confirmation is required to reinitialize it.`,
    );
    this.name = "SwedenPeopleDraftTableExistsError";
  }
}

export class SwedenPeopleDraftReplacementConfirmationError extends Error {
  readonly rowCount: number;

  constructor(rowCount: number) {
    super(
      `${PEOPLE_DRAFT_STEP_1_TABLE} contains ${rowCount} rows; explicit confirmation is required to replace them.`,
    );
    this.name = "SwedenPeopleDraftReplacementConfirmationError";
    this.rowCount = rowCount;
  }
}

const DRAFT_COLUMNS_SQL = `
  observation_id VARCHAR NOT NULL,
  company_id VARCHAR NOT NULL CHECK (trim(company_id) != ''),
  name VARCHAR NOT NULL CHECK (trim(name) != ''),
  source VARCHAR NOT NULL CHECK (
    source IN ('bolagsverket', 'esef', 'wikidata')
  ),
  source_entity_id VARCHAR NOT NULL CHECK (trim(source_entity_id) != ''),
  source_record_uid VARCHAR NOT NULL CHECK (trim(source_record_uid) != ''),
  role_original VARCHAR,
  fiscal_year INTEGER CHECK (
    fiscal_year IS NULL OR fiscal_year BETWEEN 1800 AND 9999
  ),
  description VARCHAR,
  source_profile_hash VARCHAR NOT NULL CHECK (
    trim(source_profile_hash) != ''
  ),
  source_role_hash VARCHAR NOT NULL CHECK (trim(source_role_hash) != ''),
  source_payload_json VARCHAR,
  source_observed_at VARCHAR,
  imported_at VARCHAR NOT NULL`;

export async function connectSwedenPeopleDraftDatabase(
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
): Promise<DuckDBConnection> {
  const absolutePath = resolve(databasePath);
  mkdirSync(dirname(absolutePath), { recursive: true });
  const instance = await DuckDBInstance.fromCache(absolutePath);
  return instance.connect();
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

async function createDraftTable(
  connection: DuckDBConnection,
  tableName: string,
): Promise<void> {
  if (
    tableName !== PEOPLE_DRAFT_STEP_1_TABLE &&
    tableName !== PEOPLE_DRAFT_STEP_1_BUILD_TABLE
  ) {
    throw new Error("Unknown Sweden people draft table name.");
  }
  await connection.run(`CREATE TABLE ${tableName} (${DRAFT_COLUMNS_SQL})`);
}

function mapStoredJob(
  row: StoredInitializationJob,
): SwedenPeopleDraftInitializationJob {
  return {
    jobId: row.job_id,
    workflowId: `backoffice-se-people-draft-1-${row.job_id}`,
    status: row.status,
    phase: row.phase,
    currentSource: row.current_source,
    processedRows: Number(row.processed_rows),
    totalRows: Number(row.total_rows),
    insertedRows: Number(row.inserted_rows),
    progressPercent: Number(row.progress_percent),
    message: row.message,
    errorMessage: row.error_message,
    createdAt: row.created_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    updatedAt: row.updated_at,
  };
}

async function migrateLegacyDraftOneJobs(
  connection: DuckDBConnection,
  jobDatabase: DatabaseSync,
): Promise<void> {
  if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_1_JOB_TABLE))) {
    return;
  }
  const reader = await connection.runAndReadAll(
    `SELECT *
     FROM ${PEOPLE_DRAFT_STEP_1_JOB_TABLE}
     ORDER BY created_at`,
  );
  const rows =
    reader.getRowObjectsJson() as unknown as StoredInitializationJob[];
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
          message: "Draft 1 initialization was interrupted",
          errorMessage:
            "The previous in-process background worker stopped before the source import completed.",
          completedAt: interruptedAt,
          updatedAt: interruptedAt,
        };
      }
      importLegacySwedenPeopleProcessingJob(
        jobDatabase,
        PEOPLE_DRAFT_STEP_1_JOB_TYPE,
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
      `DROP TABLE IF EXISTS ${PEOPLE_DRAFT_STEP_1_BUILD_TABLE}`,
    );
  }
  await connection.run(`DROP TABLE ${PEOPLE_DRAFT_STEP_1_JOB_TABLE}`);
}

async function ensureLegacyDraftOneJobsMigrated(
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
        await migrateLegacyDraftOneJobs(connection, jobDatabase);
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

function sourceLabel(source: SwedenPeopleDraftSource): string {
  if (source === "bolagsverket") return "Bolagsverket";
  if (source === "esef") return "ESEF";
  return "Wikidata";
}

function observationId(
  observation: SwedenPeopleDraftSourceObservation,
): string {
  return createHash("sha256")
    .update(
      [
        "se-company-person-source-observation-v1",
        observation.company_id,
        observation.source,
        observation.source_entity_id,
        observation.source_profile_hash,
        observation.source_role_hash,
      ].join("\n"),
    )
    .digest("hex");
}

function appendNullableVarchar(
  appender: DuckDBAppender,
  value: string | null,
): void {
  if (value === null) {
    appender.appendNull();
    return;
  }
  appender.appendVarchar(value);
}

function appendObservation(
  appender: DuckDBAppender,
  observation: SwedenPeopleDraftSourceObservation,
  importedAt: string,
): void {
  appender.appendVarchar(observationId(observation));
  appender.appendVarchar(observation.company_id);
  appender.appendVarchar(observation.name);
  appender.appendVarchar(observation.source);
  appender.appendVarchar(observation.source_entity_id);
  appender.appendVarchar(observation.source_record_uid);
  appendNullableVarchar(appender, observation.role_original);
  if (observation.fiscal_year === null) {
    appender.appendNull();
  } else {
    appender.appendInteger(observation.fiscal_year);
  }
  appendNullableVarchar(appender, observation.description);
  appender.appendVarchar(observation.source_profile_hash);
  appender.appendVarchar(observation.source_role_hash);
  appender.appendVarchar(observation.source_payload_json);
  appendNullableVarchar(appender, observation.source_observed_at);
  appender.appendVarchar(importedAt);
  appender.endRow();
}

export async function executeSwedenPeopleDraftInitialization(
  jobId: string,
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
  curationDatabasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
  reportProgress: (job: SwedenPeopleDraftInitializationJob) => void = () => {},
): Promise<SwedenPeopleDraftInitializationJob | null> {
  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  const jobDatabase = connectCurationDatabase(curationDatabasePath);
  ensureSwedenPeopleProcessingJobTable(jobDatabase);
  let appender: DuckDBAppender | null = null;
  let job = readSwedenPeopleProcessingJob<SwedenPeopleDraftInitializationJob>(
    jobDatabase,
    jobId,
  );
  if (!job) {
    connection.closeSync();
    jobDatabase.close();
    return null;
  }

  const startedAt = new Date().toISOString();
  job = {
    ...job,
    status: "running",
    phase: "counting",
    progressPercent: 1,
    message: "Counting source observations",
    errorMessage: "",
    startedAt,
    completedAt: null,
    updatedAt: startedAt,
  };
  persistSwedenPeopleProcessingJob(jobDatabase, job);
  reportProgress(job);

  try {
    const sourceCounts = new Map<SwedenPeopleDraftSource, number>();
    for (const source of SWEDEN_PEOPLE_DRAFT_SOURCES) {
      job = {
        ...job,
        currentSource: source,
        message: `Counting ${sourceLabel(source)} observations`,
        updatedAt: new Date().toISOString(),
      };
      persistSwedenPeopleProcessingJob(jobDatabase, job);
      reportProgress(job);
      sourceCounts.set(
        source,
        await getSwedenPeopleDraftSourceCount(source),
      );
    }

    const totalRows = [...sourceCounts.values()].reduce(
      (total, count) => total + count,
      0,
    );
    await connection.run(
      `DROP TABLE IF EXISTS ${PEOPLE_DRAFT_STEP_1_BUILD_TABLE}`,
    );
    await createDraftTable(connection, PEOPLE_DRAFT_STEP_1_BUILD_TABLE);

    job = {
      ...job,
      phase: "importing",
      processedRows: 0,
      totalRows,
      insertedRows: 0,
      progressPercent: 5,
      message: "Starting source import",
      updatedAt: new Date().toISOString(),
    };
    persistSwedenPeopleProcessingJob(jobDatabase, job);
    reportProgress(job);

    appender = await connection.createAppender(
      PEOPLE_DRAFT_STEP_1_BUILD_TABLE,
    );
    let unflushedRows = 0;
    for (const source of SWEDEN_PEOPLE_DRAFT_SOURCES) {
      job = {
        ...job,
        currentSource: source,
        message: `Importing ${sourceLabel(source)} observations`,
        updatedAt: new Date().toISOString(),
      };
      persistSwedenPeopleProcessingJob(jobDatabase, job);
      reportProgress(job);

      for await (const observation of streamSwedenPeopleDraftSource(source)) {
        appendObservation(appender, observation, startedAt);
        unflushedRows += 1;
        if (unflushedRows < PROGRESS_BATCH_SIZE) continue;

        appender.flushSync();
        const processedRows: number = job.processedRows + unflushedRows;
        job = {
          ...job,
          processedRows,
          insertedRows: processedRows,
          progressPercent:
            totalRows > 0
              ? Math.min(95, 5 + Math.floor((processedRows / totalRows) * 90))
              : 95,
          updatedAt: new Date().toISOString(),
        };
        unflushedRows = 0;
        persistSwedenPeopleProcessingJob(jobDatabase, job);
        reportProgress(job);
      }
    }

    if (unflushedRows > 0) {
      appender.flushSync();
      const processedRows: number = job.processedRows + unflushedRows;
      job = {
        ...job,
        processedRows,
        insertedRows: processedRows,
        progressPercent:
          totalRows > 0
            ? Math.min(95, 5 + Math.floor((processedRows / totalRows) * 90))
            : 95,
        updatedAt: new Date().toISOString(),
      };
      persistSwedenPeopleProcessingJob(jobDatabase, job);
      reportProgress(job);
    }
    appender.closeSync();
    appender = null;

    const validationReader = await connection.runAndReadAll(
      `SELECT
        count(*) AS row_count,
        count(DISTINCT observation_id) AS distinct_row_count
       FROM ${PEOPLE_DRAFT_STEP_1_BUILD_TABLE}`,
    );
    const [validation] = validationReader.getRowObjectsJson() as unknown as Array<{
      row_count: number | string | bigint;
      distinct_row_count: number | string | bigint;
    }>;
    if (
      Number(validation?.row_count ?? 0) !==
      Number(validation?.distinct_row_count ?? 0)
    ) {
      throw new Error("Draft 1 source observations contain duplicate identities.");
    }

    job = {
      ...job,
      phase: "publishing",
      currentSource: null,
      progressPercent: 98,
      message: "Publishing Draft 1 atomically",
      updatedAt: new Date().toISOString(),
    };
    persistSwedenPeopleProcessingJob(jobDatabase, job);
    reportProgress(job);

    const completedAt = new Date().toISOString();
    const completedJob: SwedenPeopleDraftInitializationJob = {
      ...job,
      status: "completed",
      phase: "completed",
      progressPercent: 100,
      message: "Draft 1 initialization completed",
      completedAt,
      updatedAt: completedAt,
    };
    await connection.run("BEGIN TRANSACTION");
    try {
      await connection.run(`DROP TABLE IF EXISTS ${PEOPLE_DRAFT_STEP_1_TABLE}`);
      await connection.run(
        `ALTER TABLE ${PEOPLE_DRAFT_STEP_1_BUILD_TABLE}
         RENAME TO ${PEOPLE_DRAFT_STEP_1_TABLE}`,
      );
      await connection.run("COMMIT");
      job = completedJob;
    } catch (error) {
      await connection.run("ROLLBACK");
      throw error;
    }
    persistSwedenPeopleProcessingJob(jobDatabase, completedJob);
    reportProgress(completedJob);

    try {
      await connection.run("CHECKPOINT");
    } catch (error) {
      console.warn("Sweden Draft 1 checkpoint failed", { jobId, error });
    }
  } catch (error) {
    const failedPhase = job.phase;
    try {
      appender?.closeSync();
    } catch {
      // The original import error is more useful than an appender close error.
    }
    appender = null;
    await connection.run(
      `DROP TABLE IF EXISTS ${PEOPLE_DRAFT_STEP_1_BUILD_TABLE}`,
    );
    const failedAt = new Date().toISOString();
    job = {
      ...job,
      status: "failed",
      phase: "failed",
      message: "Draft 1 initialization failed",
      errorMessage:
        "The source import failed. The previously published Draft 1 table was preserved.",
      completedAt: failedAt,
      updatedAt: failedAt,
    };
    persistSwedenPeopleProcessingJob(jobDatabase, job);
    reportProgress(job);
    console.error("Sweden Draft 1 initialization failed", {
      jobId,
      phase: failedPhase,
      source: job.currentSource,
      error,
    });
  } finally {
    connection.closeSync();
    jobDatabase.close();
  }
  return job;
}

export async function getSwedenPeopleDraftStatus(
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
): Promise<SwedenPeopleDraftStatus> {
  if (!existsSync(databasePath)) {
    return { tableExists: false, rowCount: 0 };
  }

  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  try {
    if (!(await tableExists(connection, PEOPLE_DRAFT_STEP_1_TABLE))) {
      return { tableExists: false, rowCount: 0 };
    }
    const reader = await connection.runAndReadAll(
      `SELECT count(*) AS row_count FROM ${PEOPLE_DRAFT_STEP_1_TABLE}`,
    );
    const [row] = reader.getRowObjectsJson() as unknown as CountRow[];
    return { tableExists: true, rowCount: Number(row?.row_count ?? 0) };
  } finally {
    connection.closeSync();
  }
}

export async function getLatestSwedenPeopleDraftInitializationJob(
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
  curationDatabasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): Promise<SwedenPeopleDraftInitializationJob | null> {
  await ensureLegacyDraftOneJobsMigrated(
    databasePath,
    curationDatabasePath,
  );
  const jobDatabase = connectCurationDatabase(curationDatabasePath);
  try {
    ensureSwedenPeopleProcessingJobTable(jobDatabase);
    return readLatestSwedenPeopleProcessingJob<SwedenPeopleDraftInitializationJob>(
      jobDatabase,
      PEOPLE_DRAFT_STEP_1_JOB_TYPE,
    );
  } finally {
    jobDatabase.close();
  }
}

export async function startSwedenPeopleDraftInitialization({
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
  curationDatabasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
  confirmReplacement = false,
}: {
  databasePath?: string;
  curationDatabasePath?: string;
  confirmReplacement?: boolean;
} = {}): Promise<SwedenPeopleDraftInitializationJob> {
  await ensureLegacyDraftOneJobsMigrated(
    databasePath,
    curationDatabasePath,
  );
  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  const jobDatabase = connectCurationDatabase(curationDatabasePath);
  let sqliteTransactionStarted = false;
  let job: SwedenPeopleDraftInitializationJob;
  try {
    ensureSwedenPeopleProcessingJobTable(jobDatabase);
    jobDatabase.exec("BEGIN IMMEDIATE");
    sqliteTransactionStarted = true;

    const activeJob =
      readActiveSwedenPeopleProcessingJob<SwedenPeopleDraftInitializationJob>(
        jobDatabase,
        PEOPLE_DRAFT_STEP_1_JOB_TYPE,
      );
    if (activeJob) {
      jobDatabase.exec("COMMIT");
      sqliteTransactionStarted = false;
      return activeJob;
    }

    let currentRowCount = 0;
    if (await tableExists(connection, PEOPLE_DRAFT_STEP_1_TABLE)) {
      const countReader = await connection.runAndReadAll(
        `SELECT count(*) AS row_count FROM ${PEOPLE_DRAFT_STEP_1_TABLE}`,
      );
      const [row] = countReader.getRowObjectsJson() as unknown as CountRow[];
      currentRowCount = Number(row?.row_count ?? 0);
    }
    if (currentRowCount > 0 && !confirmReplacement) {
      throw new SwedenPeopleDraftReplacementConfirmationError(currentRowCount);
    }

    const now = new Date().toISOString();
    const jobId = randomUUID();
    job = {
      jobId,
      workflowId: `backoffice-se-people-draft-1-${jobId}`,
      status: "queued",
      phase: "queued",
      currentSource: null,
      processedRows: 0,
      totalRows: 0,
      insertedRows: 0,
      progressPercent: 0,
      message: "Draft 1 initialization queued",
      errorMessage: "",
      createdAt: now,
      startedAt: null,
      completedAt: null,
      updatedAt: now,
    };
    insertSwedenPeopleProcessingJob(
      jobDatabase,
      PEOPLE_DRAFT_STEP_1_JOB_TYPE,
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
    await client.workflow.start("swedenPeopleDraftOneWorkflow", {
      workflowId: job.workflowId,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      taskQueue: swedenPeopleTemporalTaskQueue(),
      args: [{ jobId: job.jobId }],
      memo: {
        country: "SE",
        operation: "draft-1-rebuild",
      },
    });
  } catch (error) {
    const failedAt = new Date().toISOString();
    const failedJob: SwedenPeopleDraftInitializationJob = {
      ...job,
      status: "failed",
      phase: "failed",
      message: "Draft 1 initialization could not start",
      errorMessage: "Temporal could not start the Draft 1 rebuild workflow.",
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

export async function initializeSwedenPeopleDraft({
  databasePath = SWEDEN_PEOPLE_DRAFT_DATABASE_PATH,
  reinitialize = false,
}: {
  databasePath?: string;
  reinitialize?: boolean;
} = {}): Promise<SwedenPeopleDraftStatus> {
  const connection = await connectSwedenPeopleDraftDatabase(databasePath);
  let transactionStarted = false;
  try {
    const alreadyExists = await tableExists(
      connection,
      PEOPLE_DRAFT_STEP_1_TABLE,
    );
    if (alreadyExists && !reinitialize) {
      throw new SwedenPeopleDraftTableExistsError();
    }

    await connection.run("BEGIN TRANSACTION");
    transactionStarted = true;
    if (alreadyExists) {
      await connection.run(`DROP TABLE ${PEOPLE_DRAFT_STEP_1_TABLE}`);
    }
    await createDraftTable(connection, PEOPLE_DRAFT_STEP_1_TABLE);
    await connection.run("COMMIT");
    transactionStarted = false;
  } catch (error) {
    if (transactionStarted) await connection.run("ROLLBACK");
    throw error;
  } finally {
    connection.closeSync();
  }

  return getSwedenPeopleDraftStatus(databasePath);
}
