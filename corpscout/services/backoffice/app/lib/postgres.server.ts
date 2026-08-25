/**
 * The backoffice's connection to the review-queue PostgreSQL instance
 * (postgresqueue, 192.168.88.147 -- see ansible/README.md).
 *
 * Shaped after clickhouse.server.ts: one lazily created pool per process, read
 * from the environment, never constructed at import time so a machine without
 * `BACKOFFICE_POSTGRES_URL` can still build, typecheck and run the pages that
 * do not need it. Callers that can degrade gracefully ask
 * `isBackofficePostgresConfigured()` first; callers that cannot let
 * `BackofficePostgresNotConfiguredError` surface.
 *
 * Credentials never live in Git: `BACKOFFICE_POSTGRES_URL` carries the narrow
 * application role (`corpscout_backoffice_app`), and schema changes go through
 * `make migrate-up` with the separate owner URL.
 */
import "dotenv/config";
import { Pool, type PoolConfig, type QueryResultRow } from "pg";

let pool: Pool | undefined;

export class BackofficePostgresNotConfiguredError extends Error {
  constructor() {
    super(
      "BACKOFFICE_POSTGRES_URL is not set. Point it at the review-queue PostgreSQL instance (see .env.example).",
    );
    this.name = "BackofficePostgresNotConfiguredError";
  }
}

function connectionString(): string {
  return process.env.BACKOFFICE_POSTGRES_URL?.trim() ?? "";
}

/** Whether this process can talk to the review-queue database at all. */
export function isBackofficePostgresConfigured(): boolean {
  return connectionString() !== "";
}

function poolConfig(): PoolConfig {
  return {
    connectionString: connectionString(),
    application_name: "corpscout-backoffice",
    // The backoffice is a handful of concurrent reviewers, not a fleet; the
    // server allows 100 connections in total and Dagster shares them.
    max: Number(process.env.BACKOFFICE_POSTGRES_POOL_MAX ?? 5),
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
    // A backoffice page must never hold a review-queue lock open: a stuck
    // statement fails the request instead of the pool.
    statement_timeout: 30_000,
  };
}

export function getBackofficePostgresPool(): Pool {
  if (!isBackofficePostgresConfigured()) {
    throw new BackofficePostgresNotConfiguredError();
  }
  if (!pool) {
    pool = new Pool(poolConfig());
    // An idle client killed server-side (restart, admin drop) must not take
    // the process down with an unhandled 'error' event.
    pool.on("error", (error) => {
      console.error("backoffice postgres pool error", error);
    });
  }
  return pool;
}

/** Runs one parameterised statement and returns its rows. */
export async function pgQuery<T extends QueryResultRow>(
  sql: string,
  params: unknown[] = [],
): Promise<T[]> {
  const result = await getBackofficePostgresPool().query<T>(sql, params);
  return result.rows;
}

/** Runs one parameterised statement and returns its first row, if any. */
export async function pgQueryOne<T extends QueryResultRow>(
  sql: string,
  params: unknown[] = [],
): Promise<T | null> {
  const rows = await pgQuery<T>(sql, params);
  return rows[0] ?? null;
}

/** Closes the pool. Only long-lived scripts and tests need this. */
export async function closeBackofficePostgresPool(): Promise<void> {
  const current = pool;
  pool = undefined;
  await current?.end();
}
