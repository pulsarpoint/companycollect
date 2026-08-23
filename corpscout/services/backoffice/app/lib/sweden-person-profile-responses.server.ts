import { createHash, randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type {
  PersonProfileGenerationMetadata,
  PersonProfileSuggestion,
} from "~/lib/person-profile-llm.server";

export const SWEDEN_PEOPLE_CURATION_DATABASE_PATH =
  process.env.SWEDEN_PEOPLE_CURATION_DATABASE_PATH?.trim() ||
  join(process.cwd(), "data", "sweden", "people-curation.sqlite");

export interface StoredPersonProfileResponse {
  attemptId: string;
  draftTwoId: string;
  inputHash: string;
  suggestion: PersonProfileSuggestion;
  generation: PersonProfileGenerationMetadata;
  reviewStatus: "pending" | "approved" | "rejected";
  attemptCount: number;
  createdAt: string;
}

interface StoredPersonProfileResponseRow {
  attempt_id: string;
  draft_2_id: string;
  input_hash: string;
  validated_response_json: string;
  provider: string;
  model: string;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  total_tokens: number | null;
  review_status: StoredPersonProfileResponse["reviewStatus"];
  attempt_count: number;
  created_at: string;
}

export function connectCurationDatabase(databasePath: string): DatabaseSync {
  const absolutePath = resolve(databasePath);
  mkdirSync(dirname(absolutePath), { recursive: true });
  const database = new DatabaseSync(absolutePath);
  database.exec("PRAGMA busy_timeout = 5000");
  database.exec("PRAGMA journal_mode = WAL");
  database.exec(`
    CREATE TABLE IF NOT EXISTS person_profile_llm_response (
      attempt_id TEXT PRIMARY KEY,
      draft_2_id TEXT NOT NULL CHECK (trim(draft_2_id) != ''),
      input_hash TEXT NOT NULL CHECK (length(input_hash) = 64),
      input_json TEXT NOT NULL CHECK (json_valid(input_json)),
      raw_response_json TEXT NOT NULL,
      validated_response_json TEXT NOT NULL CHECK (
        json_valid(validated_response_json)
      ),
      provider TEXT NOT NULL CHECK (trim(provider) != ''),
      model TEXT NOT NULL CHECK (trim(model) != ''),
      prompt_tokens INTEGER,
      completion_tokens INTEGER,
      total_tokens INTEGER,
      review_status TEXT NOT NULL DEFAULT 'pending' CHECK (
        review_status IN ('pending', 'approved', 'rejected')
      ),
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS person_profile_llm_response_candidate
      ON person_profile_llm_response(draft_2_id, input_hash, created_at DESC);
  `);
  return database;
}

function serializedInput(input: unknown): string {
  const serialized = JSON.stringify(input);
  if (serialized === undefined) {
    throw new TypeError("The person-profile LLM input must be JSON serializable.");
  }
  return serialized;
}

export function personProfileInputHash(input: unknown): string {
  return createHash("sha256").update(serializedInput(input)).digest("hex");
}

function mapStoredResponse(
  row: StoredPersonProfileResponseRow,
): StoredPersonProfileResponse {
  return {
    attemptId: row.attempt_id,
    draftTwoId: row.draft_2_id,
    inputHash: row.input_hash,
    suggestion: JSON.parse(
      row.validated_response_json,
    ) as PersonProfileSuggestion,
    generation: {
      provider: row.provider,
      model: row.model,
      promptTokens: row.prompt_tokens,
      completionTokens: row.completion_tokens,
      totalTokens: row.total_tokens,
    },
    reviewStatus: row.review_status,
    attemptCount: Number(row.attempt_count),
    createdAt: row.created_at,
  };
}

export function getLatestPersonProfileResponse(
  {
    draftTwoId,
    input,
  }: {
    draftTwoId: string;
    input?: unknown;
  },
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): StoredPersonProfileResponse | null {
  const inputHash = input === undefined ? null : personProfileInputHash(input);
  const database = connectCurationDatabase(databasePath);
  try {
    const inputCondition = inputHash === null ? "" : "AND response.input_hash = ?";
    const parameters = inputHash === null ? [draftTwoId] : [draftTwoId, inputHash];
    const row = database
      .prepare(
        `SELECT response.*,
                (
                  SELECT count(*)
                  FROM person_profile_llm_response AS attempts
                  WHERE attempts.draft_2_id = response.draft_2_id
                    AND attempts.input_hash = response.input_hash
                ) AS attempt_count
         FROM person_profile_llm_response AS response
         WHERE response.draft_2_id = ?
           ${inputCondition}
         ORDER BY response.created_at DESC, response.rowid DESC
         LIMIT 1`,
      )
      .get(...parameters) as unknown as
      | StoredPersonProfileResponseRow
      | undefined;
    return row ? mapStoredResponse(row) : null;
  } finally {
    database.close();
  }
}

export function listPersonProfileResponseDraftTwoIds(
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): string[] {
  const database = connectCurationDatabase(databasePath);
  try {
    const rows = database
      .prepare(
        `SELECT DISTINCT draft_2_id
         FROM person_profile_llm_response
         ORDER BY draft_2_id`,
      )
      .all() as unknown as Array<{ draft_2_id: string }>;
    return rows.map((row) => row.draft_2_id);
  } finally {
    database.close();
  }
}

export function findPersonProfileResponseDraftTwoIds(
  draftTwoIds: string[],
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): Set<string> {
  const uniqueIds = [...new Set(draftTwoIds.filter((value) => value !== ""))];
  if (uniqueIds.length === 0) return new Set();
  const placeholders = uniqueIds.map(() => "?").join(", ");
  const database = connectCurationDatabase(databasePath);
  try {
    const rows = database
      .prepare(
        `SELECT DISTINCT draft_2_id
         FROM person_profile_llm_response
         WHERE draft_2_id IN (${placeholders})`,
      )
      .all(...uniqueIds) as unknown as Array<{ draft_2_id: string }>;
    return new Set(rows.map((row) => row.draft_2_id));
  } finally {
    database.close();
  }
}

export function savePersonProfileResponse(
  {
    draftTwoId,
    input,
    rawResponse,
    suggestion,
    generation,
  }: {
    draftTwoId: string;
    input: unknown;
    rawResponse: string;
    suggestion: PersonProfileSuggestion;
    generation: PersonProfileGenerationMetadata;
  },
  databasePath = SWEDEN_PEOPLE_CURATION_DATABASE_PATH,
): StoredPersonProfileResponse {
  const inputJson = serializedInput(input);
  const inputHash = createHash("sha256").update(inputJson).digest("hex");
  const attemptId = randomUUID();
  const createdAt = new Date().toISOString();
  const database = connectCurationDatabase(databasePath);
  try {
    database
      .prepare(
        `INSERT INTO person_profile_llm_response (
          attempt_id,
          draft_2_id,
          input_hash,
          input_json,
          raw_response_json,
          validated_response_json,
          provider,
          model,
          prompt_tokens,
          completion_tokens,
          total_tokens,
          review_status,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
      )
      .run(
        attemptId,
        draftTwoId,
        inputHash,
        inputJson,
        rawResponse,
        JSON.stringify(suggestion),
        generation.provider,
        generation.model,
        generation.promptTokens,
        generation.completionTokens,
        generation.totalTokens,
        createdAt,
      );
  } finally {
    database.close();
  }
  const stored = getLatestPersonProfileResponse(
    { draftTwoId, input },
    databasePath,
  );
  if (!stored) {
    throw new Error("The saved person-profile response could not be read back.");
  }
  return stored;
}
