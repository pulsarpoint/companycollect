/**
 * The "Simple sync" preview on `/admin/se/people`: what
 * `se_company_person_clickhouse`'s CLEAN COPY path (dagster_v3's
 * `company_people/normalization.py`) would process on the next unscoped run
 * -- companies backed by EXACTLY ONE source (bolagsverket/esef/wikidata)
 * whose evidence has changed since the last publish.
 *
 * This is a PORT of normalization.py's `_company_status_ctes` +
 * `build_pending_companies_sql`'s `source_count = 1` branch (the same
 * `pending_direct_company_count` normalization.py's own
 * `build_company_statistics_sql` reports), not a reimplementation of
 * different logic -- see that module's `company_status` CTE and
 * `CompanyPersonStatus`/`is_unchanged` for the source of truth this mirrors,
 * and `source_views.py`'s `build_se_company_person_source_observations_sql`
 * for the `draft_id` formula `is_unchanged` depends on. Differences from the
 * Python original, both deliberate:
 *
 * 1. UNSCOPED (no `company_ids` parameter): the Confirm button always
 *    launches `buildCleanCopyRunConfig({ companyIds: [] })` (every company,
 *    `se-company-person-pipeline.ts`'s `normalizeCompanyIdScope([]) === []`
 *    convention) exactly like the Pipeline page's own clean-copy launch, so
 *    the preview must describe that same unscoped run -- there is no
 *    per-company filter here to thread through. Mirrors
 *    `se-company-person-pipeline.server.ts`'s own unscoped stats queries
 *    (`PUBLISHED_PERSON_COUNT_SQL` et al.).
 * 2. `any(source) AS source` is added to `draft_companies`: correct only
 *    because the `source_count = 1` filter guarantees every row in a kept
 *    group shares one source -- `any()` would be arbitrary otherwise, but
 *    normalization.py never needed a per-company source label so it never
 *    had a reason to expose one.
 * 3. `source_value_json`/`fiscal_year` are dropped from the observation read
 *    (the preview never renders LLM request evidence); `draft_id` itself,
 *    `person_profile_hash`, `person_role_hash` and the three disambiguator
 *    columns are KEPT -- `is_unchanged` cannot be answered without the exact
 *    same `draft_id`s normalization.py would compute and compare against
 *    `se_company_person.draft_ids`.
 *
 * PARITY: keep this file's `SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL` in sync by
 * hand with normalization.py's `_company_status_ctes`/`build_pending_companies_sql`
 * (`source_count = 1` branch), corrections.py's `effective_company_corrections_cte`
 * and source_views.py's draft_id formula -- there is no shared source of
 * truth across the Python/TypeScript boundary, same as every other
 * hand-ported query in this directory (se-company-person-pipeline.server.ts's
 * module docstring).
 */
import { chQuery } from "~/lib/clickhouse.server";

const DATABASE = "corpscout";

/** `draft_id`, per source_views.py's `_SOURCE_OBSERVATION_ID_SQL`:
 * `SHA256('se-company-person-source-observation-v2\n{company_id}\n{source}\n
 * {source_record_uid}\n{person_profile_hash}\n{person_role_hash}\n
 * {disambiguator}')`, reinterpreted as a UUID. `is_unchanged` below compares
 * a SET of these against `se_company_person.draft_ids`, so an inexact port
 * here would silently disagree with what the asset actually published. */
function draftIdSql(sourceLiteral: string, disambiguatorColumn: string): string {
  return `reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
        'se-company-person-source-observation-v2\\n',
        company_id, '\\n', ${sourceLiteral}, '\\n', toString(source_record_uid), '\\n',
        toString(person_profile_hash), '\\n', toString(person_role_hash), '\\n',
        toString(${disambiguatorColumn})
    ))), 1, 32)))`;
}

/** Bolagsverket/ESEF/Wikidata source observation rows -- a TypeScript port of
 * source_views.py's `build_se_company_person_source_observations_sql`,
 * narrowed to the columns this preview needs: `draft_id` (for
 * `is_unchanged`), `full_name` (for the sample) and `source`/`company_id`. */
const SOURCE_OBSERVATIONS_CTE_SQL = `source_observations AS (
    SELECT
        'bolagsverket' AS source,
        company_id,
        full_name,
        ${draftIdSql("'bolagsverket'", "signatory_uid")} AS draft_id
    FROM ${DATABASE}.se_company_person_bolagsverket
    WHERE trim(full_name) != ''

    UNION ALL

    SELECT
        'esef' AS source,
        company_id,
        full_name,
        ${draftIdSql("'esef'", "candidate_uid")} AS draft_id
    FROM ${DATABASE}.se_company_person_esef
    WHERE trim(full_name) != ''

    UNION ALL

    SELECT
        'wikidata' AS source,
        company_id,
        full_name,
        ${draftIdSql("'wikidata'", "company_wikidata_id")} AS draft_id
    FROM ${DATABASE}.se_company_person_wikidata
    WHERE trim(full_name) != ''
)`;

/** Person-level correction kinds normalization.py's `apply_person_corrections`
 * actually applies (corrections.py's `PERSON_CORRECTION_KINDS`) -- role kinds
 * and `keep_separate`/`undo` are excluded because they never change what
 * `se_company_person_clickhouse` would write for a company. Hand-mirrored,
 * not imported: there is no shared module across the Python/TS boundary. */
const PERSON_CORRECTION_KINDS_SQL =
  "'merge_persons', 'reassign_draft', 'split_person', 'approve_suggestion', 'reject_suggestion', 'override_field'";

/**
 * The company-status WITH clause, unscoped: every company's source-observed
 * evidence (`draft_companies`, with `source` added), what is already
 * published (`published_companies`), the live correction ledger
 * (`effective_company_corrections`), and `company_status`'s `is_unchanged`
 * comparison -- a straight port of normalization.py's `_company_status_ctes`.
 * `pending_single_source_companies` is this preview's own addition: the
 * `NOT is_unchanged AND source_count = 1` filter that
 * `build_pending_companies_sql`/`build_company_statistics_sql`'s
 * `pending_direct_company_count` apply, materialized as its own CTE so both
 * the stats query and the sample query below can share it.
 */
export const SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL = `WITH ${SOURCE_OBSERVATIONS_CTE_SQL},
draft_companies AS (
    SELECT
        company_id,
        any(source) AS source,
        uniqExact(source) AS source_count,
        count() AS observation_count,
        arraySort(groupUniqArray(draft_id)) AS draft_ids
    FROM source_observations
    GROUP BY company_id
),
published_companies AS (
    SELECT
        company_id,
        arraySort(arrayDistinct(arrayFlatten(groupArray(draft_ids)))) AS draft_ids,
        arraySort(arrayDistinct(arrayFlatten(groupArray(
            arrayMap(id -> toString(id), correction_ids)
        )))) AS correction_ids
    FROM ${DATABASE}.se_company_person FINAL
    GROUP BY company_id
),
effective_company_corrections AS (
    SELECT
        company_id,
        arraySort(groupArrayIf(
            toString(correction_id),
            correction_kind IN (${PERSON_CORRECTION_KINDS_SQL}) AND NOT superseded
        )) AS correction_ids
    FROM (
        SELECT
            ledger.company_id,
            ledger.correction_id,
            ledger.correction_kind,
            ledger.correction_id IN (
                SELECT supersedes_correction_id
                FROM ${DATABASE}.se_company_person_correction
                WHERE supersedes_correction_id IS NOT NULL
            ) AS superseded
        FROM ${DATABASE}.se_company_person_correction AS ledger
    )
    GROUP BY company_id
),
company_status AS (
    SELECT
        drafts.company_id AS company_id,
        drafts.source AS source,
        drafts.source_count AS source_count,
        drafts.observation_count AS observation_count,
        published.company_id != ''
            AND published.draft_ids = drafts.draft_ids
            AND published.correction_ids = corrections.correction_ids AS is_unchanged
    FROM draft_companies AS drafts
    LEFT JOIN published_companies AS published USING (company_id)
    LEFT JOIN effective_company_corrections AS corrections USING (company_id)
),
pending_single_source_companies AS (
    SELECT company_id, source, observation_count
    FROM company_status
    WHERE NOT is_unchanged AND source_count = 1
)`;

/** Total companies/people this run would process, plus the same two counts
 * split per source -- the sheet's headline numbers and per-source
 * breakdown. */
export const SIMPLE_SYNC_STATS_SQL = `${SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL}
SELECT
    toString(count()) AS company_count,
    toString(sum(observation_count)) AS person_count,
    toString(countIf(source = 'bolagsverket')) AS bolagsverket_company_count,
    toString(sumIf(observation_count, source = 'bolagsverket')) AS bolagsverket_person_count,
    toString(countIf(source = 'esef')) AS esef_company_count,
    toString(sumIf(observation_count, source = 'esef')) AS esef_person_count,
    toString(countIf(source = 'wikidata')) AS wikidata_company_count,
    toString(sumIf(observation_count, source = 'wikidata')) AS wikidata_person_count
FROM pending_single_source_companies`;

/** The first `{sampleSize}` people, by company_id/source/name -- a SAMPLE
 * only (see this module's docstring: never the full list, which can run to
 * ~1M rows on a first run). Reads `source_observations` directly rather than
 * `pending_single_source_companies` alone, because the preview needs each
 * person's own `full_name`, which the aggregated status CTE does not carry. */
export const SIMPLE_SYNC_SAMPLE_SQL = `${SIMPLE_SYNC_COMPANY_STATUS_CTE_SQL}
SELECT
    full_name AS name,
    source_observations.company_id AS company_id,
    source_observations.source AS source
FROM source_observations
INNER JOIN pending_single_source_companies USING (company_id)
ORDER BY company_id, source, full_name
LIMIT {sampleSize:UInt32}`;

export const SIMPLE_SYNC_SAMPLE_SIZE = 20;

export interface SimpleSyncSourceBreakdown {
  source: "bolagsverket" | "esef" | "wikidata";
  companyCount: number;
  personCount: number;
}

export interface SimpleSyncSamplePerson {
  name: string;
  companyId: string;
  source: string;
}

export interface SimpleSyncPreview {
  companyCount: number;
  personCount: number;
  bySource: SimpleSyncSourceBreakdown[];
  sample: SimpleSyncSamplePerson[];
  sampleSize: number;
}

interface SimpleSyncStatsRow {
  company_count: string;
  person_count: string;
  bolagsverket_company_count: string;
  bolagsverket_person_count: string;
  esef_company_count: string;
  esef_person_count: string;
  wikidata_company_count: string;
  wikidata_person_count: string;
}

interface SimpleSyncSampleRow {
  name: string;
  company_id: string;
  source: string;
}

function n(value: string | undefined): number {
  return Number(value ?? 0);
}

/**
 * Loads the whole Simple Sync sheet in one call: the headline counts, the
 * per-source breakdown and a bounded sample -- two ClickHouse round trips
 * (the stats query and the sample query each re-run the CTE fresh; ClickHouse
 * has no cross-query CTE cache, and this table is small enough that this is
 * affordable per sheet OPEN, matching se-company-person-pipeline.server.ts's
 * "small enough" reasoning for its own stats).
 */
export async function loadSimpleSyncPreview(): Promise<SimpleSyncPreview> {
  const [[stats], sample] = await Promise.all([
    chQuery<SimpleSyncStatsRow>(SIMPLE_SYNC_STATS_SQL),
    chQuery<SimpleSyncSampleRow>(SIMPLE_SYNC_SAMPLE_SQL, {
      sampleSize: SIMPLE_SYNC_SAMPLE_SIZE,
    }),
  ]);
  return {
    companyCount: n(stats?.company_count),
    personCount: n(stats?.person_count),
    bySource: [
      {
        source: "bolagsverket",
        companyCount: n(stats?.bolagsverket_company_count),
        personCount: n(stats?.bolagsverket_person_count),
      },
      {
        source: "esef",
        companyCount: n(stats?.esef_company_count),
        personCount: n(stats?.esef_person_count),
      },
      {
        source: "wikidata",
        companyCount: n(stats?.wikidata_company_count),
        personCount: n(stats?.wikidata_person_count),
      },
    ],
    sample: sample.map((row) => ({
      name: row.name,
      companyId: row.company_id,
      source: row.source,
    })),
    sampleSize: SIMPLE_SYNC_SAMPLE_SIZE,
  };
}
