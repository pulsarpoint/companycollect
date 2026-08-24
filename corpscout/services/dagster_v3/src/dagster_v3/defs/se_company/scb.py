"""Swedish company artifacts from the SCB/Bolagsverket company register.

Input (source layer): sweden_company_companies_clickhouse → corpscout.se_companies
(one row per company, rebuilt weekly from the register bulk files),
corpscout.se_industries (SNI/NACE codes per company, is_primary flag) and
corpscout.text_translations (the translator service's English rendering of the
Swedish activity description, enqueued by sweden_company_translation_load) and
corpscout.se_code_labels (the curated legal-form dictionary seeded by
se_code_labels_clickhouse, English and official Swedish).
This module writes one artifact table per datatype with the standard envelope
followed by the register's own typed columns.

Assets
  se_company_info_scb_clickhouse → corpscout.se_company_info_scb
    legal name, legal form code with its curated English and Swedish labels,
    status, incorporation/dissolution, activity description (Bolagsverket
    verksamhetsbeskrivning) with its English translation, and the primary
    SNI/NACE code; a new observation is written
    only when evidence_hash changes, and the latest one per (company, source
    record) survives merges.
  se_company_address_scb_clickhouse → corpscout.se_company_address_scb
    the address SCB holds for each company (visiting or postal -- the
    register does not distinguish), read from the same
    se_company_addresses_current snapshot as the Bolagsverket artifact, one
    source value apart; a new version is written only when evidence_hash
    changes.
Downstream: info.py (legal_name is authoritative from here); address.py
(field precedence bolagsverket > scb).
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import SE_COMPANY_ID_PATTERN, publish_with_stage

GROUP_NAME = "se_company_scb"
DATABASE = "corpscout"
TABLE = "se_company_info_scb"
# Positional insert list: the envelope (evidence_hash is MATERIALIZED, so omitted) then this
# module's payload, in the order the migration declares them — pinned by the test.
# observed_at = when this artifact observed the version; the register's bulk-load stamp is
# constant and cannot order versions (se_companies.updated_from_raw_at is one value for the
# whole weekly load, older than every se_company_info.resolved_at, so a version appended
# under it would never look newer than the row it replaces and the change scan would never
# select the company again).
SE_COMPANY_INFO_SCB_COLUMNS = (
    "company_id", "source_record_uid", "observed_at", "source_run_id",
    "legal_name", "legal_name_raw", "legal_form_code", "legal_form_label_en",
    "legal_form_label_sv", "status", "incorporation_date",
    "dissolution_date", "activity_description", "activity_description_en",
    "primary_sni_code", "primary_nace_code",
)

# New versions only: publish_with_stage stages these candidates, then keeps only
# rows whose (company_id, source_record_uid, evidence_hash) is not already in the
# target -- the target's MATERIALIZED evidence_hash expression computes the hash
# on the stage, so it is never re-expressed here.
#
# industries is pre-aggregated to one row per company_id *before* joining to
# companies -- se_industries fans out per company (several SNI/NACE rows), so
# joining it straight into candidates would force a GROUP BY over every
# companies column (including the free-text activity_description) just to
# undo that fan-out, at 2.2M-company scale. Aggregating first means the join
# to companies is a plain 1:1 LEFT JOIN with no outer GROUP BY at all. The
# argMaxIf tie-break is a tuple (updated_from_raw_at, code) rather than just
# updated_from_raw_at, so two industry rows with the exact same timestamp
# resolve deterministically instead of the pick flipping (and evidence_hash
# with it) between runs.
#
# legal_form_label_en / legal_form_label_sv are the curated dictionary's names for
# companies.legal_form_code, read exactly the way corpscout.se_companies_translated reads
# them: one argMax(version) per code from corpscout.se_code_labels, joined on the raw code.
# The code column mixes two registers' systems (Bolagsverket -ORGFO text codes and SCB
# juridisk-form numbers), so the code alone is unreadable and the label is what every
# surface shows -- copied from the register like the description, never model-written.
# Both languages travel together (000306 hashes them into evidence_hash as v3), so a label
# CORRECTION re-seeded into se_code_labels is appended as a new artifact version by the
# anti-join, the same path a late translation takes. A code with no dictionary row reads as
# '' under either join_use_nulls setting, exactly like an untranslated description.
#
# activity_description_en is the translator service's English rendering of the
# Swedish verksamhetsbeskrivning, read from corpscout.text_translations exactly
# the way corpscout.se_companies_translated reads it: one explicit language pair
# (grouping by source_text_hash alone would let a second target language replace
# the English text), newest by version, keyed by cityHash64 of the source text.
# The translator runs outside Dagster, so a description translated after this run
# simply changes evidence_hash on the next one and is appended as a new version --
# stamped with that run's now64, hence strictly newer than the version it replaces
# and visible to info.py's change scan. Rows the anti-join skips are never rewritten,
# so an unchanged company keeps its original stamp instead of looking new every week.
SE_COMPANY_INFO_SCB_SQL = """WITH industries AS (
    SELECT
        industries.company_id AS company_id,
        ifNull(argMaxIf(industries.sni_code, (industries.updated_from_raw_at, industries.sni_code), industries.is_primary = 1), '') AS primary_sni_code,
        ifNull(argMaxIf(industries.nace_rev2_class_code, (industries.updated_from_raw_at, industries.nace_rev2_class_code), industries.is_primary = 1), '') AS primary_nace_code
    FROM corpscout.se_industries AS industries
    GROUP BY industries.company_id
),
candidates AS (
    SELECT
        companies.company_id AS company_id,
        ifNull(nullIf(companies.scb_source_record_uid, ''), companies.bolagsverket_source_record_uid) AS source_record_uid,
        now64(3, 'UTC') AS observed_at,
        %(source_run_id)s AS source_run_id,
        companies.legal_name AS legal_name,
        companies.legal_name_raw AS legal_name_raw,
        companies.legal_form_code AS legal_form_code,
        ifNull(lf.label_en, '') AS legal_form_label_en,
        ifNull(lf.label_sv, '') AS legal_form_label_sv,
        toString(companies.status) AS status,
        companies.incorporation_date AS incorporation_date,
        companies.dissolution_date AS dissolution_date,
        companies.activity_description AS activity_description,
        ifNull(act.translated_text, '') AS activity_description_en,
        ifNull(industries.primary_sni_code, '') AS primary_sni_code,
        ifNull(industries.primary_nace_code, '') AS primary_nace_code
    FROM corpscout.se_companies AS companies FINAL
    LEFT JOIN industries ON industries.company_id = companies.company_id
    LEFT JOIN (
        SELECT source_text_hash, argMax(translated_text, version) AS translated_text
        FROM corpscout.text_translations
        WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description'
          AND source_lang = 'sv' AND target_lang = 'en'
        GROUP BY source_text_hash
    ) AS act ON act.source_text_hash = cityHash64(ifNull(companies.activity_description, ''))
    LEFT JOIN (
        SELECT code, argMax(label_en, version) AS label_en, argMax(label_sv, version) AS label_sv
        FROM corpscout.se_code_labels
        WHERE code_type = 'legal_form'
        GROUP BY code
    ) AS lf ON lf.code = ifNull(companies.legal_form_code, '')
    WHERE match(companies.company_id, '{SE_COMPANY_ID_PATTERN}')
)
SELECT
    company_id AS company_id, source_record_uid AS source_record_uid, observed_at AS observed_at, source_run_id AS source_run_id,
    legal_name AS legal_name, legal_name_raw AS legal_name_raw, legal_form_code AS legal_form_code,
    legal_form_label_en AS legal_form_label_en, legal_form_label_sv AS legal_form_label_sv, status AS status,
    incorporation_date AS incorporation_date, dissolution_date AS dissolution_date, activity_description AS activity_description,
    activity_description_en AS activity_description_en, primary_sni_code AS primary_sni_code, primary_nace_code AS primary_nace_code
FROM candidates
WHERE source_record_uid != ''""".replace("{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN)


@dg.asset(
    name="se_company_info_scb_clickhouse",
    deps=[
        dg.AssetKey("sweden_company_companies_clickhouse"),
        dg.AssetKey("sweden_company_industries_clickhouse"),
        dg.AssetKey("sweden_company_translation_load"),
        dg.AssetKey("se_code_labels_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{TABLE}"},
    description=(
        "Register facts per Swedish company (legal name, form code with its curated "
        "English and Swedish labels, status, dates, activity description in Swedish and "
        "the translator's English, primary SNI/NACE); a new "
        "observation is written only when the evidence hash changes and the latest per "
        "(company, source record) survives merges."
    ),
)
def se_company_info_scb_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Select from se_companies (+ primary industry, + English description, + legal-form labels) → stage → validate → append."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=DATABASE,
        tables=("se_companies", "se_industries", "text_translations", "se_code_labels", TABLE),
    )
    counts = publish_with_stage(
        clickhouse=clickhouse,
        target=TABLE,
        insert_columns=SE_COMPANY_INFO_SCB_COLUMNS,
        select_sql=SE_COMPANY_INFO_SCB_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition="trim(company_id) = '' OR trim(source_record_uid) = ''",
        new_versions_only=True,
    )
    context.log.info("se_company_info_scb: appended=%s total=%s", counts.inserted, counts.total)
    return dg.MaterializeResult(
        metadata={"appended_count": counts.inserted, "total_count": counts.total,
                  "table": f"{DATABASE}.{TABLE}", "resolved_at": datetime.now(UTC).isoformat()}
    )


ADDRESS_TABLE = "se_company_address_scb"
ADDRESS_SOURCE = "scb"
# The one address_type normalized_duckdb.py's scb_addresses CTE ever emits (a
# hard-coded literal, address_rank = 1 -- one row per company). Pinned in the WHERE
# below and re-checked by the tripwire so a change to that upstream assumption fails
# loudly here instead of silently losing rows to ReplacingMergeTree collapsing two
# same-keyed versions (see the SQL comment and the tripwire below).
ADDRESS_TYPE = "visiting_or_postal"
ADDRESS_SOURCE_TABLE = "se_company_addresses_current"
SE_COMPANY_ADDRESS_SCB_COLUMNS = (
    "company_id", "source_record_uid", "observed_at", "source_run_id",
    "address_type", "address_fingerprint", "care_of", "street_address",
    "normalized_address", "postal_code", "city", "country_code",
)

# Same shape as bolagsverket.py's SELECT, one source value apart -- deliberately NOT
# factored into a shared builder: each artifact module owns its own table name, its own
# insert list and its own SELECT, and a shared generator would make a payload change in
# one source silently rewrite the other's evidence.
#
# SCB records a single address per company and does not say whether it is the visiting or
# the postal one, hence the register's own 'visiting_or_postal' type, which travels
# unchanged: the address_type is part of address_key, so renaming it here would silently
# re-key every SCB address.
#
# SCB also marks foreign addresses with the placeholders PostOrt='Utlandet' and
# PostNr='00000'. Those are the normalizer's business (migration 000265 already drops both
# from normalized_address), so they arrive here as ordinary text and are neither
# special-cased nor repaired.
#
# address_type = '{ADDRESS_TYPE}' pins the one type this source has ever emitted (see
# ADDRESS_TYPE above). The artifact's ORDER BY (company_id, source_record_uid) is only a
# unique key because of the one-row-per-company invariant this filter enforces; without
# it, a second address_type slipping into the source table would not error here -- it
# would just let ReplacingMergeTree collapse the extra row at stage-write time (same
# ORDER BY key, two versions), so the staged count would silently come out lower than
# the source actually holds. The tripwire below is the independent check for exactly
# that.
SE_COMPANY_ADDRESS_SCB_SQL = """WITH candidates AS (
    SELECT
        addresses.company_id AS company_id,
        addresses.source_record_uid AS source_record_uid,
        now64(3, 'UTC') AS observed_at,
        %(source_run_id)s AS source_run_id,
        toString(addresses.address_type) AS address_type,
        toString(addresses.address_fingerprint) AS address_fingerprint,
        addresses.care_of AS care_of,
        addresses.street_address AS street_address,
        nullIf(addresses.normalized_address, '') AS normalized_address,
        addresses.postal_code AS postal_code,
        addresses.post_town AS city,
        CAST(addresses.country_code AS Nullable(String)) AS country_code
    FROM corpscout.se_company_addresses_current AS addresses
    WHERE addresses.source = '{ADDRESS_SOURCE}'
      AND addresses.address_type = '{ADDRESS_TYPE}'
      AND addresses.has_address = 1
      AND match(addresses.company_id, '{SE_COMPANY_ID_PATTERN}')
)
SELECT
    company_id AS company_id, source_record_uid AS source_record_uid,
    observed_at AS observed_at, source_run_id AS source_run_id,
    address_type AS address_type, address_fingerprint AS address_fingerprint,
    care_of AS care_of, street_address AS street_address,
    normalized_address AS normalized_address, postal_code AS postal_code,
    city AS city, country_code AS country_code
FROM candidates
WHERE source_record_uid != ''""".replace(
    "{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN
).replace("{ADDRESS_SOURCE}", ADDRESS_SOURCE).replace("{ADDRESS_TYPE}", ADDRESS_TYPE)

# Tripwire (I2): recomputes the candidates CTE's own filters directly against the source
# table, independently of anything publish_with_stage's stage table may already have
# collapsed. Compared against PublishCounts.staged (the row count observed on the stage
# right after the SELECT above ran) once the asset's publish call returns; a mismatch
# means the source held a different number of matching rows than made it into the stage
# -- exactly the silent-collapse scenario the reviewer reproduced -- and is worth
# failing the run over rather than logging and moving on.
#
# Deliberately NOT filtered on address_type: that pin is the invariant this tripwire
# measures, so repeating it here would make the count agree with the pinned SELECT by
# construction -- blind to a second address_type appearing under the same
# source_record_uid (ReplacingMergeTree collapses it either way, both counts would read
# the same lower number) and blind to the literal itself being renamed upstream (both
# counts would read zero). The tripwire counts every row this source actually holds;
# the pinned SELECT counts only the one type it expects. Under today's guaranteed
# one-type-per-source invariant the two agree, so no false positives -- a second type or
# a renamed type makes them disagree.
SE_COMPANY_ADDRESS_SCB_SOURCE_COUNT_SQL = """SELECT count()
FROM corpscout.se_company_addresses_current AS addresses
WHERE addresses.source = %(source)s
  AND addresses.has_address = 1
  AND match(addresses.company_id, '{SE_COMPANY_ID_PATTERN}')
  AND addresses.source_record_uid != ''""".replace(
    "{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN
)


@dg.asset(
    name="se_company_address_scb_clickhouse",
    deps=[dg.AssetKey("sweden_company_addresses_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{ADDRESS_TABLE}"},
    description=(
        "The address SCB holds for each Swedish company (visiting or postal -- the register "
        "does not distinguish), as an append-only artifact; a new version is written only "
        "when the evidence hash changes."
    ),
)
def se_company_address_scb_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Select the SCB rows of the address snapshot -> stage -> validate -> append."""
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(ADDRESS_SOURCE_TABLE, ADDRESS_TABLE)
    )
    counts = publish_with_stage(
        clickhouse=clickhouse,
        target=ADDRESS_TABLE,
        insert_columns=SE_COMPANY_ADDRESS_SCB_COLUMNS,
        select_sql=SE_COMPANY_ADDRESS_SCB_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition=(
            "trim(company_id) = '' OR trim(source_record_uid) = '' OR trim(address_type) = ''"
        ),
        new_versions_only=True,
    )
    with clickhouse.get_connection() as client:
        source_count = int(
            client.execute(
                SE_COMPANY_ADDRESS_SCB_SOURCE_COUNT_SQL,
                {"source": ADDRESS_SOURCE},
            )[0][0]
        )
    context.log.info(
        "se_company_address_scb: staged=%s source_count=%s", counts.staged, source_count
    )
    if source_count != counts.staged:
        raise ValueError(
            f"se_company_address_scb: staged count {counts.staged} does not match "
            f"source count {source_count} for source={ADDRESS_SOURCE!r} "
            f"address_type={ADDRESS_TYPE!r} -- the source pipeline may be emitting more "
            "than one address row per company for this source, which "
            "ReplacingMergeTree's ORDER BY (company_id, source_record_uid) would "
            "silently collapse -- or the pinned address_type no longer matches "
            "what the source emits."
        )
    context.log.info("se_company_address_scb: appended=%s total=%s", counts.inserted, counts.total)
    return dg.MaterializeResult(
        metadata={"appended_count": counts.inserted, "total_count": counts.total,
                  "table": f"{DATABASE}.{ADDRESS_TABLE}", "resolved_at": datetime.now(UTC).isoformat()}
    )


defs = dg.Definitions(assets=[se_company_info_scb_clickhouse, se_company_address_scb_clickhouse])
