import re
from collections.abc import Callable, Mapping
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from dagster_v3.defs.france_decp_procurement import tables

_NON_DIGITS = re.compile(r"\D+")
_EMPTY_MARKERS = frozenset({"", "CDL", "NC", "NON COMMUNIQUE", "NON COMMUNIQUÉ"})


def normalize_decp_identifier(value: str, identifier_type: str) -> str:
    raw = value.strip()
    if raw.upper() in _EMPTY_MARKERS:
        return ""
    digits = _NON_DIGITS.sub("", raw)
    normalized_type = identifier_type.strip().upper()
    if len(digits) == 14:
        return digits[:9]
    if len(digits) == 9:
        return digits
    if len(digits) == 11 and (
        normalized_type in {"TVA", "TVA INTRACOMMUNAUTAIRE", "VAT"}
        or raw.upper().startswith("FR")
    ):
        return digits[-9:]
    return ""


def expand_contract_holders(
    record: Mapping[str, Any],
    *,
    source_run_id: str,
    source_object_key: str,
    source_retrieved_at: datetime,
    resolved_at: datetime,
) -> list[dict[str, Any]]:
    contract_id = _text(record.get("id"))
    buyer_id_raw = _text(record.get("acheteur_id"))
    rows: list[dict[str, Any]] = []
    for ordinal in range(1, 4):
        holder_id_raw = _text(record.get(f"titulaire_id_{ordinal}"))
        holder_id_type = _text(record.get(f"titulaire_typeidentifiant_{ordinal}"))
        if holder_id_raw.upper() in _EMPTY_MARKERS:
            continue
        holder_siren = normalize_decp_identifier(holder_id_raw, holder_id_type)
        source_record_id = sha256(
            f"{buyer_id_raw}|{contract_id}|{ordinal}|{holder_id_raw}".encode()
        ).hexdigest()
        rows.append(
            {
                "source_slug": tables.SOURCE_SLUG,
                "source_run_id": source_run_id,
                "source_record_id": source_record_id,
                "contract_id": contract_id,
                "holder_ordinal": ordinal,
                "holder_id_raw": holder_id_raw,
                "holder_id_type": holder_id_type,
                "holder_siren": holder_siren,
                "buyer_id_raw": buyer_id_raw,
                "buyer_siren": normalize_decp_identifier(buyer_id_raw, "SIRET"),
                "notification_date": _date(record.get("datenotification")),
                "publication_date": _date(record.get("datepublicationdonnees")),
                "title": _text(record.get("objet")),
                "nature": _text(record.get("nature")),
                "procedure": _text(record.get("procedure")),
                "cpv_code": _text(record.get("codecpv")),
                "duration_months": _integer(record.get("dureemois")),
                "contract_amount_eur": _amount(record.get("montant")),
                "contract_amount_usd": None,
                "contract_amount_attributable": 0,
                "price_form": _text(record.get("formeprix")),
                "offers_received": _integer(record.get("offresrecues")),
                "framework_id": _meaningful(record.get("idaccordcadre")),
                "modification_id": _meaningful(record.get("idmodification")),
                "modification_amount_eur": _amount(record.get("montantmodification")),
                "modification_notification_date": _date(
                    record.get("datenotificationmodificationmodification")
                ),
                "subcontract_id": _meaningful(record.get("idactesoustraitance")),
                "subcontract_amount_eur": _amount(
                    record.get("montantactesoustraitance")
                ),
                "subcontractor_id_raw": _meaningful(record.get("idsoustraitant")),
                "source_system": _text(record.get("source")),
                "source_url": tables.CATALOG_URL,
                "source_object_key": source_object_key,
                "source_retrieved_at": source_retrieved_at,
                "resolved_at": resolved_at,
                "match_eligibility": (
                    "eligible" if holder_siren else "invalid_holder_identifier"
                ),
            }
        )
    return rows


def replace_raw_table(
    *,
    connection: Any,
    csv_path: Path,
    source_run_id: str,
    source_object_key: str,
    source_retrieved_at: datetime,
) -> int:
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {tables.DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE} AS
        SELECT
            CAST(? AS VARCHAR) AS source_run_id,
            row_number() OVER ()::UBIGINT AS source_line_number,
            CAST(? AS VARCHAR) AS source_object_key,
            CAST(? AS TIMESTAMP) AS source_retrieved_at,
            *
        FROM read_csv(
            ?,
            delim=';',
            header=true,
            all_varchar=true,
            strict_mode=false,
            quote='"',
            escape='"',
            encoding='utf-8'
        )
        """,
        [
            source_run_id,
            source_object_key,
            source_retrieved_at,
            str(csv_path),
        ],
    )
    columns = {
        str(row[0])
        for row in connection.execute(
            f"DESCRIBE {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE}"
        ).fetchall()
    }
    missing = [
        column for column in tables.EXPECTED_SOURCE_COLUMNS if column not in columns
    ]
    if missing:
        raise ValueError(f"DECP CSV is missing columns: {', '.join(missing)}")
    row_count = _count(connection, tables.RAW_TABLE)
    if row_count == 0:
        raise ValueError("DECP CSV produced zero raw contracts")
    return row_count


def build_contract_holder_candidates(
    *,
    connection: Any,
    source_run_id: str,
    resolved_at: datetime,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    raw = f"{tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE}"
    target = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    holder_digits = "regexp_replace(trim(holder_id_raw), '[^0-9]', '', 'g')"
    buyer_digits = "regexp_replace(trim(acheteur_id), '[^0-9]', '', 'g')"
    source_version_rows = int(
        connection.execute(
            f"""
            SELECT coalesce(sum(
                CASE WHEN upper(coalesce(trim(titulaire_id_1), '')) NOT IN
                    ('', 'CDL', 'NC', 'NON COMMUNIQUE', 'NON COMMUNIQUÉ')
                    THEN 1 ELSE 0 END
                + CASE WHEN upper(coalesce(trim(titulaire_id_2), '')) NOT IN
                    ('', 'CDL', 'NC', 'NON COMMUNIQUE', 'NON COMMUNIQUÉ')
                    THEN 1 ELSE 0 END
                + CASE WHEN upper(coalesce(trim(titulaire_id_3), '')) NOT IN
                    ('', 'CDL', 'NC', 'NON COMMUNIQUE', 'NON COMMUNIQUÉ')
                    THEN 1 ELSE 0 END
            ), 0)
            FROM {raw}
            """
        ).fetchone()[0]
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {target} AS
        WITH expanded AS (
            SELECT
                r.*,
                h.holder_ordinal,
                coalesce(trim(h.holder_id_raw), '') AS holder_id_raw,
                coalesce(trim(h.holder_id_type), '') AS holder_id_type
            FROM {raw} AS r
            CROSS JOIN LATERAL (
                VALUES
                    (1, titulaire_id_1, titulaire_typeidentifiant_1),
                    (2, titulaire_id_2, titulaire_typeidentifiant_2),
                    (3, titulaire_id_3, titulaire_typeidentifiant_3)
            ) AS h(holder_ordinal, holder_id_raw, holder_id_type)
            WHERE upper(coalesce(trim(h.holder_id_raw), '')) NOT IN
                ('', 'CDL', 'NC', 'NON COMMUNIQUE', 'NON COMMUNIQUÉ')
        ),
        normalized AS (
            SELECT
                *,
                CASE
                    WHEN length({holder_digits}) = 14
                        THEN substr({holder_digits}, 1, 9)
                    WHEN length({holder_digits}) = 9
                        THEN {holder_digits}
                    WHEN length({holder_digits}) = 11
                         AND (
                            upper(holder_id_type) IN
                                ('TVA', 'TVA INTRACOMMUNAUTAIRE', 'VAT')
                            OR upper(holder_id_raw) LIKE 'FR%'
                         )
                        THEN right({holder_digits}, 9)
                    ELSE ''
                END AS holder_siren,
                CASE
                    WHEN length({buyer_digits}) = 14
                        THEN substr({buyer_digits}, 1, 9)
                    WHEN length({buyer_digits}) = 9
                        THEN {buyer_digits}
                    ELSE ''
                END AS buyer_siren
            FROM expanded
        ),
        candidates AS (
            SELECT
                '{tables.SOURCE_SLUG}' AS source_slug,
                CAST(? AS VARCHAR) AS source_run_id,
                lower(sha256(concat_ws(
                    '|',
                    coalesce(acheteur_id, ''),
                    coalesce(id, ''),
                    cast(holder_ordinal AS VARCHAR),
                    holder_id_raw
                ))) AS source_record_id,
                coalesce(id, '') AS contract_id,
                holder_ordinal::INTEGER AS holder_ordinal,
                holder_id_raw,
                holder_id_type,
                holder_siren,
                coalesce(acheteur_id, '') AS buyer_id_raw,
                buyer_siren,
                try_cast(nullif(trim(datenotification), '') AS DATE)
                    AS notification_date,
                try_cast(nullif(trim(datepublicationdonnees), '') AS DATE)
                    AS publication_date,
                coalesce(objet, '') AS title,
                coalesce(nature, '') AS nature,
                coalesce(procedure, '') AS procedure,
                coalesce(codecpv, '') AS cpv_code,
                try_cast(nullif(trim(dureemois), '') AS INTEGER) AS duration_months,
                try_cast(nullif(trim(montant), '') AS DECIMAL(38, 2))
                    AS contract_amount_eur,
                CAST(NULL AS DECIMAL(38, 2)) AS contract_amount_usd,
                0::TINYINT AS contract_amount_attributable,
                coalesce(formeprix, '') AS price_form,
                try_cast(nullif(trim(offresrecues), '') AS INTEGER)
                    AS offers_received,
                CASE
                    WHEN upper(coalesce(trim(idaccordcadre), '')) IN
                        ('', 'CDL', 'NC')
                        THEN ''
                    ELSE trim(idaccordcadre)
                END AS framework_id,
                CASE
                    WHEN upper(coalesce(trim(idmodification), '')) IN
                        ('', 'CDL', 'NC')
                        THEN ''
                    ELSE trim(idmodification)
                END AS modification_id,
                try_cast(
                    nullif(trim(montantmodification), '') AS DECIMAL(38, 2)
                ) AS modification_amount_eur,
                try_cast(
                    nullif(
                        trim(datenotificationmodificationmodification),
                        ''
                    ) AS DATE
                ) AS modification_notification_date,
                CASE
                    WHEN upper(coalesce(trim(idactesoustraitance), '')) IN
                        ('', 'CDL', 'NC')
                        THEN ''
                    ELSE trim(idactesoustraitance)
                END AS subcontract_id,
                try_cast(
                    nullif(trim(montantactesoustraitance), '') AS DECIMAL(38, 2)
                ) AS subcontract_amount_eur,
                CASE
                    WHEN upper(coalesce(trim(idsoustraitant), '')) IN
                        ('', 'CDL', 'NC')
                        THEN ''
                    ELSE trim(idsoustraitant)
                END AS subcontractor_id_raw,
                coalesce(source, '') AS source_system,
                '{tables.CATALOG_URL}' AS source_url,
                source_object_key,
                source_retrieved_at,
                CAST(? AS TIMESTAMP) AS resolved_at,
                CASE
                    WHEN holder_siren = '' THEN 'invalid_holder_identifier'
                    ELSE 'eligible'
                END AS match_eligibility,
                greatest(
                    try_cast(nullif(trim(datepublicationdonnees), '') AS DATE),
                    try_cast(
                        nullif(
                            trim(datepublicationdonneesmodificationmodification),
                            ''
                        ) AS DATE
                    ),
                    try_cast(
                        nullif(
                            trim(datepublicationdonneesactesoustraitance),
                            ''
                        ) AS DATE
                    ),
                    try_cast(
                        nullif(
                            trim(
                                datepublicationdonneesmodificationactesoustraitance
                            ),
                            ''
                        ) AS DATE
                    )
                ) AS _version_publication_date,
                greatest(
                    try_cast(nullif(trim(datenotification), '') AS DATE),
                    try_cast(
                        nullif(
                            trim(datenotificationmodificationmodification),
                            ''
                        ) AS DATE
                    ),
                    try_cast(
                        nullif(trim(datenotificationactesoustraitance), '') AS DATE
                    ),
                    try_cast(
                        nullif(
                            trim(
                                datenotificationmodificationsoustraitancemodificationactesoustraitance
                            ),
                            ''
                        ) AS DATE
                    )
                ) AS _version_notification_date,
                source_line_number AS _source_line_number
            FROM normalized
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY source_record_id
                    ORDER BY
                        _version_publication_date DESC NULLS LAST,
                        _version_notification_date DESC NULLS LAST,
                        _source_line_number DESC
                ) AS _version_rank
            FROM candidates
        )
        SELECT * EXCLUDE (
            _version_publication_date,
            _version_notification_date,
            _source_line_number,
            _version_rank
        )
        FROM ranked
        WHERE _version_rank = 1
        """,
        [source_run_id, resolved_at],
    )
    candidate_rows = _count(connection, tables.CANDIDATES_TABLE)
    counts = {
        "source_version_rows": source_version_rows,
        "candidate_rows": candidate_rows,
        "collapsed_version_rows": source_version_rows - candidate_rows,
        "eligible_rows": _count_where(
            connection, tables.CANDIDATES_TABLE, "match_eligibility = 'eligible'"
        ),
        "contracts": int(
            connection.execute(
                f"SELECT count(DISTINCT contract_id) FROM {target}"
            ).fetchone()[0]
        ),
    }
    if log is not None:
        log("Built DECP contract-holder candidates: %s", counts)
    return counts


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _meaningful(value: Any) -> str:
    text = _text(value)
    if text.upper() in _EMPTY_MARKERS:
        return ""
    return text


def _date(value: Any) -> date | None:
    text = _meaningful(value)
    if text == "":
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _integer(value: Any) -> int | None:
    text = _meaningful(value)
    if text == "":
        return None
    try:
        return int(float(text.replace(",", ".")))
    except ValueError:
        return None


def _amount(value: Any) -> str | None:
    text = _meaningful(value)
    if text == "":
        return None
    normalized = text.replace(" ", "").replace(",", ".")
    try:
        float(normalized)
    except ValueError:
        return None
    return normalized


def _count(connection: Any, table: str) -> int:
    return int(
        connection.execute(
            f"SELECT count(*) FROM {tables.DUCKDB_SCHEMA}.{table}"
        ).fetchone()[0]
    )


def _count_where(connection: Any, table: str, condition: str) -> int:
    return int(
        connection.execute(
            f"SELECT count(*) FROM {tables.DUCKDB_SCHEMA}.{table} WHERE {condition}"
        ).fetchone()[0]
    )
