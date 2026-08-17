from typing import Any


SEARCH_DOCUMENT_INPUT_COLUMNS = (
    "index_scope",
    "document_id",
    "country_code",
    "raw_address",
    "search_text",
    "street_name",
    "house_number",
    "unit",
    "postal_code",
    "locality",
    "address_kind",
    "reference_precision",
    "latitude",
    "longitude",
    "coordinate_spread_meters",
    "supporting_record_count",
    "source_record_id",
    "source_record_url",
)

SEARCH_DOCUMENT_COLUMNS = SEARCH_DOCUMENT_INPUT_COLUMNS + (
    "normalized_raw_address",
    "normalized_search_text",
    "normalized_street",
    "normalized_house_number",
    "normalized_unit",
    "normalized_postal_code",
    "normalized_locality",
    "raw_tokens",
    "street_tokens",
    "raw_trigrams",
    "street_deletion_signatures",
    "search_document_key",
)


def replace_address_search_document_input_table(
    connection: Any,
    *,
    table_name: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {table_name} (
            index_scope varchar,
            document_id varchar,
            country_code varchar,
            raw_address varchar,
            search_text varchar,
            street_name varchar,
            house_number varchar,
            unit varchar,
            postal_code varchar,
            locality varchar,
            address_kind varchar,
            reference_precision varchar,
            latitude double,
            longitude double,
            coordinate_spread_meters double,
            supporting_record_count uinteger,
            source_record_id varchar,
            source_record_url varchar
        )
        """
    )


def replace_address_search_documents(
    connection: Any,
    *,
    source_sql: str,
    table_name: str,
) -> None:
    """Materialize normalized raw and parsed fields in one search document."""
    normalized_raw_address = _normalized_text_sql("raw_address")
    normalized_search_text = _normalized_text_sql("search_text")
    normalized_street_text = _normalized_text_sql("street_name")
    normalized_street = _compact_text_sql("street_name")
    normalized_house_number = _compact_text_sql("house_number")
    normalized_unit = _compact_text_sql("unit")
    normalized_postal_code = _compact_text_sql("postal_code")
    normalized_locality = _compact_text_sql("locality")

    connection.execute(
        f"""
        create or replace table {table_name} as
        with source as (
            {source_sql}
        ), normalized as (
            select
                index_scope,
                document_id,
                upper(trim(coalesce(country_code, ''))) as country_code,
                coalesce(raw_address, '') as raw_address,
                coalesce(search_text, '') as search_text,
                coalesce(street_name, '') as street_name,
                coalesce(house_number, '') as house_number,
                coalesce(unit, '') as unit,
                coalesce(postal_code, '') as postal_code,
                coalesce(locality, '') as locality,
                coalesce(address_kind, '') as address_kind,
                coalesce(reference_precision, '') as reference_precision,
                latitude::double as latitude,
                longitude::double as longitude,
                coordinate_spread_meters::double as coordinate_spread_meters,
                supporting_record_count::uinteger as supporting_record_count,
                coalesce(source_record_id, '') as source_record_id,
                coalesce(source_record_url, '') as source_record_url,
                {normalized_raw_address} as normalized_raw_address,
                {normalized_search_text} as normalized_search_text,
                {normalized_street_text} as normalized_street_text,
                {normalized_street} as normalized_street,
                {normalized_house_number} as normalized_house_number,
                {normalized_unit} as normalized_unit,
                {normalized_postal_code} as normalized_postal_code,
                {normalized_locality} as normalized_locality
            from source
        ), indexed as (
            select
                * exclude (normalized_street_text),
                list_filter(
                    string_split(normalized_raw_address, ' '),
                    token -> token != ''
                ) as raw_tokens,
                list_filter(
                    string_split(normalized_street_text, ' '),
                    token -> token != ''
                ) as street_tokens,
                {_character_trigrams_sql("normalized_raw_address")}
                    as raw_trigrams,
                {_deletion_signatures_sql("normalized_street")}
                    as street_deletion_signatures
            from normalized
        )
        select
            *,
            md5(concat_ws(
                '|',
                index_scope,
                country_code,
                normalized_street,
                normalized_house_number,
                normalized_postal_code,
                normalized_locality,
                reference_precision
            )) as search_document_key
        from indexed
        """
    )


def _normalized_text_sql(expression: str) -> str:
    return f"""
trim(regexp_replace(
    strip_accents(lower(nfc_normalize(trim(coalesce({expression}, ''))))),
    '[^[:alnum:]]+',
    ' ',
    'g'
))
""".strip()


def _compact_text_sql(expression: str) -> str:
    return f"""
regexp_replace(
    strip_accents(lower(nfc_normalize(trim(coalesce({expression}, ''))))),
    '[^[:alnum:]]+',
    '',
    'g'
)
""".strip()


def _character_trigrams_sql(expression: str) -> str:
    return f"""
case
    when length({expression}) = 0 then []::varchar[]
    when length({expression}) < 3 then [{expression}]
    else list_sort(list_distinct(list_transform(
        range(1, length({expression}) - 1),
        position -> substr({expression}, position, 3)
    )))
end
""".strip()


def _deletion_signatures_sql(expression: str) -> str:
    return f"""
case
    when length({expression}) = 0 then []::varchar[]
    else list_sort(list_distinct(list_transform(
        range(1, length({expression}) + 1),
        position -> concat(
            substr({expression}, 1, position - 1),
            substr({expression}, position + 1)
        )
    )))
end
""".strip()
