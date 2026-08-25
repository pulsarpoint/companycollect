from collections.abc import Mapping, Sequence
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

STREET_VARIANT_COLUMNS = (
    "document_id",
    "index_scope",
    "country_code",
    "street_variant",
    "normalized_street_variant",
    "variant_kind",
    "variant_rank",
    "street_deletion_signatures",
)

LIBPOSTAL_EXPANSION_VARIANT_KIND = "libpostal_expansion"
LIBPOSTAL_EXPANSION_VARIANT_RANK = 1
SUFFIX_EXPANSION_VARIANT_KIND = "suffix_expansion"
SUFFIX_EXPANSION_VARIANT_RANK = 2

# How many letters a glued abbreviation must follow before it is read as a suffix
# rather than as the whole street name: `Norra V` keeps its parsed form, `Ringv`
# earns a `Ringvägen` variant.
MINIMUM_GLUED_SUFFIX_STEM_LENGTH = 3


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


def expanded_street_suffix_variants(
    street_name: str,
    suffix_expansions: Mapping[str, str],
) -> tuple[str, ...]:
    """Expand a street suffix abbreviation glued to the last token's stem.

    Swedish registers truncate the suffix and glue what is left to the stem, so
    `STAVSTENSV 3` is Stavstensvägen 3 and `Sandgr 1` is Sandgränd 1. The longest
    configured abbreviation wins, so a map holding both `gr` and `g` reads `Sandgr`
    as a gränd. Returns the expanded street, or nothing when the last token carries
    no configured abbreviation or too short a stem to be one.
    """
    tokens = street_name.split()
    if not tokens:
        return ()
    last_token = tokens[-1]
    lowered = last_token.lower()
    for suffix in sorted(suffix_expansions, key=len, reverse=True):
        if not lowered.endswith(suffix):
            continue
        stem = last_token[: len(last_token) - len(suffix)]
        if len(stem) < MINIMUM_GLUED_SUFFIX_STEM_LENGTH:
            continue
        if not stem[-MINIMUM_GLUED_SUFFIX_STEM_LENGTH:].isalpha():
            continue
        expansion = suffix_expansions[suffix]
        abbreviation = last_token[len(stem) :]
        expanded_token = stem + (
            expansion.upper() if abbreviation.isupper() else expansion
        )
        return (" ".join([*tokens[:-1], expanded_token]),)
    return ()


def replace_address_street_variants(
    connection: Any,
    *,
    document_table: str,
    variant_table: str,
    languages_by_country: Mapping[str, Sequence[str]],
    suffix_expansions_by_country: Mapping[str, Mapping[str, str]],
) -> None:
    """Materialize parsed, libpostal-expanded and suffix-expanded street variants."""
    # Importing pypostal initializes libpostal's multi-gigabyte language model.
    # Keep it out of Dagster definition discovery and load it during materialization.
    import pyarrow as pa
    from postal.expand import ADDRESS_STREET, expand_address

    expansion_countries: list[str] = []
    expansion_streets: list[str] = []
    expanded_streets: list[str] = []
    expansion_kinds: list[str] = []
    expansion_ranks: list[int] = []

    def record(
        country_code: str,
        street_name: str,
        expanded_street: str,
        *,
        kind: str,
        rank: int,
    ) -> None:
        if expanded_street.strip() == "":
            return
        expansion_countries.append(country_code)
        expansion_streets.append(street_name)
        expanded_streets.append(expanded_street)
        expansion_kinds.append(kind)
        expansion_ranks.append(rank)

    street_rows = connection.execute(
        f"""
        select distinct country_code, street_name
        from {document_table}
        where street_name != ''
        order by country_code, street_name
        """
    ).fetchall()
    for row_country_code, row_street_name in street_rows:
        country_code = str(row_country_code)
        street_name = str(row_street_name)
        languages = languages_by_country.get(country_code)
        if languages:
            for expanded_street in expand_address(
                street_name,
                languages=list(languages),
                address_components=ADDRESS_STREET,
            ):
                record(
                    country_code,
                    street_name,
                    expanded_street,
                    kind=LIBPOSTAL_EXPANSION_VARIANT_KIND,
                    rank=LIBPOSTAL_EXPANSION_VARIANT_RANK,
                )
        suffix_expansions = suffix_expansions_by_country.get(country_code)
        if suffix_expansions:
            for expanded_street in expanded_street_suffix_variants(
                street_name,
                suffix_expansions,
            ):
                record(
                    country_code,
                    street_name,
                    expanded_street,
                    kind=SUFFIX_EXPANSION_VARIANT_KIND,
                    rank=SUFFIX_EXPANSION_VARIANT_RANK,
                )

    expansion_input = "_address_resolution_street_expansion_input"
    if expanded_streets:
        registered_rows = "_address_resolution_street_expansion_rows"
        connection.register(
            registered_rows,
            pa.table(
                {
                    "country_code": expansion_countries,
                    "street_name": expansion_streets,
                    "expanded_street": expanded_streets,
                    "variant_kind": expansion_kinds,
                    "variant_rank": expansion_ranks,
                }
            ),
        )
        try:
            connection.execute(
                f"""
                create or replace temporary table {expansion_input} as
                select distinct
                    country_code::varchar as country_code,
                    street_name::varchar as street_name,
                    expanded_street::varchar as expanded_street,
                    variant_kind::varchar as variant_kind,
                    variant_rank::utinyint as variant_rank
                from {registered_rows}
                """
            )
        finally:
            connection.unregister(registered_rows)
    else:
        connection.execute(
            f"""
            create or replace temporary table {expansion_input} (
                country_code varchar,
                street_name varchar,
                expanded_street varchar,
                variant_kind varchar,
                variant_rank utinyint
            )
            """
        )

    normalized_expanded_street = _compact_text_sql("expanded_street")
    connection.execute(
        f"""
        create or replace table {variant_table} as
        with candidates as (
            select
                document_id,
                index_scope,
                country_code,
                street_name as street_variant,
                normalized_street as normalized_street_variant,
                'parsed'::varchar as variant_kind,
                0::utinyint as variant_rank
            from {document_table}
            where normalized_street != ''

            union all

            select
                document.document_id,
                document.index_scope,
                document.country_code,
                expansion.expanded_street,
                {normalized_expanded_street} as normalized_street_variant,
                expansion.variant_kind,
                expansion.variant_rank
            from {document_table} document
            inner join {expansion_input} expansion
                on expansion.country_code = document.country_code
               and expansion.street_name = document.street_name
        ), deduplicated as (
            select *
            from candidates
            where normalized_street_variant != ''
            qualify row_number() over (
                partition by document_id, normalized_street_variant
                order by variant_rank, street_variant
            ) = 1
        )
        select
            *,
            {_deletion_signatures_sql("normalized_street_variant")}
                as street_deletion_signatures
        from deduplicated
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
