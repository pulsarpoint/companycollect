{% macro normalize_identity_value(value_expression) -%}
lowerUTF8(
    replaceRegexpAll(
        ifNull({{ value_expression }}, ''),
        '[^\\p{L}\\p{N}]',
        ''
    )
)
{%- endmacro %}


{% macro sql_string_literal_list(values) -%}
(
    {%- for value in values -%}
    '{{ value | replace("'", "''") }}'{{ ', ' if not loop.last else '' }}
    {%- endfor -%}
)
{%- endmacro %}


{% macro normalize_address_text(value_expression) -%}
trim(
    replaceRegexpAll(
        lowerUTF8(normalizeUTF8NFKC(ifNull({{ value_expression }}, ''))),
        '[^\\p{L}\\p{N}]+',
        ' '
    )
)
{%- endmacro %}


{% macro normalize_address_compact(value_expression) -%}
replaceRegexpAll(
    lowerUTF8(normalizeUTF8NFKC(ifNull({{ value_expression }}, ''))),
    '[^\\p{L}\\p{N}]',
    ''
)
{%- endmacro %}


{% macro normalize_address_country(value_expression) -%}
multiIf(
    {{ normalize_address_compact(value_expression) }} IN (
        'se', 'swe', 'sweden', 'sverige'
    ),
    'se',
    {{ normalize_address_compact(value_expression) }}
)
{%- endmacro %}


{% macro normalize_postal_address(street_expression, postal_expression, town_expression, country_expression) -%}
arrayStringConcat(
    arrayFilter(component -> component != '', [
        replaceRegexpAll(
            {{ normalize_address_text(street_expression) }},
            ' +[0-9]+ +tr$',
            ''
        ),
        if(
            {{ normalize_address_compact(postal_expression) }} = '00000',
            '',
            {{ normalize_address_compact(postal_expression) }}
        ),
        if(
            {{ normalize_address_text(town_expression) }} = 'utlandet',
            '',
            {{ normalize_address_text(town_expression) }}
        ),
        {{ normalize_address_country(country_expression) }}
    ]),
    '|'
)
{%- endmacro %}


{% macro strip_one_sweden_legal_form(tokens_expression) -%}
arraySlice(
    {{ tokens_expression }},
    1,
    greatest(
        length({{ tokens_expression }})
        - multiIf(
            length({{ tokens_expression }}) >= 2
                AND arraySlice({{ tokens_expression }}, -2) IN (
                    ['ab', 'publ'],
                    ['aktiebolag', 'publ'],
                    ['ekonomisk', 'förening'],
                    ['ideell', 'förening']
                ),
            2,
            length({{ tokens_expression }}) >= 1
                AND arrayElement({{ tokens_expression }}, -1) IN (
                    'kommanditbolag',
                    'handelsbolag',
                    'aktiebolag',
                    'stiftelse',
                    'ab',
                    'hb',
                    'kb'
                ),
            1,
            0
        ),
        0
    )
)
{%- endmacro %}


{% macro sweden_core_name_tokens(tokens_expression) -%}
{{ strip_one_sweden_legal_form(strip_one_sweden_legal_form(tokens_expression)) }}
{%- endmacro %}
