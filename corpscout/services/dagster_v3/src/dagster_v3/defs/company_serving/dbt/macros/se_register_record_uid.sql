{#- The company-source-record uid of a Swedish register row, byte-identical to
    dagster_v3.defs.se_company.common.register_record_uid_sql (tests/test_se_register_record_uid.py
    pins the two equal). source_slug is 'sweden_bolagsverket' or 'sweden_scb'; alias names a row
    of se_bolagsverket_companies or se_scb_companies. -#}
{%- macro se_register_record_uid(source_slug, alias) -%}
lower(hex(SHA256(concat('company-source-record-v1\nstructured\n', '{{ source_slug }}', '\nregistry_company\n', {{ alias }}.source_record_id, '\n', lowerUTF8({{ alias }}.source_payload_hash)))))
{%- endmacro %}
