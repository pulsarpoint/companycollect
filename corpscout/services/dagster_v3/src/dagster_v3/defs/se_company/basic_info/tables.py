"""Table names and column tuples of the basic-info entity, pinned against the DDL."""

DATABASE = "corpscout"

SUGGESTION_TABLE = "se_company_basic_info_suggestion"
MAIN_TABLE = "se_company_basic_info"
HISTORY_TABLE = "se_company_basic_info_history"
PRECEDENCE_TABLE = "se_company_basic_info_precedence"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"

# The ten value columns a suggestion row carries, in DDL order (economic_activity was
# added after status by migration 000394, slice 6).
VALUE_COLUMNS: tuple[str, ...] = (
    "legal_name",
    "legal_form_code",
    "status",
    "economic_activity",
    "incorporation_date",
    "lei",
    "wikidata_id",
    "description",
    "description_language",
    "description_sv",
)

# The fields the fold decides with a precedence map: every value column except
# description_language, which follows the description winner (spec 4 and 5).
FOLDED_FIELDS: tuple[str, ...] = tuple(c for c in VALUE_COLUMNS if c != "description_language")

# The folded fields that are '' rather than NULL on the main row when unknown (status since
# the first DDL, economic_activity since 000394): the fold writes '' and reads '' as no value.
NON_NULLABLE_FIELDS: tuple[str, ...] = ("status", "economic_activity")

# economic_activity's three values, from SCB's Företagsstatus codes 1, 0 and 9.
ECONOMIC_ACTIVITIES: tuple[str, ...] = ("active", "never", "ceased")

# What an extractor (or the backoffice) inserts: every column of the table, in DDL order.
SUGGESTION_INSERT_COLUMNS: tuple[str, ...] = (
    "company_id",
    "source",
    "source_record_uid",
    "observed_at",
    *VALUE_COLUMNS,
    "decided_by",
    "note",
    "suggested_at",
    "source_run_id",
    "extractor_version",
)

# The main row, in DDL order: each folded field followed by its _source, with
# description_language after description_source and without a source of its own.
MAIN_COLUMNS: tuple[str, ...] = (
    "company_id",
    "legal_name",
    "legal_name_source",
    "legal_form_code",
    "legal_form_code_source",
    "status",
    "status_source",
    "economic_activity",
    "economic_activity_source",
    "incorporation_date",
    "incorporation_date_source",
    "lei",
    "lei_source",
    "wikidata_id",
    "wikidata_id_source",
    "description",
    "description_source",
    "description_language",
    "description_sv",
    "description_sv_source",
    "folded_at",
    "fold_version",
    "source_run_id",
)

HISTORY_COLUMNS: tuple[str, ...] = (*MAIN_COLUMNS, "changed_fields")

# The precedence table with its company scope (spec 3.5, amended 2026-09-05): '' is a
# global rule exported from code; a company id is a reviewer rule for that company only.
PRECEDENCE_COLUMNS: tuple[str, ...] = (
    "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
)
