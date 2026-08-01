CONTRACT_FACTS_TABLE = "company_contract_facts"

CONTRACT_FACTS_TABLES = (CONTRACT_FACTS_TABLE,)

# Column order is the contract with migration 000234 and is asserted by
# tests/test_clickhouse_migrations.py.
CONTRACT_FACTS_COLUMNS = (
    "country_code",
    "contract_ref",
    "company_id",
    "contract_id",
    "source_slug",
    "source_notice_id",
    "source_lot_id",
    "source_winner_ordinal",
    "winner_name",
    "source_url",
    "publication_date",
    "buyer_name",
    "buyer_id",
    "title",
    "agreement_type",
    "cpv_code",
    "directive_governed",
    "value_amount_original",
    "value_currency",
    "value_amount_usd",
    "notice_value_amount_original",
    "notice_value_currency",
    "notice_value_amount_usd",
    "value_source_field",
    "notice_value_source_field",
    "source_updated_at",
    "contract_key",
    "resolved_at",
)

# Everything the per-country views carry, in their own order. contract_ref and
# resolved_at are computed by the loader rather than selected.
SOURCE_COLUMNS = tuple(
    column
    for column in CONTRACT_FACTS_COLUMNS
    if column not in ("contract_ref", "resolved_at")
)

# The countries whose contracts are published as a *_government_contracts
# view. Read from the views rather than rebuilt here: they hold the identity
# rules -- which national id a TED winner resolves through, how a contract_key
# is composed -- and a second copy of that would drift.
CONTRACT_COUNTRIES = ("br", "ee", "fi", "fr", "lv", "no", "se", "sk")

# A floor, not an equality: contracts grow. Guards against replacing a
# populated table from a source that has gone empty, which is exactly what a
# broken upstream view looks like.
MIN_CONTRACT_FACTS_ROWS = 300_000


AWARD_FACTS_TABLE = "company_contract_award_facts"

AWARD_FACTS_TABLES = (AWARD_FACTS_TABLE,)

# Column order is the contract with migration 000236 and is asserted by
# tests/test_clickhouse_migrations.py. The 26 of the contract shape plus the
# three supplier columns those pages show -- winner_registered_id,
# winner_match_status, winner_country.
AWARD_FACTS_COLUMNS = (
    "country_code",
    "contract_ref",
    "company_id",
    "contract_id",
    "source_slug",
    "source_notice_id",
    "source_lot_id",
    "source_winner_ordinal",
    "winner_name",
    "winner_registered_id",
    "winner_match_status",
    "winner_country",
    "source_url",
    "publication_date",
    "buyer_name",
    "buyer_id",
    "title",
    "agreement_type",
    "cpv_code",
    "directive_governed",
    "value_amount_original",
    "value_currency",
    "value_amount_usd",
    "notice_value_amount_original",
    "notice_value_currency",
    "notice_value_amount_usd",
    "value_source_field",
    "notice_value_source_field",
    "source_updated_at",
    "contract_key",
    "resolved_at",
)

# Only two registers publish the award shape. Brazil is why this exists: its
# page sources br_pncp_contracts through the view and reached 12.8 GiB at 4.6M
# rows.
AWARD_COUNTRIES = ("br", "no")

MIN_AWARD_FACTS_ROWS = 50_000


ROLLUP_TABLE = "company_contract_rollup"

ROLLUP_TABLES = (ROLLUP_TABLE,)

# Column order is the contract with migration 000238 and is asserted by
# tests/test_clickhouse_migrations.py. One row per (country, contract): the
# first fourteen are what the contracts list selects, the rest what it filters
# and sorts on.
ROLLUP_COLUMNS = (
    "country_code",
    "contract_ref",
    "contract_date",
    "buyer_name",
    "title",
    "agreement_type",
    "cpv_code",
    "winner_name",
    "winner_registered_id",
    "winner_match_status",
    "supplier_count",
    "amount_original",
    "currency",
    "amount_usd",
    "source_url",
    "publication_date",
    "resolved_at",
)

# A floor, not an equality. Brazil alone contributes 4.6M contracts, so this is
# an order of magnitude below the real figure -- it exists to catch a rollup
# built from an emptied facts table, not to track growth.
MIN_ROLLUP_ROWS = 500_000
