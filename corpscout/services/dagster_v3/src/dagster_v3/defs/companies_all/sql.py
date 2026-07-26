"""Per-country SELECTs producing the uniform ``companies_all`` row set.

Every fragment below is duplicated by design from the backoffice's
``app/lib/countries.ts`` per-country registry expressions (Python can't
import the TS registry) -- see the companies_all plan
(``corpscout/docs/superpowers/plans/2026-07-18-companies-all.md``). The
guard against the two specs drifting is the live parity sweep planned for a
later task; this module intentionally never "simplifies" an expression
relative to the source of truth.

Every column reference below was verified live against ``system.columns``
on 2026-07-18. Discrepancies found vs. the audit draft that seeded this
module:

- None. Every column named in the audit (including the countries flagged
  for extra scrutiny -- br's ``municipality_name``/``state``, se's
  ``company_id``/``sequence`` on ``se_industries``, fi's
  ``source_industry_code``, lv's ``lv_companies_nace`` columns) exists
  exactly as described. Specific confirmations:
  - ``br_companies`` carries ``municipality_name`` (String) and ``state``
    (LowCardinality(String)) directly -- the registry's ``place`` expression
    binds against the companies table itself, not ``br_establishments``.
  - ``se_companies`` has BOTH a ``registration_number`` column (the
    public-facing id exported as ``company_id`` in companies_all) and its
    own internal ``company_id`` column, used to join
    ``se_industries``/``se_company_financials_latest``. Since the
    2026-07-18 Swedish identity-normalization fix (dagster_v3
    ``sweden_company/normalized_duckdb.py``), that internal ``company_id``
    is no longer a distinct synthetic id -- it shares
    ``registration_number``'s id space (10-digit orgnr for legal entities,
    12-digit person ids for sole traders), because both columns are now
    derived from the same normalized identity. The explicit
    ``c.company_id`` qualification for SE's join keys is still required to
    disambiguate against ``ind.company_id``/``fin.company_id`` (a SQL
    column-naming collision, unrelated to the identity fix). No other
    country's companies table has a column literally named ``company_id``.
  - ``se_industries`` has ``sequence`` (UInt8) and ``is_primary`` (UInt8) and
    no ``description_en``/``description_original`` columns -- its industry
    label fallback chain is shorter than the "no" pattern (only
    ``nace_categories.description_en`` or the raw code, no per-row
    descriptions to fall back to).
  - ``fi_industries.source_industry_code`` is ``Nullable(String)`` -- the
    fi industry code is derived from it (not ``nace_normalized_code``,
    which also exists on the table but is not what the registry queries).
  - ``lv_companies_nace`` is not a narrow lookup table: it carries every
    ``lv_companies`` column plus ``nace_code``/``nace_confidence``/
    ``nace_label``. The industry subquery only selects the three columns
    it needs, so this doesn't change the SQL, but it does mean the
    ``LIMIT 1 BY regcode`` matters (the underlying rows are not
    pre-deduplicated to one per company). ``nace_code``/``nace_label`` are
    non-nullable ``String`` and empty for every row today, per the plan.

Financial columns (``revenue_amount_usd``/``fiscal_year``/``employees``) on
the ``{code}_company_financials_latest`` tables are ``Nullable`` and a
LEFT JOIN miss naturally yields NULL (confirmed in the company_financials_latest
work); ``fin.company_id != ''`` is the has-row test because ``company_id`` is
non-nullable ``String`` there (a miss yields ``''``, never NULL).
"""

# code -> per-country fragment spec, keyed on the shared SELECT template's
# placeholders. `financials_table`/`financials_join_key` are None for fr/cz,
# which have no financials-latest summary table -- `build_country_insert_select`
# renders those two countries from `_NO_FINANCIALS_TEMPLATE` instead, which
# drops the `fin` join entirely and emits NULL-typed literals for the four
# financial columns.
SOURCES = {
    "no": {
        "companies_table": "no_companies",
        "id": "org_number",
        "name": "name",
        "active": "is_active = 1",
        "status": "lifecycle_status",
        "legal_form": "coalesce(legal_form_description_original, legal_form_code)",
        "place": "''",
        "size": "''",
        "industry_subquery": (
            "SELECT toString(i.org_number) AS company_id, "
            "i.nace_normalized_code AS industry_code, "
            "coalesce(nullIf(n.description_en,''), i.description_en, "
            "i.description_original, i.nace_normalized_code) AS industry_label "
            "FROM corpscout.no_industries AS i "
            "LEFT JOIN corpscout.nace_categories AS n "
            "ON n.normalized_code = substring(i.nace_normalized_code,1,4) "
            "AND n.is_current = 1 "
            "ORDER BY i.is_primary DESC LIMIT 1 BY i.org_number"
        ),
        "industry_join_key": "org_number",
        "financials_table": "no_company_financials_latest",
        "financials_join_key": "org_number",
    },
    "fi": {
        "companies_table": "fi_companies",
        "id": "business_id",
        "name": "name",
        "active": "is_active = 1",
        "status": "lifecycle_status",
        "legal_form": (
            "coalesce(legal_form_description_en, legal_form_description_original, "
            "legal_form_code)"
        ),
        "place": "''",
        "size": "''",
        "industry_subquery": (
            "SELECT toString(i.business_id) AS company_id, "
            "coalesce(i.source_industry_code,'') AS industry_code, "
            "coalesce(nullIf(n.description_en,''), i.description_en, "
            "i.description_original, coalesce(i.source_industry_code,'')) "
            "AS industry_label "
            "FROM corpscout.fi_industries AS i "
            "LEFT JOIN corpscout.nace_categories AS n "
            "ON n.normalized_code = substring(coalesce(i.source_industry_code,''),1,4) "
            "AND n.is_current = 1 "
            "ORDER BY i.is_primary DESC LIMIT 1 BY i.business_id"
        ),
        "industry_join_key": "business_id",
        "financials_table": "fi_company_financials_latest",
        "financials_join_key": "business_id",
    },
    "se": {
        "companies_table": "se_companies",
        "id": "registration_number",
        "name": "legal_name",
        "active": "status = 'active'",
        "status": "status",
        "legal_form": "legal_form_code",
        "place": "''",
        "size": "''",
        "industry_subquery": (
            "SELECT toString(i.company_id) AS company_id, "
            "i.nace_rev2_class_code AS industry_code, "
            "coalesce(nullIf(n.description_en,''), i.nace_rev2_class_code) "
            "AS industry_label "
            "FROM corpscout.se_industries AS i "
            "LEFT JOIN corpscout.nace_categories AS n "
            "ON n.normalized_code = i.nace_rev2_class_code AND n.is_current = 1 "
            "ORDER BY i.is_primary DESC, i.sequence ASC LIMIT 1 BY i.company_id"
        ),
        # se_companies has its OWN `company_id` column (same normalized id
        # space as `registration_number` since the 2026-07-18 identity fix).
        # Industries/financials are keyed on company_id, so the join key must
        # be qualified `c.company_id` -- an unqualified `company_id` would be
        # ambiguous against `ind.company_id`/`fin.company_id`.
        "industry_join_key": "c.company_id",
        "financials_table": "se_company_financials_latest",
        "financials_join_key": "c.company_id",
    },
    "ee": {
        "companies_table": "ee_companies",
        "id": "reg_code",
        "name": "name",
        "active": "is_active = 1",
        "status": "coalesce(nullIf(status_en,''), status_original)",
        "legal_form": "coalesce(nullIf(legal_form_en,''), legal_form_original)",
        "place": "location",
        "size": "''",
        "industry_subquery": (
            "SELECT toString(i.reg_code) AS company_id, "
            "i.nace_normalized_code AS industry_code, "
            "coalesce(nullIf(n.description_en,''), i.description_en, "
            "i.description_original, i.nace_normalized_code) AS industry_label "
            "FROM corpscout.ee_industries AS i "
            "LEFT JOIN corpscout.nace_categories AS n "
            "ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1 "
            "ORDER BY i.is_primary DESC LIMIT 1 BY i.reg_code"
        ),
        "industry_join_key": "reg_code",
        "financials_table": "ee_company_financials_latest",
        "financials_join_key": "reg_code",
    },
    "lv": {
        "companies_table": "lv_companies",
        "id": "regcode",
        "name": "legal_name",
        "active": "is_active = 1",
        "status": "status",
        "legal_form": "coalesce(nullIf(legal_form_description_en,''), legal_form_text)",
        "place": "coalesce(address_city_name,'')",
        "size": "''",
        # lv_companies_nace carries every lv_companies column plus
        # nace_code/nace_confidence/nace_label; nace_code/nace_label are
        # non-nullable and empty for every row today (auto-fills later).
        "industry_subquery": (
            "SELECT toString(regcode) AS company_id, "
            "coalesce(nace_code,'') AS industry_code, "
            "coalesce(nullIf(nace_label,''), nace_code, '') AS industry_label "
            "FROM corpscout.lv_companies_nace LIMIT 1 BY regcode"
        ),
        "industry_join_key": "regcode",
        "financials_table": "lv_company_financials_latest",
        "financials_join_key": "regcode",
    },
    "gb": {
        "companies_table": "gb_companies",
        "id": "company_number",
        "name": "name",
        "active": "is_active = 1",
        "status": "company_status",
        "legal_form": "company_category",
        "place": "city",
        "size": "''",
        "industry_subquery": (
            "SELECT toString(i.company_number) AS company_id, "
            "i.nace_normalized_code AS industry_code, "
            "coalesce(nullIf(n.description_en,''), i.description_en, "
            "i.description_original, i.nace_normalized_code) AS industry_label "
            "FROM corpscout.gb_industries AS i "
            "LEFT JOIN corpscout.nace_categories AS n "
            "ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1 "
            "ORDER BY i.is_primary DESC LIMIT 1 BY i.company_number"
        ),
        "industry_join_key": "company_number",
        "financials_table": "gb_company_financials_latest",
        "financials_join_key": "company_number",
    },
    "fr": {
        "companies_table": "fr_companies",
        "id": "siren",
        "name": "name",
        "active": "is_active = 1",
        "status": "status_en",
        "legal_form": "legal_form_en",
        "place": "city",
        "size": "''",
        "industry_subquery": (
            "SELECT toString(i.siren) AS company_id, "
            "i.nace_normalized_code AS industry_code, "
            "coalesce(nullIf(n.description_en,''), i.description_en, "
            "i.description_original, i.nace_normalized_code) AS industry_label "
            "FROM corpscout.fr_industries AS i "
            "LEFT JOIN corpscout.nace_categories AS n "
            "ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1 "
            "ORDER BY i.is_primary DESC LIMIT 1 BY i.siren"
        ),
        "industry_join_key": "siren",
        # France has no financials-latest summary table.
        "financials_table": None,
        "financials_join_key": None,
    },
    "br": {
        "companies_table": "br_companies",
        "id": "cnpj_basico",
        "name": "legal_name",
        "active": "is_active = 1",
        "status": "status_en",
        "legal_form": "''",
        # Verified live: municipality_name/state live on br_companies itself
        # (not only br_establishments) -- the registry expression binds
        # against the companies table as the audit assumed.
        "place": "concat(municipality_name, ' / ', state)",
        "size": "company_size_en",
        "industry_subquery": (
            "SELECT toString(e.cnpj_basico) AS company_id, "
            "e.primary_cnae_code AS industry_code, "
            "coalesce(nullIf(m.nace_description_en,''), e.primary_cnae_code) "
            "AS industry_label "
            "FROM corpscout.br_establishments AS e "
            "LEFT JOIN corpscout.br_cnae_to_nace AS m "
            "ON m.cnae_normalized_code = e.primary_cnae_code "
            "WHERE e.is_headquarters = 1 "
            "ORDER BY e.primary_cnae_code != '' DESC LIMIT 1 BY e.cnpj_basico"
        ),
        "industry_join_key": "cnpj_basico",
        "financials_table": "br_company_financials_latest",
        "financials_join_key": "cnpj_basico",
    },
    "cz": {
        "companies_table": "cz_companies",
        "id": "ico",
        "name": "name",
        "active": "is_active = 1",
        "status": "if(is_active = 1, 'active', 'inactive')",
        "legal_form": "legal_form_en",
        "place": "city",
        "size": "''",
        "industry_subquery": (
            "SELECT toString(i.ico) AS company_id, "
            "i.nace_normalized_code AS industry_code, "
            "coalesce(nullIf(n.description_en,''), i.description_en, "
            "i.description_original, i.nace_normalized_code) AS industry_label "
            "FROM corpscout.cz_industries AS i "
            "LEFT JOIN corpscout.nace_categories AS n "
            "ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1 "
            "ORDER BY i.is_primary DESC LIMIT 1 BY i.ico"
        ),
        "industry_join_key": "ico",
        # Czechia has no financials-latest summary table.
        "financials_table": None,
        "financials_join_key": None,
    },
    "sk": {
        "companies_table": "sk_companies",
        "id": "ico",
        "name": "name",
        "active": "is_active = 1",
        "status": "if(is_active = 1, 'active', 'inactive')",
        "legal_form": "coalesce(nullIf(legal_form_en,''), legal_form_original)",
        "place": "city",
        "size": "''",
        "industry_subquery": (
            "SELECT toString(i.ico) AS company_id, "
            "i.nace_normalized_code AS industry_code, "
            "coalesce(nullIf(n.description_en,''), i.description_en, "
            "i.description_original, i.nace_normalized_code) AS industry_label "
            "FROM corpscout.sk_industries AS i "
            "LEFT JOIN corpscout.nace_categories AS n "
            "ON n.normalized_code = i.nace_normalized_code AND n.is_current = 1 "
            "ORDER BY i.is_primary DESC LIMIT 1 BY i.ico"
        ),
        "industry_join_key": "ico",
        "financials_table": "sk_company_financials_latest",
        "financials_join_key": "ico",
    },
}

# Shared SELECT shape for every column in `COMPANIES_ALL_COLUMNS`, for
# countries with a financials-latest summary table to LEFT JOIN.
_TEMPLATE = """
SELECT
  '{code}' AS country_code,
  toString({id}) AS company_id,
  coalesce({name}, '') AS name,
  lowerUTF8(coalesce({name}, '')) AS name_normalized,
  toUInt8({active}) AS is_active,
  coalesce(toString({status}), '') AS status,
  coalesce(toString({legal_form}), '') AS legal_form,
  coalesce(toString({place}), '') AS place,
  coalesce(toString({size}), '') AS size,
  coalesce(ind.industry_code, '') AS industry_code,
  coalesce(ind.industry_label, '') AS industry_label,
  fin.revenue_amount_usd AS revenue_usd,
  fin.fiscal_year AS fiscal_year,
  fin.employees AS employees,
  toUInt8(fin.company_id != '') AS has_financials,
  toUInt8(proc.company_id != '') AS has_government_contract,
  proc.public_award_count AS public_award_count,
  proc.public_award_last_date AS public_award_last_date,
  -- The contract summary is a view, so there is no resolve time to read:
  -- it is always current. The informative stamp is when the underlying
  -- source data last changed.
  coalesce(proc.source_updated_at, now64(3)) AS signals_resolved_at,
  now64(3) AS resolved_at
FROM corpscout.{companies_table} AS c
LEFT JOIN ({industry_subquery}) AS ind ON ind.company_id = toString({industry_join_key})
LEFT JOIN corpscout.{financials_table} AS fin ON fin.company_id = toString({financials_join_key})
LEFT JOIN corpscout.company_government_contract_summary AS proc
  ON proc.country_code = '{code}' AND proc.company_id = toString({id})
"""

# fr/cz have no financials-latest summary table: no `fin` join, and the four
# financial columns are NULL-typed literals matching the migration's
# Nullable(...) column types (has_financials is a real UInt8, always 0).
_NO_FINANCIALS_TEMPLATE = """
SELECT
  '{code}' AS country_code,
  toString({id}) AS company_id,
  coalesce({name}, '') AS name,
  lowerUTF8(coalesce({name}, '')) AS name_normalized,
  toUInt8({active}) AS is_active,
  coalesce(toString({status}), '') AS status,
  coalesce(toString({legal_form}), '') AS legal_form,
  coalesce(toString({place}), '') AS place,
  coalesce(toString({size}), '') AS size,
  coalesce(ind.industry_code, '') AS industry_code,
  coalesce(ind.industry_label, '') AS industry_label,
  CAST(NULL AS Nullable(Float64)) AS revenue_usd,
  CAST(NULL AS Nullable(Int32)) AS fiscal_year,
  CAST(NULL AS Nullable(Float64)) AS employees,
  toUInt8(0) AS has_financials,
  toUInt8(proc.company_id != '') AS has_government_contract,
  proc.public_award_count AS public_award_count,
  proc.public_award_last_date AS public_award_last_date,
  -- The contract summary is a view, so there is no resolve time to read:
  -- it is always current. The informative stamp is when the underlying
  -- source data last changed.
  coalesce(proc.source_updated_at, now64(3)) AS signals_resolved_at,
  now64(3) AS resolved_at
FROM corpscout.{companies_table} AS c
LEFT JOIN ({industry_subquery}) AS ind ON ind.company_id = toString({industry_join_key})
LEFT JOIN corpscout.company_government_contract_summary AS proc
  ON proc.country_code = '{code}' AND proc.company_id = toString({id})
"""


def build_country_insert_select(code: str) -> str:
    """Return the companies_all SELECT body for one country `code`.

    The caller (Task 2's asset) wraps this in
    ``INSERT INTO corpscout.{stage} ({columns}) {select}`` using the explicit
    `COMPANIES_ALL_COLUMNS` column list, so SELECT order and stage-table
    order can never silently misalign.
    """
    if code not in SOURCES:
        raise ValueError(
            f"Unknown companies_all source code: {code!r}; "
            f"expected one of {sorted(SOURCES)}"
        )
    spec = SOURCES[code]
    if spec["financials_table"] is None:
        return _NO_FINANCIALS_TEMPLATE.format(code=code, **spec)
    return _TEMPLATE.format(code=code, **spec)
