"""Website candidates from corpscout.company_domains (the reviewed, multi-source domain table
the serving build maintains; company_serving_current publishes the SE partition).

One row per company: a reviewer's confirmed_primary wins, otherwise the highest-confidence
suggestion nobody has reviewed yet; rejected and inactive rows are never candidates, and a
reviewer's related-not-primary decision is respected -- a confirmed_related domain is not
this company's website, however confident the suggestion behind it was. The uid is the row's
evidence_fingerprint (a review decision or new evidence changes it), observed_at its
last_seen_at.
"""

from functools import partial

import dagster as dg

from dagster_v3.defs.se_company.common import DATABASE
from dagster_v3.defs.se_company.fields.candidates.common import (
    CandidateExtractor,
    changed_companies_scope_sql,
    candidate_rows_from_result,
    define_candidate_asset,
    json_object_sql,
    json_string_sql,
)

SOURCE = "domains"
EXTRACTOR_VERSION = "domains-candidates-v1"
DOMAINS_TABLE = "company_domains"
COUNTRY = "SE"


def build_scope_sql() -> str:
    return changed_companies_scope_sql(source=SOURCE, changes_sql=f"""    SELECT company_id, resolved_at AS changed_at FROM {DATABASE}.{DOMAINS_TABLE} WHERE country_code = '{COUNTRY}'""")


def build_candidates_sql() -> str:
    website_json = json_object_sql({
        "compare_key": json_string_sql("lowerUTF8(root_domain)"),
        "review_status": json_string_sql("review_status"),
        "root_domain": json_string_sql("root_domain"),
    })
    return f"""WITH domains AS (
    SELECT company_id, evidence_fingerprint AS source_record_uid, last_seen_at AS observed_at,
        website_url, root_domain, toString(review_status) AS review_status
    FROM {DATABASE}.{DOMAINS_TABLE} FINAL
    WHERE country_code = '{COUNTRY}' AND company_id IN %(company_ids)s AND is_active = 1
      AND trim(website_url) != '' AND trim(evidence_fingerprint) != ''
      AND (review_status = 'confirmed_primary' OR (suggested_primary = 1 AND review_status = 'unreviewed'))
    ORDER BY (review_status = 'confirmed_primary') DESC, suggested_confidence DESC, root_domain ASC
    LIMIT 1 BY company_id
)
SELECT company_id, 'website', source_record_uid, observed_at, website_url,
    {website_json}
FROM domains"""


rows_from_result = partial(candidate_rows_from_result, source=SOURCE, extractor_version=EXTRACTOR_VERSION)

EXTRACTOR = CandidateExtractor(
    source=SOURCE, extractor_version=EXTRACTOR_VERSION, source_tables=(DOMAINS_TABLE,),
    build_scope_sql=build_scope_sql, build_candidates_sql=build_candidates_sql,
)

se_company_field_candidates_domains = define_candidate_asset(
    EXTRACTOR,
    deps=("company_serving_current",),
    description=(
        "Website candidates for Swedish companies from the reviewed domain table: the confirmed "
        "primary domain, else the best suggested one. Preview by default; execute: true appends new evidence."
    ),
)

defs = dg.Definitions(assets=[se_company_field_candidates_domains])
