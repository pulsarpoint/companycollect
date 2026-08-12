# Company Domain Suggestions

This pipeline produces review candidates. It never writes canonical company-domain facts.

## Normalized matching contracts

Matching is split into three dbt layers so company registry facts never depend on web or
technology data:

1. `stg_se_company_match_features` is the country-side view. It emits long-form normalized
   `identifier`, `name`, `address`, and `industry` features with the original value and source
   field retained as evidence. Its inputs are Sweden registry, current-address, industry, and
   GLEIF tables only.
2. `stg_web_domain_match_features` is the crawl-partitioned web-side table. It normalizes root
   domain labels, country-code TLDs, JSON-LD organization names and postal addresses, identifiers,
   and Common Crawl NACE classifications into an auditable long-form contract. The first build
   selects the latest crawl; normal incremental builds select only crawl ids newer than the latest
   indexed partition. An explicit `web_feature_crawl_id` rebuilds that crawl and atomically replaces
   its partition, including removal of features that disappeared from the upstream snapshot. Crawl
   ids are resolved before the main query and rendered as literal source filters so ClickHouse can
   prune the underlying crawl partitions.
3. Matching models join those contracts, enforce joint-signal rules and fan-out limits, and
   publish scored suggestions plus evidence.

Each web feature carries both `crawl_id`, the partition where it is available for matching, and
`source_crawl_id`, the snapshot where the evidence was observed. Domain, identifier, name, address,
and TLD evidence is native to the indexed crawl. Industry extraction may lag the domain crawl, so
the model carries forward the most recent industry snapshot that is not newer than the indexed
crawl while retaining its original source crawl for auditability.

The company feature model is deliberately a view rather than another snapshot table. Current
registry and address snapshots remain the owners of freshness and history; normalization is
recomputed consistently when queried.

`company_domain_web_features_dbt_job` owns the global web-side materialization. It is deliberately
separate from the country-partitioned suggestion job, so each Common Crawl snapshot is indexed once
and can later be reused by every country pipeline.

## Deterministic stage

`sweden_company_domain_suggestions_dbt_job` is the deterministic first stage. A domain becomes a
candidate through one of two paths:

- an exact VAT or LEI match; or
- an exact normalized postal-address match together with an exact four-digit NACE match.

Neither address nor industry can generate a candidate by itself. Names, domain labels, people,
country and generic registration-number observations also cannot create candidates in this stage.
Legal-name/domain similarity and a Sweden `.se` TLD remain reserved as bounded boosts for a future
scoring version.

Exact VAT/LEI evidence is authoritative. If address-plus-NACE confirms the same company/domain
pair, its evidence and score are added. If it points to another domain, it cannot replace the
identifier suggestion. Address-plus-NACE can resolve an otherwise unmatched company only when one
eligible domain remains.

For Sweden, the expected VAT value is `SE` plus the ten-digit organization number plus `01`. The
pipeline uses that value only as a match key. It does not infer that every company is VAT
registered. LEIs come from GLEIF records registered to a Sweden company and must be 20
alphanumeric characters.

The dbt graph produces:

- `company_domain_identifier_matches_dbt`, an append-only audit history of every exact VAT/LEI
  match, including matches classified as directories or ambiguous;
- `company_domain_suggestions_dbt`, unique non-directory company/domain matches only;
- `company_domain_suggestion_evidence_dbt`, the identifier, address and NACE source evidence for
  those suggestions;
- `company_domain_dbt_discovery_runs`, the completed-run marker and country-level counts.

Dagster supplies the country, discovery run id and the versioned fan-out limit. A run becomes
visible only after dbt models and checks pass and the final Dagster asset writes its completion
marker.

## Identifier fan-out

The initial Sweden limit is five distinct legal identifier values per root domain. Fan-out is
calculated from all identifier features in the active web identity index before joining them to
Sweden companies. A global directory with hundreds of LEIs is therefore classified as a directory
even when only a few of those LEIs match Sweden companies. Names and other web content are not
included in this count.

The web identity index keeps identifier observations from only the latest crawl in which each
domain was observed. If an identifier disappears from a later domain crawl, rebuilding the index
removes it instead of carrying the older observation forward indefinitely.

Each company/domain candidate is classified as:

- `directory` when the root domain contains more than the configured identifier limit;
- `ambiguous` when a company has exact identifier evidence on more than one eligible domain;
- `unique` when exactly one eligible domain remains.

Directory evidence is retained in the identifier-match history but never published as a
deterministic suggestion. Directory matches are excluded before counting eligible domains, so a
company can still resolve uniquely when its VAT appears on both its official site and a registry
site.

Migration `000262_corpscout_company_domain_suggestions_dbt` owns the suggestion, evidence and run
tables. Migration `000263_corpscout_company_domain_identifier_matches_dbt` owns the identifier
match history and adds resolved/unresolved counts to the run marker. Migration
`000266_corpscout_company_domain_address_nace_matching` adds the address score and updates the
active suggestion view. Scoring version 5 adds global identifier-directory detection and
latest-domain-crawl identifier freshness without changing the output schema.

## Address and industry fan-out

The address path first limits a normalized web address to five root domains. Addresses observed on
more domains are retained in the intermediate audit models but classified as directories and
cannot publish a suggestion. Eligible address matches are then joined to an exact four-digit NACE
code shared by the Sweden company and the web domain.

The resulting company/domain pairs are classified as:

- `directory` when the address exceeds the configured domain limit;
- `ambiguous` when a company has more than one eligible address-plus-NACE domain;
- `unique` when exactly one eligible domain remains.

Scoring version 5 assigns 35 points to exact address evidence and 25 points to exact NACE evidence.
The composite path therefore scores 60. A single exact identifier type scores 70 and matching both
VAT and LEI scores 100; all scores are capped at 100 when multiple authoritative signals confirm
the same pair.

## Deferred LLM stage

LLM-assisted matching is intentionally not implemented in this stage. A later workflow may process
unmatched companies, ambiguous matches and directory-only matches using a bounded domain shortlist.
LLM review remains reserved for companies unresolved after exact identifiers and the bounded
address-plus-industry path. It may consume name, people, industry, country, and domain-label
evidence from the deterministic shortlist rather than searching the entire web corpus.

## Review boundary

Suggestions remain replaceable offline output. Reviewer decisions belong in the transactional
application database and must reference a suggestion and discovery run. Accepting a suggestion
creates a separately sourced canonical relationship; rejecting it must not mutate source evidence.
