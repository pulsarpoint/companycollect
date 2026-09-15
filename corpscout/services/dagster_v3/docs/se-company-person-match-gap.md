# Person match-gap view

`corpscout.se_company_person_match_gap` is a derived, refreshable materialized view
created by migration 000406. It lists companies with unresolved name differences between
active published people whose sources do not overlap:

- Call names: equal surname tokens, with one person's given-name tokens strictly
  containing the other's.
- Double surnames: equal given-name token sets, with one person's two surname tokens
  containing the other's single surname token.

Both rules exclude conflicting birth years. The view uses the normalized observations
referenced by published people and excludes pairs already joined by a stored match with
confidence at least 0.8 for the company's current, error-free match input.

The columns are `company_id`, `call_name_pairs`, `double_surname_pairs`, and `computed_at`.
It refreshes hourly at :30, can lag a fold by an hour, and is not an input to the matcher
or fold. Its SQL is maintained in `person/tables.py`; migration tests pin the view to that
builder, and ClickHouse integration tests exercise both name rules and their exclusions.

The unused request queue and response design was retired on 2026-09-13. Its original
DDL and proposed workflow were removed. Migration 000406 retains the view and the
already-deployed `request_id` columns on the match tables. The current matcher writes
the empty-string default to those columns.

Global refresh and publish actions, including the saved prompt and model configuration,
are described in the [backoffice People actions](../../backoffice/docs/people-actions.md).
