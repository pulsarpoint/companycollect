/**
 * The published SE person entity (spec 2026-09-09, section 3.3), under the name
 * migration 000396 gave it. Every backoffice read of the published persons names it
 * through this constant, so slice 4's rename to `corpscout.se_company_person` is a
 * one-line change here.
 *
 * MIND THE PREFIX: after that rename `corpscout.se_company_person_suggestion`,
 * `_normalized`, `_history`, `_rule` and `_precedence` all start with this string, so a
 * match on the table -- in a test, or in a fake client's dispatch -- has to carry the
 * alias that follows it.
 */
export const SE_COMPANY_PERSON_TABLE = "corpscout.se_company_person_v2";
