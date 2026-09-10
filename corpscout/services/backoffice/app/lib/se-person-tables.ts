/**
 * The published SE person entity (spec 2026-09-09, section 3.3), under the name migration
 * 000398 gave it. It was BUILT as `corpscout.se_company_person_v2` -- the 2026-08-19 table
 * held the final name until person slice 0 dropped it -- and renamed in slice 4. Every
 * backoffice read of the published persons names it through this constant.
 *
 * MIND THE PREFIX: `corpscout.se_company_person_suggestion`, `_normalized`, `_history`,
 * `_rule` and `_precedence` all start with this string, so a match on the table -- in a
 * test, or in a fake client's dispatch -- has to carry the alias that follows it.
 */
export const SE_COMPANY_PERSON_TABLE = "corpscout.se_company_person";
