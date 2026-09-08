/**
 * The published SE address entity (spec 2026-09-06, section 3.3), under the final name
 * migration 000393 gave it. Every backoffice read of the published addresses names it
 * through this constant.
 *
 * MIND THE PREFIX: `corpscout.se_company_address_suggestion`, `_normalized`, `_history`,
 * `_rule` and `_precedence` all start with this string, so a match on the table -- in a
 * test, or in a fake client's dispatch -- has to carry the alias that follows it.
 */
export const SE_COMPANY_ADDRESS_TABLE = "corpscout.se_company_address";
