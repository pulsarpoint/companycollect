import { chQuery } from "~/lib/clickhouse.server";
import { getCountry } from "~/lib/countries";
import type { PublicContractRow } from "~/lib/queries.server";

/**
 * The company's government-contract awards, in the canonical
 * PublicContractRow shape the public company page renders.
 *
 * The SQL is deliberately NOT duplicated here: the tab sends the exact
 * `publicContractsQuery` the SE country config declares (se_government_
 * contracts WHERE company_id, newest 100), so the admin tab and the public
 * page can never drift apart on what counts as this company's award.
 */
export async function loadSeCompanyContracts(
  companyId: string,
): Promise<PublicContractRow[]> {
  const query = getCountry("se")?.detail?.publicContractsQuery;
  if (!query) return [];
  return chQuery<PublicContractRow>(query, { id: companyId });
}
