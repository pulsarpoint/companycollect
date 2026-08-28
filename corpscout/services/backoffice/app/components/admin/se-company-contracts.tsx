import { FileTextIcon } from "lucide-react";
import { PublicContractsSection } from "~/components/detail/public-contracts-section";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import type { PublicContractRow } from "~/lib/queries.server";

/**
 * The company's government-contract awards -- the SAME PublicContractsSection
 * the public company page renders, fed by the same SE publicContractsQuery,
 * so the admin tab can never disagree with the public page about a win. Only
 * the empty state is this tab's own: the public page simply omits the section,
 * while a reviewer opening the tab deserves an answer.
 */
export function SeCompanyContractsTab({
  contracts,
}: {
  contracts: PublicContractRow[];
}) {
  if (contracts.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <FileTextIcon />
          </EmptyMedia>
          <EmptyTitle>No contract awards</EmptyTitle>
          <EmptyDescription>
            No exact-matched government contract awards name this company as a
            winner in the procurement sources.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return <PublicContractsSection contracts={contracts} />;
}
