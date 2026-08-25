import { SearchXIcon } from "lucide-react";
import { buttonVariants } from "~/components/ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";

/**
 * The company area's 404: an id that is in neither `se_company_info` nor
 * `se_companies`.
 *
 * Deliberately NOT the same view as the Info tab's "not published yet". That
 * one means "the register knows this company, Dagster has not enriched it",
 * which resolves itself on the next run; this one means the id does not exist
 * at all, which never will. Telling a reviewer to wait for a run that will
 * never publish anything is the worse of the two mistakes.
 */
export function SeCompanyNotFound({ companyId }: { companyId: string }) {
  return (
    <Empty className="border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <SearchXIcon />
        </EmptyMedia>
        <EmptyTitle>No company with this id in the register</EmptyTitle>
        <EmptyDescription>
          {companyId} is not in se_companies, so nothing downstream can carry
          it either. Check the organization number, or look for the company in
          the register list.
        </EmptyDescription>
      </EmptyHeader>
      <EmptyContent>
        {/* Plain anchor, not <Link>: this component renders in tests without a
            Router, the same way SeCompanyInfoNotPublished does. */}
        <a
          className={buttonVariants({ variant: "outline" })}
          href="/admin/se/companies"
        >
          Back to the company list
        </a>
      </EmptyContent>
    </Empty>
  );
}
