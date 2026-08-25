import { LineChartIcon } from "lucide-react";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";

// A scaffolded tab: no loader yet, so nothing here imports `~/lib/*.server` and
// the module stays trivially client-safe. The real list (companies by
// financial datatype, with filters and bulk actions) lands later.

export function meta() {
  return [{ title: "Companies · Financial | CompanyCollect" }];
}

export default function AdminSeCompaniesFinancial() {
  return (
    <Empty className="border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <LineChartIcon />
        </EmptyMedia>
        <EmptyTitle>Coming soon</EmptyTitle>
        <EmptyDescription>
          This view will list companies by financial datatype, with filters and
          bulk actions.
        </EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}
